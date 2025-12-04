# data_collecter/imdb_reviews.py

import asyncio
import aiohttp
import pandas as pd
import time
import json
from pathlib import Path
from urllib.parse import quote
from datetime import datetime
import html
import re

# ==========================
# IMDb GraphQL 기본 설정
# ==========================

GRAPHQL_URL = "https://caching.graphql.imdb.com/"
OPERATION_NAME = "TitleReviewsRefine"
PERSISTED_QUERY_HASH = (
    "d389bc70c27f09c00b663705f0112254e8a7c75cde1cfd30e63a2d98c1080c87"
)

MAX_CALLS_PER_SECOND = 2  # 초당 요청 수
TIMEOUT = aiohttp.ClientTimeout(total=30, connect=10)
MAX_RETRIES = 3
REVIEWS_PER_REQUEST = 25  # 한 요청당 리뷰 개수

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/120.0.0.0 Safari/537.36"
)


# ==========================
# Rate Limiter
# ==========================


class RateLimiter:
    def __init__(self, rate: float):
        self.rate = rate
        self.tokens = rate
        self.updated_at = time.monotonic()
        self.lock = asyncio.Lock()

    async def acquire(self):
        async with self.lock:
            now = time.monotonic()
            elapsed = now - self.updated_at
            self.tokens = min(self.rate, self.tokens + elapsed * self.rate)
            self.updated_at = now

            if self.tokens < 1:
                sleep_time = (1 - self.tokens) / self.rate
                await asyncio.sleep(sleep_time)
                self.tokens = 1

            self.tokens -= 1


rate_limiter = RateLimiter(MAX_CALLS_PER_SECOND)


# ==========================
# GraphQL URL / 요청
# ==========================


def build_graphql_url(
    imdb_id: str,
    after_cursor: str | None = None,
    first: int = REVIEWS_PER_REQUEST,
    sort_by: str = "HELPFULNESS_SCORE",
) -> str:
    """
    IMDb GraphQL 요청 URL 생성
    """
    variables = {
        "const": imdb_id,
        "first": first,
        "locale": "en-US",  # 영어 리뷰
        "sort": {"by": sort_by, "order": "DESC"},
        "filter": {},
    }
    if after_cursor:
        variables["after"] = after_cursor

    extensions = {"persistedQuery": {"sha256Hash": PERSISTED_QUERY_HASH, "version": 1}}

    variables_json = json.dumps(variables, separators=(",", ":"))
    extensions_json = json.dumps(extensions, separators=(",", ":"))

    url = (
        f"{GRAPHQL_URL}?"
        f"operationName={OPERATION_NAME}"
        f"&variables={quote(variables_json)}"
        f"&extensions={quote(extensions_json)}"
    )
    return url


async def fetch_json(session: aiohttp.ClientSession, url: str, retry: int = 0):
    """
    GraphQL 호출 + 재시도
    """
    if retry >= MAX_RETRIES:
        return None

    await rate_limiter.acquire()

    headers = {
        "User-Agent": USER_AGENT,
        "Accept": "application/json",
        "Content-Type": "application/json",
    }

    try:
        async with session.get(url, headers=headers, timeout=TIMEOUT) as resp:
            if resp.status == 429:
                wait_time = 5 * (retry + 1)
                print(f"⚠️  429 Too Many Requests → {wait_time}s 대기 후 재시도")
                await asyncio.sleep(wait_time)
                return await fetch_json(session, url, retry + 1)

            if resp.status != 200:
                if retry < MAX_RETRIES - 1:
                    await asyncio.sleep(2**retry)
                    return await fetch_json(session, url, retry + 1)
                print(f"❌ HTTP {resp.status}")
                return None

            return await resp.json()

    except asyncio.TimeoutError:
        if retry < MAX_RETRIES - 1:
            await asyncio.sleep(2**retry)
            return await fetch_json(session, url, retry + 1)
        print("❌ Timeout")
        return None

    except Exception as e:
        if retry < MAX_RETRIES - 1:
            await asyncio.sleep(2**retry)
            return await fetch_json(session, url, retry + 1)
        print(f"❌ 요청 에러: {e}")
        return None


# ==========================
# 리뷰 파싱
# ==========================


def clean_html(text: str | None) -> str | None:
    if not text:
        return None
    text = html.unescape(text)
    text = text.replace("<br/>", "\n").replace("<br>", "\n")
    text = re.sub("<[^<]+?>", "", text)
    return text.strip()


def parse_review_node(node: dict, imdb_id: str) -> dict | None:
    """
    GraphQL 응답에서 필요한 컬럼만 추출
    (팀에서 합의한 컬럼 이름으로 바로 맞춰서 반환)
    """
    try:
        review_id = node.get("id")

        author = node.get("author", {})
        username = author.get("username", {}).get("text")
        user_id = author.get("userId")

        author_rating = node.get("authorRating")

        helpful = node.get("helpfulness", {})
        up = helpful.get("upVotes", 0)
        down = helpful.get("downVotes", 0)

        review_date = node.get("submissionDate")

        summary = node.get("summary", {})
        review_title = summary.get("originalText")

        text_data = node.get("text", {}).get("originalText", {})
        review_text_html = text_data.get("plaidHtml")
        review_text = clean_html(review_text_html)

        return {
            "author_rating": author_rating,
            "review_title": review_title,
            "review_text": review_text,
            "review_id": review_id,
            "username": username,
            "user_id": user_id,
            "review_date": review_date,
            "helpful_up_votes": up,
            "helpful_down_votes": down,
            "imdb_id": imdb_id,
            # 길이는 나중에 감성분석에서 유용하니까 같이 저장
            "review_text_length": len(review_text) if review_text else 0,
        }
    except Exception as e:
        print(f"⚠️ 리뷰 파싱 에러: {e}")
        return None


# ==========================
# 한 영화의 모든 리뷰 수집
# ==========================


async def fetch_reviews_for_title(
    session: aiohttp.ClientSession,
    imdb_id: str,
    title: str = "",
    max_reviews: int | None = None,
) -> list[dict]:
    """
    한 imdb_id(영화)에 대한 리뷰 전부(or 최대 N개) 수집
    """
    all_reviews: list[dict] = []
    after_cursor: str | None = None
    page = 0

    while True:
        page += 1
        url = build_graphql_url(imdb_id, after_cursor)
        data = await fetch_json(session, url)
        if not data:
            break

        title_data = data.get("data", {}).get("title", {})
        reviews_data = title_data.get("reviews", {})
        edges = reviews_data.get("edges", [])
        total = reviews_data.get("total", 0)

        if not edges:
            break

        for edge in edges:
            node = edge.get("node", {})
            review = parse_review_node(node, imdb_id)
            if review:
                all_reviews.append(review)

        # 최대 리뷰 개수 제한
        if max_reviews is not None and len(all_reviews) >= max_reviews:
            all_reviews = all_reviews[:max_reviews]
            break

        page_info = reviews_data.get("pageInfo", {})
        has_next = page_info.get("hasNextPage", False)
        after_cursor = page_info.get("endCursor")

        if not has_next or not after_cursor:
            break

        await asyncio.sleep(0.1)

    if all_reviews:
        print(f"✅ {title} ({imdb_id})  {len(all_reviews):,}/{total}개")
    else:
        print(f"➖ {title} ({imdb_id}) 리뷰 없음")

    return all_reviews


# ==========================
# 메인 크롤링 함수
# ==========================


async def crawl_imdb_reviews(
    input_csv_path: str,
    output_csv_path: str,
    max_titles: int | None = None,
    max_reviews_per_title: int | None = None,
):
    """
    캔 전용 IMDb 리뷰 크롤러

    - input_csv_path : movies_2005_2009_for_reviews.csv
      (id, title, imdb_id, ... 포함)
    - output_csv_path : 저장할 리뷰 CSV 경로
    - max_titles : 테스트용 (앞에서 N편만)
    - max_reviews_per_title : 영화당 최대 리뷰 개수 (None이면 전체)
    """

    t0 = datetime.now()
    print("=" * 70)
    print("🚀 IMDb 리뷰 크롤링 시작")
    print(f"📂 입력: {input_csv_path}")
    print(f"💾 출력: {output_csv_path}")
    print("=" * 70)

    df = pd.read_csv(input_csv_path)
    df = df[df["imdb_id"].notna()].copy()

    # 이미 수집된 imdb_id 스킵 (재시작용)
    done_ids: set[str] = set()
    if Path(output_csv_path).exists():
        print("📌 기존 리뷰 파일 발견 → 이미 수집된 imdb_id 스킵")
        df_done = pd.read_csv(output_csv_path, usecols=["imdb_id"])
        done_ids = set(df_done["imdb_id"].unique())
        print(f"   이미 수집된 영화 수: {len(done_ids)}")

    # 남은 타이틀만 대상
    targets = df[~df["imdb_id"].isin(done_ids)].copy()

    if max_titles is not None:
        targets = targets.head(max_titles)

    print(f"🎯 이번에 수집할 영화 수: {len(targets)}")

    connector = aiohttp.TCPConnector(limit=20, force_close=False)
    all_results: list[dict] = []
    batch_size = 10

    async with aiohttp.ClientSession(connector=connector, timeout=TIMEOUT) as session:
        for start in range(0, len(targets), batch_size):
            batch = targets.iloc[start : start + batch_size]

            tasks = [
                fetch_reviews_for_title(
                    session,
                    row["imdb_id"],
                    row.get("title", ""),
                    max_reviews=max_reviews_per_title,
                )
                for _, row in batch.iterrows()
            ]

            batch_results = await asyncio.gather(*tasks)

            for reviews in batch_results:
                all_results.extend(reviews)

            # 배치마다 저장
            if all_results:
                df_batch = pd.DataFrame(all_results)
                file_exists = Path(output_csv_path).exists()
                df_batch.to_csv(
                    output_csv_path,
                    mode="a",
                    header=not file_exists,
                    index=False,
                    encoding="utf-8-sig",
                )
                print(f"💾 배치 저장: {len(df_batch)}개 리뷰")
                all_results.clear()

            done = min(start + batch_size, len(targets))
            elapsed_min = (datetime.now() - t0).total_seconds() / 60
            print(f"📊 진행 상황: {done}/{len(targets)} (경과 {elapsed_min:.1f}분)")

    print("\n✅ 크롤링 완료")
    total_min = (datetime.now() - t0).total_seconds() / 60
    print(f"⏱️ 총 소요 시간: {total_min:.1f}분")
