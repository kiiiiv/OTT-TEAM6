# ==========================================================
# IMDB FULL DATA COLLECTOR
# - GraphQL Reviews (전체 페이지)
# - IMDB Rating (JSON-LD)
# - Metascore (JSON-LD 또는 <span class="score-meta">)
# - 대상: imdb_id 보유 & vote_count >= 30 TV Series
#   입력 파일: tv_series_2016_2025_FULL.csv
# ==========================================================

import asyncio
import aiohttp
import pandas as pd
import json
from datetime import datetime
from pathlib import Path
import time
from urllib.parse import quote
import html
import re
import argparse

# ==========================================================
# 설정
# ==========================================================

# GraphQL API 설정
GRAPHQL_URL = "https://caching.graphql.imdb.com/"
OPERATION_NAME = "TitleReviewsRefine"
PERSISTED_QUERY_HASH = "d389bc70c27f09c00b663705f0112254e8a7c75cde1cfd30e63a2d98c1080c87"

# Rate Limiting (GraphQL + HTML 통합)
MAX_CALLS_PER_SECOND = 2
TIMEOUT = aiohttp.ClientTimeout(total=30, connect=10)
MAX_RETRIES = 3

# 한 번에 가져올 리뷰 수
REVIEWS_PER_REQUEST = 25

# User-Agent
USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/120.0.0.0 Safari/537.36"
)

# 출력 파일
OUTPUT_REVIEWS_CSV = "imdb_reviews_graphql.csv"
OUTPUT_TITLES_CSV = "imdb_title_stats.csv"
OUTPUT_REVIEWS_PARQUET = "imdb_reviews_graphql.parquet"
CHECKPOINT_FILE = "imdb_graphql_checkpoint.json"

# 통계
stats = {
    "series_total": 0,
    "series_success": 0,
    "series_failed": 0,
    "reviews_total": 0,
    "requests": 0,
    "start_time": None
}

# ==========================================================
# Rate Limiter
# ==========================================================
class RateLimiter:
    def __init__(self, rate):
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

# ==========================================================
# 공통 HTTP 호출 함수
# ==========================================================
async def get_json(session, url, retry=0):
    """JSON 응답용 (GraphQL 등)"""
    if retry >= MAX_RETRIES:
        return None
    
    await rate_limiter.acquire()
    stats["requests"] += 1

    headers = {
        "User-Agent": USER_AGENT,
        "Accept": "application/json",
        "Content-Type": "application/json",
    }

    try:
        async with session.get(url, headers=headers, timeout=TIMEOUT) as resp:
            if resp.status == 429 and retry < MAX_RETRIES - 1:
                wait_time = 5 * (retry + 1)
                print(f"⚠️  Rate limited, waiting {wait_time}s...")
                await asyncio.sleep(wait_time)
                return await get_json(session, url, retry + 1)
            
            if resp.status != 200:
                if retry < MAX_RETRIES - 1:
                    await asyncio.sleep(2 ** retry)
                    return await get_json(session, url, retry + 1)
                return None
            
            return await resp.json()
    except asyncio.TimeoutError:
        if retry < MAX_RETRIES - 1:
            await asyncio.sleep(2 ** retry)
            return await get_json(session, url, retry + 1)
        return None
    except Exception:
        if retry < MAX_RETRIES - 1:
            await asyncio.sleep(2 ** retry)
            return await get_json(session, url, retry + 1)
        return None


async def get_html(session, url, retry=0):
    """IMDB title HTML용"""
    if retry >= MAX_RETRIES:
        return None
    
    await rate_limiter.acquire()
    stats["requests"] += 1

    headers = {
        "User-Agent": USER_AGENT,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
    }

    try:
        async with session.get(url, headers=headers, timeout=TIMEOUT) as resp:
            if resp.status == 429 and retry < MAX_RETRIES - 1:
                wait_time = 5 * (retry + 1)
                print(f"⚠️  HTML Rate limited, waiting {wait_time}s...")
                await asyncio.sleep(wait_time)
                return await get_html(session, url, retry + 1)
            
            if resp.status != 200:
                if retry < MAX_RETRIES - 1:
                    await asyncio.sleep(2 ** retry)
                    return await get_html(session, url, retry + 1)
                return None
            
            return await resp.text()
    except asyncio.TimeoutError:
        if retry < MAX_RETRIES - 1:
            await asyncio.sleep(2 ** retry)
            return await get_html(session, url, retry + 1)
        return None
    except Exception:
        if retry < MAX_RETRIES - 1:
            await asyncio.sleep(2 ** retry)
            return await get_html(session, url, retry + 1)
        return None

# ==========================================================
# GraphQL 쿼리 생성 (리뷰)
# ==========================================================
def build_graphql_url(imdb_id, after_cursor=None, first=25, sort_by="HELPFULNESS_SCORE"):
    variables = {
        "const": imdb_id,
        "first": first,
        "locale": "en-US",
        "sort": {
            "by": sort_by,
            "order": "DESC"
        },
        "filter": {}
    }
    
    if after_cursor:
        variables["after"] = after_cursor
    
    extensions = {
        "persistedQuery": {
            "sha256Hash": PERSISTED_QUERY_HASH,
            "version": 1
        }
    }
    
    variables_json = json.dumps(variables, separators=(',', ':'))
    extensions_json = json.dumps(extensions, separators=(',', ':'))
    
    url = (
        f"{GRAPHQL_URL}?"
        f"operationName={OPERATION_NAME}"
        f"&variables={quote(variables_json)}"
        f"&extensions={quote(extensions_json)}"
    )
    
    return url

# ==========================================================
# 리뷰 파싱
# ==========================================================
def clean_html_text(text):
    if not text:
        return None
    text = html.unescape(text)
    text = text.replace('<br/>', '\n').replace('<br>', '\n')
    text = re.sub('<[^<]+?>', '', text)
    return text.strip()

def parse_review_node(node, imdb_id):
    try:
        # 텍스트
        text_data = node.get('text', {}).get('originalText', {})
        review_text_html = text_data.get('plaidHtml')
        review_text = clean_html_text(review_text_html)

        # helpful
        helpfulness = node.get('helpfulness', {})
        up_votes = helpfulness.get('upVotes', 0)
        down_votes = helpfulness.get('downVotes', 0)
        total_votes = up_votes + down_votes

        # 작성자
        author = node.get('author', {})
        username = author.get('username', {}).get('text')
        user_id = author.get('userId')

        # 메타데이터
        summary = node.get('summary', {})
        review_title = summary.get('originalText')

        return {
            'imdb_id': imdb_id,
            'review_id': node.get('id'),
            'username': username,
            'user_id': user_id,
            'author_rating': node.get('authorRating'),
            'helpful_up_votes': up_votes,
            'helpful_down_votes': down_votes,
            'helpful_total': total_votes,
            'helpful_ratio': round(up_votes / total_votes, 3) if total_votes > 0 else None,
            'submission_date': node.get('submissionDate'),
            'review_title': review_title,
            'review_text': review_text,
            'review_text_length': len(review_text) if review_text else 0,
            'is_spoiler': node.get('spoiler', False),
        }
    except Exception as e:
        print(f"⚠️  Error parsing review node: {e}")
        return None

# ==========================================================
# 1) 한 시리즈의 리뷰 전체 수집
# ==========================================================
async def fetch_all_reviews_for_series(session, imdb_id, series_title="", max_reviews=None):
    all_reviews = []
    after_cursor = None
    page = 0
    
    try:
        while True:
            page += 1
            url = build_graphql_url(imdb_id, after_cursor, REVIEWS_PER_REQUEST)
            response = await get_json(session, url)
            
            if not response:
                break
            
            data = response.get('data', {})
            title_data = data.get('title', {})
            reviews_data = title_data.get('reviews', {})
            total_reviews = reviews_data.get('total', 0)
            
            edges = reviews_data.get('edges', [])
            if not edges:
                break
            
            for edge in edges:
                node = edge.get('node', {})
                review = parse_review_node(node, imdb_id)
                if review:
                    all_reviews.append(review)
            
            if max_reviews and len(all_reviews) >= max_reviews:
                all_reviews = all_reviews[:max_reviews]
                break
            
            page_info = reviews_data.get('pageInfo', {})
            has_next_page = page_info.get('hasNextPage', False)
            after_cursor = page_info.get('endCursor')
            
            if not has_next_page or not after_cursor:
                break
            
            await asyncio.sleep(0.1)
        
        stats["reviews_total"] += len(all_reviews)
        
        if all_reviews:
            print(f"✅ [Reviews] {series_title} ({imdb_id}): {len(all_reviews):,}/{total_reviews}개 수집")
        
        return all_reviews
    
    except Exception as e:
        print(f"❌ [Reviews] {series_title} ({imdb_id}): {str(e)[:100]}")
        return all_reviews

# ==========================================================
# 2) IMDB HTML에서 Rating + Metascore 추출
# ==========================================================
def parse_title_stats_from_html(imdb_id, html_text):
    """
    IMDB title HTML에서
    - ratingValue, ratingCount (JSON-LD)
    - metascore (metacritic.score 또는 <span class="score-meta">)
    를 추출
    """
    imdb_rating = None
    imdb_rating_count = None
    metascore = None

    # 1) JSON-LD 블록 추출
    ld_match = re.search(
        r'<script type="application/ld\+json">(.*?)</script>',
        html_text,
        re.S
    )
    if ld_match:
        try:
            data = json.loads(ld_match.group(1))
            agg = data.get("aggregateRating", {})
            imdb_rating = agg.get("ratingValue")
            imdb_rating_count = agg.get("ratingCount")

            # 일부 페이지는 JSON 안에 metacritic 정보 포함
            mc = data.get("metacritic") or {}
            if isinstance(mc, dict):
                metascore = mc.get("score", metascore)
        except Exception as e:
            print(f"⚠️  JSON-LD parse error ({imdb_id}): {e}")

    # 2) HTML span.score-meta Fallback
    if metascore is None:
        ms_match = re.search(r'<span class="score-meta">(\d+)</span>', html_text)
        if ms_match:
            metascore = ms_match.group(1)

    return {
        "imdb_id": imdb_id,
        "imdb_rating": imdb_rating,
        "imdb_rating_count": imdb_rating_count,
        "metascore": metascore,
    }

async def fetch_imdb_title_stats(session, imdb_id):
    url = f"https://www.imdb.com/title/{imdb_id}/"
    html_text = await get_html(session, url)
    if not html_text:
        print(f"⚠️  [Title] {imdb_id}: HTML 가져오기 실패")
        return {
            "imdb_id": imdb_id,
            "imdb_rating": None,
            "imdb_rating_count": None,
            "metascore": None,
        }
    return parse_title_stats_from_html(imdb_id, html_text)

# ==========================================================
# 체크포인트 관리
# ==========================================================
def save_checkpoint(processed_ids):
    stats_copy = stats.copy()
    if stats_copy.get('start_time'):
        stats_copy['start_time'] = stats_copy['start_time'].isoformat()
    
    checkpoint = {
        'processed_ids': list(processed_ids),
        'stats': stats_copy,
        'timestamp': datetime.now().isoformat()
    }
    with open(CHECKPOINT_FILE, 'w') as f:
        json.dump(checkpoint, f)

def load_checkpoint():
    processed_ids = set()
    
    # 1) 체크포인트 파일
    if Path(CHECKPOINT_FILE).exists():
        try:
            with open(CHECKPOINT_FILE, 'r') as f:
                checkpoint = json.load(f)
                processed_ids.update(checkpoint.get('processed_ids', []))
                print(f"📌 체크포인트에서 {len(checkpoint.get('processed_ids', [])):,}개 ID 로드")
        except (json.JSONDecodeError, Exception) as e:
            print(f"⚠️  체크포인트 파일 손상됨, 삭제하고 진행: {e}")
            try:
                Path(CHECKPOINT_FILE).unlink()
            except:
                pass
    
    # 2) 기존 리뷰 CSV 기준 (이미 수집된 imdb_id)
    if Path(OUTPUT_REVIEWS_CSV).exists():
        try:
            df_existing = pd.read_csv(OUTPUT_REVIEWS_CSV)
            if 'imdb_id' in df_existing.columns:
                existing_ids = df_existing['imdb_id'].unique()
                processed_ids.update(existing_ids)
                print(f"📌 기존 리뷰 CSV에서 {len(existing_ids):,}개 시리즈 발견")
        except Exception as e:
            print(f"⚠️  기존 리뷰 CSV 로드 실패: {e}")
    
    return processed_ids

# ==========================================================
# 메인 실행
# ==========================================================
async def main(input_csv_path, vote_threshold=30, max_reviews_per_series=None):
    print("=" * 90)
    print("🚀 IMDB FULL DATA COLLECTOR (Reviews + Rating + Metascore)")
    print("=" * 90)
    
    stats["start_time"] = datetime.now()
    t0 = datetime.now()
    
    # 1. 데이터 로드
    print("\n📂 데이터 로드 중...")
    df = pd.read_csv(input_csv_path)
    df_filtered = df[(df['vote_count'] >= vote_threshold) & (df['imdb_id'].notna())]
    df_filtered = df_filtered.drop_duplicates(subset=['imdb_id'])
    
    print(f"✅ 전체 시리즈: {len(df):,}개")
    print(f"✅ 필터링 (vote_count>={vote_threshold} & imdb_id 존재): {len(df_filtered):,}개")
    
    if len(df_filtered) == 0:
        print("⚠️  조건을 만족하는 데이터가 없습니다.")
        return
    
    # 2. 체크포인트 로드
    processed_ids = load_checkpoint()
    series_list = df_filtered[['id', 'title', 'imdb_id']].to_dict('records')
    
    if processed_ids:
        print(f"📌 체크포인트 기준 이미 처리된 시리즈: {len(processed_ids):,}개")
        series_list = [s for s in series_list if s['imdb_id'] not in processed_ids]
        print(f"📌 남은 작업: {len(series_list):,}개")
    
    if len(series_list) == 0:
        print("✅ 모든 데이터가 이미 처리되었습니다.")
        return
    
    stats["series_total"] = len(series_list)
    
    # 3. 크롤링 설정
    print(f"\n🚀 크롤링 시작")
    print(f"⚙️  Rate Limit: {MAX_CALLS_PER_SECOND}회/초")
    print(f"⚙️  리뷰/요청: {REVIEWS_PER_REQUEST}개")
    
    if max_reviews_per_series:
        print(f"⚙️  시리즈당 최대 리뷰: {max_reviews_per_series}개")
        estimated_time = len(series_list) * (max_reviews_per_series / REVIEWS_PER_REQUEST / MAX_CALLS_PER_SECOND) / 60
    else:
        print(f"⚙️  시리즈당 최대 리뷰: 전체")
        estimated_time = len(series_list) * 5  # 대략 추정
    
    print(f"⏱️  러프 예상 시간: {estimated_time:.0f}분")
    
    connector = aiohttp.TCPConnector(
        limit=20,
        force_close=False,
        enable_cleanup_closed=True
    )
    
    all_reviews_results = []
    all_title_stats_results = []
    batch_size = 10
    
    async with aiohttp.ClientSession(connector=connector, timeout=TIMEOUT) as session:
        for i in range(0, len(series_list), batch_size):
            batch = series_list[i:i+batch_size]
            
            # 각 시리즈별 (리뷰 + 타이틀 통계) 동시 처리
            async def process_series(s):
                imdb_id = s['imdb_id']
                title = s['title']
                reviews, title_stats = await asyncio.gather(
                    fetch_all_reviews_for_series(
                        session,
                        imdb_id,
                        title,
                        max_reviews_per_series
                    ),
                    fetch_imdb_title_stats(session, imdb_id)
                )
                return s, reviews, title_stats
            
            tasks = [process_series(s) for s in batch]
            batch_results = await asyncio.gather(*tasks, return_exceptions=True)
            
            for result in batch_results:
                if isinstance(result, Exception):
                    stats["series_failed"] += 1
                    continue
                
                series, reviews, title_stats = result
                imdb_id = series['imdb_id']
                
                # 리뷰 수집 결과
                if isinstance(reviews, list) and reviews:
                    all_reviews_results.extend(reviews)
                    processed_ids.add(imdb_id)
                    stats["series_success"] += 1
                elif isinstance(reviews, list):
                    # 리뷰 0개지만 시리즈 자체는 처리 완료로 간주
                    processed_ids.add(imdb_id)
                    stats["series_success"] += 1
                else:
                    stats["series_failed"] += 1
                
                # 타이틀 통계 결과
                if isinstance(title_stats, dict):
                    all_title_stats_results.append(title_stats)
            
            # 주기적 저장
            if (i + batch_size) % 50 == 0:
                # 체크포인트
                save_checkpoint(processed_ids)
                
                # 리뷰 중간 저장
                if all_reviews_results:
                    df_batch = pd.DataFrame(all_reviews_results)
                    df_batch = df_batch.drop_duplicates(subset=['review_id'])
                    file_exists = Path(OUTPUT_REVIEWS_CSV).exists()
                    df_batch.to_csv(
                        OUTPUT_REVIEWS_CSV,
                        mode='a',
                        header=not file_exists,
                        index=False,
                        encoding='utf-8-sig'
                    )
                    all_reviews_results.clear()
                    print(f"💾 리뷰 중간 저장 완료 ({len(df_batch):,}개)")
                
                # 타이틀 통계 중간 저장
                if all_title_stats_results:
                    df_titles_batch = pd.DataFrame(all_title_stats_results)
                    df_titles_batch = df_titles_batch.drop_duplicates(subset=['imdb_id'])
                    file_exists_titles = Path(OUTPUT_TITLES_CSV).exists()
                    df_titles_batch.to_csv(
                        OUTPUT_TITLES_CSV,
                        mode='a',
                        header=not file_exists_titles,
                        index=False,
                        encoding='utf-8-sig'
                    )
                    all_title_stats_results.clear()
                    print(f"💾 타이틀 통계 중간 저장 완료 ({len(df_titles_batch):,}개)")
            
            # 진행 상황 출력
            elapsed = (datetime.now() - t0).total_seconds() / 60
            progress = stats["series_success"] + stats["series_failed"]
            rate = progress / elapsed if elapsed > 0 else 0
            eta = (stats["series_total"] - progress) / rate if rate > 0 else 0
            
            print(
                f"\n📊 진행: {progress}/{stats['series_total']} "
                f"({progress/stats['series_total']*100:.1f}%) | "
                f"성공: {stats['series_success']} | 실패: {stats['series_failed']} | "
                f"총 리뷰: {stats['reviews_total']:,}개 | "
                f"요청: {stats['requests']:,}회 | "
                f"속도: {rate:.1f}시리즈/분 | ETA: {eta:.0f}분\n"
            )
    
    # 4. 최종 저장
    print("\n💾 최종 저장 중...")

    # 남은 리뷰 저장
    if all_reviews_results:
        df_batch = pd.DataFrame(all_reviews_results)
        df_batch = df_batch.drop_duplicates(subset=['review_id'])
        file_exists = Path(OUTPUT_REVIEWS_CSV).exists()
        df_batch.to_csv(
            OUTPUT_REVIEWS_CSV,
            mode='a',
            header=not file_exists,
            index=False,
            encoding='utf-8-sig'
        )

    # 남은 타이틀 통계 저장
    if all_title_stats_results:
        df_titles_batch = pd.DataFrame(all_title_stats_results)
        df_titles_batch = df_titles_batch.drop_duplicates(subset=['imdb_id'])
        file_exists_titles = Path(OUTPUT_TITLES_CSV).exists()
        df_titles_batch.to_csv(
            OUTPUT_TITLES_CSV,
            mode='a',
            header=not file_exists_titles,
            index=False,
            encoding='utf-8-sig'
        )

    # 리뷰 전체 중복 제거
    if Path(OUTPUT_REVIEWS_CSV).exists():
        df_results = pd.read_csv(OUTPUT_REVIEWS_CSV)
        df_results = df_results.drop_duplicates(subset=['review_id'])
        df_results.to_csv(OUTPUT_REVIEWS_CSV, index=False, encoding='utf-8-sig')
    else:
        df_results = pd.DataFrame()

    # 타이틀 통계 중복 제거
    if Path(OUTPUT_TITLES_CSV).exists():
        df_titles = pd.read_csv(OUTPUT_TITLES_CSV)
        df_titles = df_titles.drop_duplicates(subset=['imdb_id'])
        df_titles.to_csv(OUTPUT_TITLES_CSV, index=False, encoding='utf-8-sig')
    else:
        df_titles = pd.DataFrame()
    
    # parquet 저장 (리뷰만)
    try:
        if not df_results.empty:
            df_results.to_parquet(OUTPUT_REVIEWS_PARQUET, index=False)
    except Exception as e:
        print(f"⚠️  Parquet 저장 실패: {e}")

    # 체크포인트 제거
    if Path(CHECKPOINT_FILE).exists():
        Path(CHECKPOINT_FILE).unlink()
    
    # 5. 최종 통계 출력
    elapsed = (datetime.now() - t0).total_seconds() / 60
    
    print("\n" + "=" * 90)
    print("🎉 크롤링 완료!")
    print("=" * 90)
    print(f"📌 시리즈: {stats['series_success']:,}/{stats['series_total']:,}개 성공")
    
    if not df_results.empty:
        print(f"📌 총 리뷰: {len(df_results):,}개 (중복 제거 후)")
        if stats['series_success'] > 0:
            print(f"📌 평균 리뷰: {len(df_results)/stats['series_success']:.1f}개/시리즈")
    else:
        print("📌 총 리뷰: 0개")
    
    if not df_titles.empty:
        print(f"📌 타이틀 통계: {len(df_titles):,}개 시리즈")
    
    print(f"📌 총 요청: {stats['requests']:,}회")
    print(f"⏱️  총 시간: {elapsed:.1f}분 ({elapsed/60:.2f}시간)")
    
    if stats['series_success'] > 0 and elapsed > 0:
        print(f"📊 속도: {stats['series_success']/elapsed:.1f}시리즈/분")
        if not df_results.empty:
            print(f"📊 리뷰 수집 속도: {len(df_results)/elapsed:.0f}개/분")
    
    print("=" * 90)
    
    # 샘플 출력
    if not df_results.empty:
        print("\n📊 리뷰 샘플:")
        print(df_results.head(3).to_string())
        print(f"\n✅ 리뷰 결과 파일: {OUTPUT_REVIEWS_CSV}")
    
    if not df_titles.empty:
        print("\n📊 타이틀 통계 샘플:")
        print(df_titles.head(3).to_string())
        print(f"\n✅ 타이틀 결과 파일: {OUTPUT_TITLES_CSV}")

# ==========================================================
# 실행
# ==========================================================
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='IMDB FULL DATA COLLECTOR')
    parser.add_argument('--input', '-i', default='tv_series_2016_2025_FULL.csv',
                        help='입력 CSV 파일 (기본: tv_series_2016_2025_FULL.csv)')
    parser.add_argument('--vote', '-v', type=int, default=30,
                        help='최소 vote_count (기본: 30)')
    parser.add_argument('--max-reviews', '-m', type=int, default=None,
                        help='시리즈당 최대 리뷰 수 (기본: 전체)')
    
    args = parser.parse_args()
    
    if not Path(args.input).exists():
        print(f"❌ 파일을 찾을 수 없습니다: {args.input}")
    else:
        asyncio.run(main(args.input, args.vote, args.max_reviews))
