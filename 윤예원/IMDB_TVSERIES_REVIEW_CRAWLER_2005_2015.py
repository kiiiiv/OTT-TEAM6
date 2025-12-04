# ==========================================================
# IMDB GraphQL API 리뷰 크롤러 (비동기 고성능)
# HTML 스크래핑 대신 공식 GraphQL API 사용
# 훨씬 빠르고 안정적!
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

# ==========================================================
# 설정
# ==========================================================

# GraphQL API 설정
GRAPHQL_URL = "https://caching.graphql.imdb.com/"
OPERATION_NAME = "TitleReviewsRefine"
PERSISTED_QUERY_HASH = "d389bc70c27f09c00b663705f0112254e8a7c75cde1cfd30e63a2d98c1080c87"

# Rate Limiting (GraphQL은 더 관대함)
MAX_CALLS_PER_SECOND = 2
TIMEOUT = aiohttp.ClientTimeout(total=30, connect=10)
MAX_RETRIES = 3

# 한 번에 가져올 리뷰 수 (25가 최적)
REVIEWS_PER_REQUEST = 25

# User-Agent
USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"

# 출력 파일
OUTPUT_CSV = "imdb_reviews_graphql.csv"
OUTPUT_PARQUET = "imdb_reviews_graphql.parquet"
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
# GraphQL 쿼리 생성
# ==========================================================
def build_graphql_url(imdb_id, after_cursor=None, first=25, sort_by="HELPFULNESS_SCORE"):
    """
    GraphQL API URL 생성
    
    Args:
        imdb_id: IMDB ID (예: tt0944947)
        after_cursor: 페이지네이션 커서 (다음 페이지)
        first: 한 번에 가져올 리뷰 수
        sort_by: 정렬 기준 (HELPFULNESS_SCORE, SUBMISSION_DATE, etc.)
    """
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
    
    # URL 인코딩
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
# GraphQL API 호출
# ==========================================================
async def fetch_graphql(session, url, retry=0):
    """GraphQL API 호출 (재시도 포함)"""
    if retry >= MAX_RETRIES:
        return None
    
    await rate_limiter.acquire()
    stats["requests"] += 1
    
    headers = {
        'User-Agent': USER_AGENT,
        'Accept': 'application/json',
        'Content-Type': 'application/json',
    }
    
    try:
        async with session.get(url, headers=headers, timeout=TIMEOUT) as resp:
            if resp.status == 429:
                wait_time = 5 * (retry + 1)
                print(f"⚠️  Rate limited, waiting {wait_time}s...")
                await asyncio.sleep(wait_time)
                return await fetch_graphql(session, url, retry + 1)
            
            if resp.status != 200:
                if retry < MAX_RETRIES - 1:
                    await asyncio.sleep(2 ** retry)
                    return await fetch_graphql(session, url, retry + 1)
                return None
            
            return await resp.json()
    
    except asyncio.TimeoutError:
        if retry < MAX_RETRIES - 1:
            await asyncio.sleep(2 ** retry)
            return await fetch_graphql(session, url, retry + 1)
        return None
    
    except Exception as e:
        if retry < MAX_RETRIES - 1:
            await asyncio.sleep(2 ** retry)
            return await fetch_graphql(session, url, retry + 1)
        return None

# ==========================================================
# 리뷰 파싱
# ==========================================================
def parse_review_node(node, imdb_id):
    """GraphQL response의 review node 파싱"""
    try:
        # HTML 태그 제거 함수
        def clean_html(text):
            if not text:
                return None
            # HTML 엔티티 디코딩
            text = html.unescape(text)
            # <br/> 태그를 줄바꿈으로
            text = text.replace('<br/>', '\n').replace('<br>', '\n')
            # 나머지 HTML 태그 제거
            import re
            text = re.sub('<[^<]+?>', '', text)
            return text.strip()
        
        # 기본 정보
        review_id = node.get('id')
        
        # 작성자
        author_data = node.get('author', {})
        username = author_data.get('username', {}).get('text')
        user_id = author_data.get('userId')
        
        # 평점
        author_rating = node.get('authorRating')
        
        # Helpful 투표
        helpfulness = node.get('helpfulness', {})
        up_votes = helpfulness.get('upVotes', 0)
        down_votes = helpfulness.get('downVotes', 0)
        
        # 날짜
        submission_date = node.get('submissionDate')
        
        # 제목
        summary = node.get('summary', {})
        review_title = summary.get('originalText')
        
        # 내용
        text_data = node.get('text', {}).get('originalText', {})
        review_text_html = text_data.get('plaidHtml')
        review_text = clean_html(review_text_html)
        
        # Spoiler 여부
        is_spoiler = node.get('spoiler', False)
        
        return {
            'imdb_id': imdb_id,
            'review_id': review_id,
            'username': username,
            'user_id': user_id,
            'author_rating': author_rating,
            'helpful_up_votes': up_votes,
            'helpful_down_votes': down_votes,
            'helpful_total': up_votes + down_votes,
            'helpful_ratio': round(up_votes / (up_votes + down_votes), 3) if (up_votes + down_votes) > 0 else None,
            'submission_date': submission_date,
            'review_title': review_title,
            'review_text': review_text,
            'review_text_length': len(review_text) if review_text else 0,
            'is_spoiler': is_spoiler,
        }
    
    except Exception as e:
        print(f"⚠️  Error parsing review node: {e}")
        return None

# ==========================================================
# 한 시리즈의 모든 리뷰 수집
# ==========================================================
async def fetch_all_reviews_for_series(session, imdb_id, series_title="", max_reviews=None):
    """
    한 시리즈의 모든 리뷰 수집 (GraphQL API 사용)
    
    Args:
        session: aiohttp session
        imdb_id: IMDB ID
        series_title: 시리즈 제목 (로깅용)
        max_reviews: 최대 리뷰 수 (None이면 전체)
    
    Returns:
        list: 리뷰 리스트
    """
    all_reviews = []
    after_cursor = None
    page = 0
    
    try:
        while True:
            page += 1
            
            # GraphQL URL 생성
            url = build_graphql_url(imdb_id, after_cursor, REVIEWS_PER_REQUEST)
            
            # API 호출
            response = await fetch_graphql(session, url)
            
            if not response:
                break
            
            # 데이터 추출
            data = response.get('data', {})
            title_data = data.get('title', {})
            reviews_data = title_data.get('reviews', {})
            
            # 총 리뷰 수
            total_reviews = reviews_data.get('total', 0)
            
            # 리뷰 파싱
            edges = reviews_data.get('edges', [])
            if not edges:
                break
            
            for edge in edges:
                node = edge.get('node', {})
                review = parse_review_node(node, imdb_id)
                if review:
                    all_reviews.append(review)
            
            # 최대 리뷰 수 체크
            if max_reviews and len(all_reviews) >= max_reviews:
                all_reviews = all_reviews[:max_reviews]
                break
            
            # 다음 페이지 커서
            page_info = reviews_data.get('pageInfo', {})
            has_next_page = page_info.get('hasNextPage', False)
            after_cursor = page_info.get('endCursor')
            
            if not has_next_page or not after_cursor:
                break
            
            # 짧은 대기
            await asyncio.sleep(0.1)
        
        stats["reviews_total"] += len(all_reviews)
        
        if all_reviews:
            print(f"✅ {series_title} ({imdb_id}): {len(all_reviews):,}/{total_reviews} 리뷰")
        
        return all_reviews
    
    except Exception as e:
        print(f"❌ {series_title} ({imdb_id}): {str(e)[:100]}")
        return all_reviews

# ==========================================================
# 체크포인트 관리
# ==========================================================
def save_checkpoint(processed_ids):
    """중간 저장"""
    # stats 복사 (datetime을 문자열로 변환)
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
    """체크포인트 로드 + 기존 CSV에서 처리된 ID 로드"""
    processed_ids = set()
    
    # 1. 체크포인트 파일에서 로드 (에러 처리 추가)
    if Path(CHECKPOINT_FILE).exists():
        try:
            with open(CHECKPOINT_FILE, 'r') as f:
                checkpoint = json.load(f)
                processed_ids.update(checkpoint.get('processed_ids', []))
                print(f"📌 체크포인트에서 {len(checkpoint.get('processed_ids', [])):,}개 ID 로드")
        except (json.JSONDecodeError, Exception) as e:
            print(f"⚠️  체크포인트 파일 손상됨, 삭제하고 계속 진행: {e}")
            try:
                Path(CHECKPOINT_FILE).unlink()
            except:
                pass
    
    # 2. 기존 CSV 파일에서 로드 (중복 방지)
    if Path(OUTPUT_CSV).exists():
        try:
            df_existing = pd.read_csv(OUTPUT_CSV)
            if 'imdb_id' in df_existing.columns:
                existing_ids = df_existing['imdb_id'].unique()
                processed_ids.update(existing_ids)
                print(f"📌 기존 CSV에서 {len(existing_ids):,}개 시리즈 발견")
        except Exception as e:
            print(f"⚠️  기존 CSV 로드 실패: {e}")
    
    return processed_ids

# ==========================================================
# 메인 실행
# ==========================================================
async def main(input_csv_path, vote_threshold=30, max_reviews_per_series=None):
    """
    전체 리뷰 수집 (GraphQL API)
    
    Args:
        input_csv_path: TMDB CSV 파일 경로
        vote_threshold: 최소 vote_count
        max_reviews_per_series: 시리즈당 최대 리뷰 수 (None이면 전체)
    """
    print("=" * 90)
    print("🚀 IMDB GraphQL API 리뷰 크롤러")
    print("✨ HTML 스크래핑보다 5-10배 빠르고 안정적!")
    print("=" * 90)
    
    stats["start_time"] = datetime.now()
    t0 = datetime.now()
    
    # 1. 데이터 로드
    print("\n📂 데이터 로드 중...")
    df = pd.read_csv(input_csv_path)
    df_filtered = df[(df['vote_count'] >= vote_threshold) & (df['imdb_id'].notna())]
    
    print(f"✅ 전체 시리즈: {len(df):,}개")
    print(f"✅ 필터링 (vote_count>={vote_threshold} & imdb_id 존재): {len(df_filtered):,}개")
    
    if len(df_filtered) == 0:
        print("⚠️  조건을 만족하는 데이터가 없습니다.")
        return
    
    # 2. 체크포인트 로드
    processed_ids = load_checkpoint()
    series_list = df_filtered[['id', 'title', 'imdb_id']].to_dict('records')
    
    if processed_ids:
        print(f"📌 체크포인트 로드: {len(processed_ids):,}개 처리 완료")
        series_list = [s for s in series_list if s['imdb_id'] not in processed_ids]
        print(f"📌 남은 작업: {len(series_list):,}개")
    
    if len(series_list) == 0:
        print("✅ 모든 데이터가 이미 처리되었습니다.")
        return
    
    stats["series_total"] = len(series_list)
    
    # 3. 크롤링
    print(f"\n🚀 크롤링 시작")
    print(f"⚙️  Rate Limit: {MAX_CALLS_PER_SECOND}회/초")
    print(f"⚙️  리뷰/요청: {REVIEWS_PER_REQUEST}개")
    
    if max_reviews_per_series:
        print(f"⚙️  시리즈당 최대: {max_reviews_per_series}개")
        estimated_time = len(series_list) * (max_reviews_per_series / REVIEWS_PER_REQUEST / MAX_CALLS_PER_SECOND) / 60
    else:
        print(f"⚙️  시리즈당 최대: 전체")
        estimated_time = len(series_list) * 5  # 평균 5분 추정
    
    print(f"⏱️  예상 시간: {estimated_time:.0f}분")
    
    connector = aiohttp.TCPConnector(
        limit=20,
        force_close=False,
        enable_cleanup_closed=True
    )
    
    all_results = []
    batch_size = 10
    
    async with aiohttp.ClientSession(connector=connector, timeout=TIMEOUT) as session:
        for i in range(0, len(series_list), batch_size):
            batch = series_list[i:i+batch_size]
            
            # 배치 처리
            tasks = [
                fetch_all_reviews_for_series(
                    session,
                    s['imdb_id'],
                    s['title'],
                    max_reviews_per_series
                )
                for s in batch
            ]
            
            batch_results = await asyncio.gather(*tasks, return_exceptions=True)
            
            # 결과 수집
            for series, reviews in zip(batch, batch_results):
                if isinstance(reviews, list) and reviews:
                    all_results.extend(reviews)
                    processed_ids.add(series['imdb_id'])
                    stats["series_success"] += 1
                elif isinstance(reviews, list):
                    processed_ids.add(series['imdb_id'])
                    stats["series_failed"] += 1
                else:
                    stats["series_failed"] += 1
            
            # 주기적 저장
            if (i + batch_size) % 50 == 0:
                save_checkpoint(processed_ids)
                
                # CSV 저장 (append 모드)
                if all_results:
                    df_batch = pd.DataFrame(all_results)
                    # 중복 제거
                    df_batch = df_batch.drop_duplicates(subset=['review_id'])
                    
                    # 파일 존재 여부에 따라 header 설정
                    file_exists = Path(OUTPUT_CSV).exists()
                    df_batch.to_csv(OUTPUT_CSV, mode='a', header=not file_exists, index=False, encoding='utf-8-sig')
                    
                    # 저장 후 메모리에서 제거 (메모리 절약)
                    all_results.clear()
                    print(f"💾 중간 저장 완료 ({len(df_batch):,}개 리뷰)")

            
            # 진행 상황
            elapsed = (datetime.now() - t0).total_seconds() / 60
            progress = stats["series_success"] + stats["series_failed"]
            rate = progress / elapsed if elapsed > 0 else 0
            eta = (stats["series_total"] - progress) / rate if rate > 0 else 0
            
            print(f"\n📊 진행: {progress}/{stats['series_total']} ({progress/stats['series_total']*100:.1f}%) | "
                  f"성공: {stats['series_success']} | 실패: {stats['series_failed']} | "
                  f"총 리뷰: {stats['reviews_total']:,}개 | "
                  f"요청: {stats['requests']:,}회 | "
                  f"속도: {rate:.1f}/분 | ETA: {eta:.0f}분\n")
    
    # 4. 최종 저장
    print("\n💾 최종 저장 중...")
    
    # 남은 결과 저장
    if all_results:
        df_batch = pd.DataFrame(all_results)
        df_batch = df_batch.drop_duplicates(subset=['review_id'])
        file_exists = Path(OUTPUT_CSV).exists()
        df_batch.to_csv(OUTPUT_CSV, mode='a', header=not file_exists, index=False, encoding='utf-8-sig')
    
    # 전체 CSV 로드 및 중복 제거
    if Path(OUTPUT_CSV).exists():
        df_results = pd.read_csv(OUTPUT_CSV)
        df_results = df_results.drop_duplicates(subset=['review_id'])
        # 중복 제거 후 다시 저장
        df_results.to_csv(OUTPUT_CSV, index=False, encoding='utf-8-sig')
    else:
        df_results = pd.DataFrame()
    
    try:
        if not df_results.empty:
            df_results.to_parquet(OUTPUT_PARQUET, index=False)
    except Exception as e:
        print(f"⚠️  Parquet 저장 실패: {e}")

    
    if Path(CHECKPOINT_FILE).exists():
        Path(CHECKPOINT_FILE).unlink()
    
    # 5. 통계
    elapsed = (datetime.now() - t0).total_seconds() / 60
    
    print("\n" + "=" * 90)
    print("🎉 크롤링 완료!")
    print("=" * 90)
    print(f"📌 시리즈: {stats['series_success']:,}/{stats['series_total']:,}개 성공")
    
    if not df_results.empty:
        print(f"📌 총 리뷰: {len(df_results):,}개 (중복 제거 후)")
        print(f"📌 평균: {len(df_results)/stats['series_success']:.1f}개/시리즈" if stats['series_success'] > 0 else "")
    else:
        print(f"📌 총 리뷰: 0개")
    
    print(f"📌 총 요청: {stats['requests']:,}회")
    print(f"⏱️  총 시간: {elapsed:.1f}분 ({elapsed/60:.2f}시간)")
    
    if stats['series_success'] > 0:
        print(f"📊 속도: {stats['series_success']/elapsed:.1f}개/분")
        if not df_results.empty:
            print(f"📊 리뷰 수집 속도: {len(df_results)/elapsed:.0f}개/분")
    
    print("=" * 90)
    
    # 샘플
    if not df_results.empty:
        print("\n📊 샘플 데이터:")
        print(df_results.head(3).to_string())
        print(f"\n✅ 결과 파일: {OUTPUT_CSV}")
        
        # 통계 정보
        print("\n📈 리뷰 통계:")
        if 'author_rating' in df_results.columns:
            print(f"  평균 평점: {df_results['author_rating'].mean():.2f}/10")
        if 'review_text_length' in df_results.columns:
            print(f"  평균 텍스트 길이: {df_results['review_text_length'].mean():.0f}자")
        if 'is_spoiler' in df_results.columns:
            print(f"  Spoiler 리뷰: {df_results['is_spoiler'].sum():,}개 ({df_results['is_spoiler'].sum()/len(df_results)*100:.1f}%)")
        if 'helpful_total' in df_results.columns:
            print(f"  평균 helpful 투표: {df_results['helpful_total'].mean():.1f}개")
    else:
        print("\n⚠️  수집된 리뷰가 없습니다.")

# ==========================================================
# 실행
# ==========================================================
if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='IMDB GraphQL API 리뷰 크롤러')
    parser.add_argument('--input', '-i', default='tv_series_2005_2015_FULL.csv',
                        help='입력 CSV 파일')
    parser.add_argument('--vote', '-v', type=int, default=30,
                        help='최소 vote_count (기본: 30)')
    parser.add_argument('--max-reviews', '-m', type=int, default=None,
                        help='시리즈당 최대 리뷰 수 (기본: 전체)')
    
    args = parser.parse_args()
    
    if not Path(args.input).exists():
        print(f"❌ 파일을 찾을 수 없습니다: {args.input}")
    else:
        asyncio.run(main(args.input, args.vote, args.max_reviews))
