# ==========================================================
# IMDB RATING COLLECTOR (2005-2015)
# - IMDB Rating & Rating Count만 수집
# - 대상: 2005-01-01 ~ 2015-12-31 TV Series
# ==========================================================

import asyncio
import aiohttp
import pandas as pd
import json
from datetime import datetime
from pathlib import Path
import time
import re
import argparse

# ==========================================================
# 설정
# ==========================================================

# Rate Limiting
MAX_CALLS_PER_SECOND = 2
TIMEOUT = aiohttp.ClientTimeout(total=30, connect=10)
MAX_RETRIES = 3

# User-Agent
USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/120.0.0.0 Safari/537.36"
)

# 출력 파일
OUTPUT_CSV = "imdb_ratings_2005_2015.csv"
CHECKPOINT_FILE = "imdb_rating_checkpoint.json"

# 통계
stats = {
    "series_total": 0,
    "series_success": 0,
    "series_failed": 0,
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
# HTTP 호출 함수
# ==========================================================
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
                print(f"⚠️  Rate limited, waiting {wait_time}s...")
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
# IMDB Rating 추출
# ==========================================================
def parse_rating_from_html(imdb_id, html_text):
    """
    IMDB title HTML에서 ratingValue, ratingCount 추출 (JSON-LD)
    """
    imdb_rating = None
    imdb_rating_count = None

    # JSON-LD 블록 추출
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
        except Exception as e:
            print(f"⚠️  JSON-LD parse error ({imdb_id}): {e}")

    return {
        "imdb_id": imdb_id,
        "imdb_rating": imdb_rating,
        "imdb_rating_count": imdb_rating_count,
    }

async def fetch_imdb_rating(session, imdb_id):
    url = f"https://www.imdb.com/title/{imdb_id}/"
    html_text = await get_html(session, url)
    if not html_text:
        print(f"⚠️  {imdb_id}: HTML 가져오기 실패")
        return {
            "imdb_id": imdb_id,
            "imdb_rating": None,
            "imdb_rating_count": None,
        }
    return parse_rating_from_html(imdb_id, html_text)

# ==========================================================
# 체크포인트 관리
# ==========================================================
def save_checkpoint(processed_ids):
    checkpoint = {
        'processed_ids': list(processed_ids),
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
    
    # 2) 기존 CSV 기준
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
# 날짜 필터링
# ==========================================================
def filter_by_date_range(df, start_date='2005-01-01', end_date='2015-12-31'):
    """
    first_air_date 컬럼 기준으로 날짜 필터링
    """
    if 'first_air_date' not in df.columns:
        print("⚠️  'first_air_date' 컬럼이 없습니다. 날짜 필터링을 건너뜁니다.")
        return df
    
    df['first_air_date'] = pd.to_datetime(df['first_air_date'], errors='coerce')
    start = pd.to_datetime(start_date)
    end = pd.to_datetime(end_date)
    
    df_filtered = df[(df['first_air_date'] >= start) & (df['first_air_date'] <= end)]
    
    print(f"📅 날짜 필터링: {start_date} ~ {end_date}")
    print(f"   원본: {len(df):,}개 → 필터링 후: {len(df_filtered):,}개")
    
    return df_filtered

# ==========================================================
# 메인 실행
# ==========================================================
async def main(input_csv_path, vote_threshold=30):
    print("=" * 90)
    print("🚀 IMDB RATING COLLECTOR (2005-2015)")
    print("=" * 90)
    
    stats["start_time"] = datetime.now()
    t0 = datetime.now()
    
    # 1. 데이터 로드
    print("\n📂 데이터 로드 중...")
    df = pd.read_csv(input_csv_path)
    
    # 날짜 필터링 (2005-2015)
    df = filter_by_date_range(df, '2005-01-01', '2015-12-31')
    
    # vote_count 필터링
    df_filtered = df[(df['vote_count'] >= vote_threshold) & (df['imdb_id'].notna())]
    df_filtered = df_filtered.drop_duplicates(subset=['imdb_id'])
    
    print(f"✅ 최종 필터링 (vote_count>={vote_threshold} & imdb_id 존재): {len(df_filtered):,}개")
    
    if len(df_filtered) == 0:
        print("⚠️  조건을 만족하는 데이터가 없습니다.")
        return
    
    # 2. 체크포인트 로드
    processed_ids = load_checkpoint()
    series_list = df_filtered[['imdb_id']].to_dict('records')
    
    if processed_ids:
        print(f"📌 이미 처리된 시리즈: {len(processed_ids):,}개")
        series_list = [s for s in series_list if s['imdb_id'] not in processed_ids]
        print(f"📌 남은 작업: {len(series_list):,}개")
    
    if len(series_list) == 0:
        print("✅ 모든 데이터가 이미 처리되었습니다.")
        return
    
    stats["series_total"] = len(series_list)
    
    # 3. 크롤링 설정
    print(f"\n🚀 크롤링 시작")
    print(f"⚙️  Rate Limit: {MAX_CALLS_PER_SECOND}회/초")
    
    estimated_time = len(series_list) / MAX_CALLS_PER_SECOND / 60
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
            
            tasks = [fetch_imdb_rating(session, s['imdb_id']) for s in batch]
            batch_results = await asyncio.gather(*tasks, return_exceptions=True)
            
            for result in batch_results:
                if isinstance(result, Exception):
                    stats["series_failed"] += 1
                    continue
                
                if isinstance(result, dict):
                    all_results.append(result)
                    processed_ids.add(result['imdb_id'])
                    stats["series_success"] += 1
                else:
                    stats["series_failed"] += 1
            
            # 주기적 저장
            if (i + batch_size) % 50 == 0:
                save_checkpoint(processed_ids)
                
                if all_results:
                    df_batch = pd.DataFrame(all_results)
                    df_batch = df_batch.drop_duplicates(subset=['imdb_id'])
                    file_exists = Path(OUTPUT_CSV).exists()
                    df_batch.to_csv(
                        OUTPUT_CSV,
                        mode='a',
                        header=not file_exists,
                        index=False,
                        encoding='utf-8-sig'
                    )
                    all_results.clear()
                    print(f"💾 중간 저장 완료 ({len(df_batch):,}개)")
            
            # 진행 상황 출력
            elapsed = (datetime.now() - t0).total_seconds() / 60
            progress = stats["series_success"] + stats["series_failed"]
            rate = progress / elapsed if elapsed > 0 else 0
            eta = (stats["series_total"] - progress) / rate if rate > 0 else 0
            
            print(
                f"📊 진행: {progress}/{stats['series_total']} "
                f"({progress/stats['series_total']*100:.1f}%) | "
                f"성공: {stats['series_success']} | 실패: {stats['series_failed']} | "
                f"요청: {stats['requests']:,}회 | "
                f"속도: {rate:.1f}개/분 | ETA: {eta:.0f}분"
            )
    
    # 4. 최종 저장
    print("\n💾 최종 저장 중...")

    if all_results:
        df_batch = pd.DataFrame(all_results)
        df_batch = df_batch.drop_duplicates(subset=['imdb_id'])
        file_exists = Path(OUTPUT_CSV).exists()
        df_batch.to_csv(
            OUTPUT_CSV,
            mode='a',
            header=not file_exists,
            index=False,
            encoding='utf-8-sig'
        )

    # 전체 중복 제거
    if Path(OUTPUT_CSV).exists():
        df_results = pd.read_csv(OUTPUT_CSV)
        df_results = df_results.drop_duplicates(subset=['imdb_id'])
        df_results.to_csv(OUTPUT_CSV, index=False, encoding='utf-8-sig')
    else:
        df_results = pd.DataFrame()

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
        print(f"📌 총 수집: {len(df_results):,}개 (중복 제거 후)")
        
        # rating이 있는 데이터 통계
        has_rating = df_results['imdb_rating'].notna().sum()
        print(f"📌 Rating 보유: {has_rating:,}개 ({has_rating/len(df_results)*100:.1f}%)")
        
        if has_rating > 0:
            print(f"📌 평균 Rating: {df_results['imdb_rating'].mean():.2f}")
            print(f"📌 평균 Rating Count: {df_results['imdb_rating_count'].mean():.0f}")
    else:
        print("📌 수집된 데이터 없음")
    
    print(f"📌 총 요청: {stats['requests']:,}회")
    print(f"⏱️  총 시간: {elapsed:.1f}분 ({elapsed/60:.2f}시간)")
    
    if stats['series_success'] > 0 and elapsed > 0:
        print(f"📊 속도: {stats['series_success']/elapsed:.1f}개/분")
    
    print("=" * 90)
    
    # 샘플 출력
    if not df_results.empty:
        print("\n📊 결과 샘플:")
        print(df_results.head(10).to_string())
        print(f"\n✅ 결과 파일: {OUTPUT_CSV}")

# ==========================================================
# 실행
# ==========================================================
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='IMDB RATING COLLECTOR (2005-2015)')
    parser.add_argument('--input', '-i', default='TMDB_tv_series_2005_2015_FULL.csv',
                        help='입력 CSV 파일 (기본: TMDB_tv_series_2005_2015_FULL.csv)')
    parser.add_argument('--vote', '-v', type=int, default=30,
                        help='최소 vote_count (기본: 30)')
    
    args = parser.parse_args()
    
    if not Path(args.input).exists():
        print(f"❌ 파일을 찾을 수 없습니다: {args.input}")
    else:
        asyncio.run(main(args.input, args.vote))
