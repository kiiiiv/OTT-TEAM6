from file_collecter.tmdb_data_collecter.set_up import *

def generate_date_periods(start_date, end_date, months=1):
    """
    시작일과 종료일 사이를 3개월 단위로 분할하는 함수.
    """

    periods = []
    current = datetime.strptime(start_date, "%Y-%m-%d")
    end_dt = datetime.strptime(end_date, "%Y-%m-%d")

    while current <= end_dt:
        # 다음 기간의 시작일 계산 (정확히 N개월 후)
        next_date = current + relativedelta(months=months)
        period_end = min(next_date - timedelta(days=1), end_dt)

        periods.append((
            current.strftime("%Y-%m-%d"),
            period_end.strftime("%Y-%m-%d")
        ))

        current = next_date

    return periods

def fetch_single_page(page, start_date, end_date):
    """단일 페이지를 가져오는 함수"""
    base_url = "https://api.themoviedb.org/3/discover/movie"
    params = {
        "api_key": API_KEY,
        "language": "en-US",
        "sort_by": "popularity.desc",
        "primary_release_date.gte": start_date,
        "primary_release_date.lte": end_date,
        "page": page,
        "include_adult": True,
    }

    try:
        response = session.get(base_url, params=params, headers=HEADERS, timeout=10)
        data = response.json()
        ids = [item.get("id") for item in data.get("results", []) if item.get("id")]
        return ids, data.get("total_pages", 1), data.get("total_results", 0)

    except Exception as e:
        print(f"페이지 {page}에서 오류: {e}")
        return [], 1, 0

def fetch_movies_id_between_dates(start_date, end_date):
    """
    TMDB Discover API를 사용하여 특정 기간 내 모든 영화 목록을 수집 (멀티스레드).
    - 수집 효율성을 위해 병렬 처리(멀티스레드)를 사용합니다.
    - TMDB 정책에 따라 최대 500페이지(약 10,000개) 제한을 적용합니다.
    """
    # 1. 먼저 첫 페이지를 가져와서 총 페이지 수와 결과 수 확인 (fetch_single_page는 이미 영화용으로 수정되었다고 가정)
    # 이 호출을 통해 total_pages, total_results를 파악합니다.
    results, total_pages, total_results = fetch_single_page(1, start_date, end_date)
    all_ids_set = set(results) # 첫 페이지 결과 저장

    # 2. 500페이지 제한 적용 (TMDB의 discover API 제한)
    max_pages = min(total_pages, 500)

    print(f"총 {total_results:,}개 ({total_pages}페이지) → 수집 가능: {max_pages}페이지")

    # 수집 제한으로 인해 놓치는 데이터가 있다면 안내
    if total_pages > 500:
        print(f"500페이지 제한으로 인해 {(total_pages - 500) * 20:,}개는 수집 불가")

    # 총 페이지가 1페이지 이하일 경우, 추가 작업 없이 첫 페이지 결과만 반환
    if max_pages == 1:
        return all_ids_set

    # 3. 나머지 페이지를 멀티스레드로 병렬 수집
    with ThreadPoolExecutor(max_workers=20) as executor:
        # 2페이지부터 최대 수집 페이지(max_pages)까지의 모든 페이지 요청을 예약
        futures = {
            executor.submit(fetch_single_page, page, start_date, end_date): page
            for page in range(2, max_pages + 1)
        }

        # 완료되는 대로 결과를 처리 (tqdm은 진행률 표시 라이브러리)
        for future in tqdm(as_completed(futures), total=len(futures), desc=f"  페이지 수집", leave=False):
            # 스레드 결과에서 영화 목록만 추출 (total_pages, total_results는 무시)
            page_results, _, _ = future.result()
            # 수집된 영화 리스트에 추가
            all_ids_set.update(page_results)

    # 4. 최종적으로 수집된 모든 영화 목록 반환
    return all_ids_set

def collect_movie_ids(start_date, end_date):
    periods = generate_date_periods(start_date, end_date)
    all_ids = set()
    for period in periods:
        all_ids.update(fetch_movies_id_between_dates(period[0], period[1]))

    print(f"전체 ID 개수: {len(all_ids)}")

    return list(all_ids)