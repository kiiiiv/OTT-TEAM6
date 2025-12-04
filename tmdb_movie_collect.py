from datetime import date
import file_collecter.tmdb_data_collecter as tdc
import time
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm

start_date = '2005-01-01'
end_date = '2025-11-30'

if __name__ == "__main__":
    total_start = time.time()

    # 기간을 3개월 단위로 분할
    periods = tdc.generate_date_periods(start_date, end_date, months=3)

    print(f"\n전체 기간: {start_date} ~ {end_date}")
    print(f"분할 기간: {len(periods)}개")
    print("="*90)

    # ID 데이터를 집합으로 변환하여 중복 제거
    all_ids = set()

    for idx, (period_start, period_end) in enumerate(periods, 1):
        print(f"\n[{idx}/{len(periods)}] 기간: {period_start} ~ {period_end}")

        # 기간내 영화 ID 수집
        current_ids_set = tdc.collect_movie_ids(period_start, period_end)
        # ID를 집합에 추가
        all_ids.update(current_ids_set)

        print(f"해당 기간 내 수집된 ID: {len(current_ids_set):,}개 (누적 ID: {len(all_ids):,}개)")

    # 집합을 리스트로 변환
    id_list = list(all_ids)

    print(f"\n{'='*90}")
    print(f"최종 ID 개수: {len(id_list):,}개")

    print(f"\n{'='*90}")
    print("상세 정보 멀티스레드 수집 시작...\n")

    results = []
    # 3. 상세 정보 수집 단계: ID 리스트를 사용
    with ThreadPoolExecutor(max_workers=30) as executor:
        # 딕셔너리 대신 ID 자체를 future에 매핑
        futures = {executor.submit(tdc.fetch_movie_details, series_id): series_id for series_id in id_list}

        for future in tqdm(as_completed(futures), total=len(futures), desc="TV 상세 수집"):
            detail = future.result()
            if detail:
                results.append(detail)

    print(f"\n상세 정보 수집 완료: {len(results):,}개")

    # 저장
    df = pd.DataFrame(results)
    # 컬럼 이름이 'first_air_date'라고 가정
    df = df.sort_values(["release_date", "popularity"], ascending=[True, False])
    df.to_csv("files/movies.csv", index=False, encoding="utf-8-sig")

    elapsed = time.time() - total_start

    print("\n" + "="*90)
    print("========================== DONE ==========================")
    print(f"총 데이터: {len(df):,}개")
    print(f"소요시간: {elapsed/60:.2f}분 ({elapsed:.2f}초)")
    print(f"저장 파일: movie.csv")
    print("="*90)