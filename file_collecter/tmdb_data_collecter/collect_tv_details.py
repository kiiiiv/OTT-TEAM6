from file_collecter.tmdb_data_collecter.set_up import *

def list_to_str(lst, key="name"):
    return ", ".join([str(item.get(key, "")) for item in lst]) if lst else ""

def fetch_tv_details(tv_id):
    url = f"https://api.themoviedb.org/3/tv/{tv_id}"
    # providers 정보 추가를 위해 append_to_response에 watch/providers를 추가합니다.
    # WATCH_PROVIDERS_REGION은 'KR' 또는 'US' 등 원하는 국가 코드를 사용합니다. (예: KR)
    params = {
        "api_key": API_KEY,
        "language": "en-US",
        "append_to_response": "credits,watch/providers,keywords",
    }

    try:
        response = session.get(url, params=params, headers=HEADERS, timeout=10)
        response.raise_for_status() # 4xx, 5xx 에러 발생 시 예외 처리
        data = response.json()

    except Exception as e:
        print(f"Error fetching details for {tv_id}: {e}")
        return None

    genres_data = data.get("genres", [])
    genre_ids = list_to_str(genres_data, key="id")

    keywords_block = data.get("keywords", {})
    if isinstance(keywords_block, dict):
        keywords_list = [k.get("name") for k in keywords_block.get("keywords", [])]
    else:
        keywords_list = []

    credits = data.get("credits", {})
    cast_list = credits.get("cast", []) if isinstance(credits, dict) else []
    crew_list = credits.get("crew", []) if isinstance(credits, dict) else []

    # 상위 배우 5명 (원래 코드 주석은 3명이었으나 코드는 5명이므로 5명으로 유지)
    top_cast = [c.get("name") for c in cast_list[:5]]

    # 감독
    directors = [c.get("name") for c in crew_list if c.get("job") == "Director"]

    # 작가 (Writer, Screenplay)
    writers = [
        c.get("name") for c in crew_list
        if c.get("department") == "Writing" or c.get("job") in ("Writer", "Screenplay")
    ]

    providers_data = data.get("watch/providers", {})
    providers_results = providers_data.get("results", {}) if isinstance(providers_data, dict) else {}

    # 국가별 OTT 정보 (flatrate = 구독형)
    providers_flatrate = {}
    for country, info in providers_results.items():
        flatrate = info.get("flatrate", [])
        provider_names = [p["provider_name"] for p in flatrate]
        if provider_names:
            providers_flatrate[country] = provider_names

    providers_rent = {}
    for country, info in providers_results.items():
        rent = info.get("rent", [])
        provider_names = [p["provider_name"] for p in rent]
        if provider_names:
            providers_rent[country] = provider_names

    providers_buy = {}
    for country, info in providers_results.items():
        buy = info.get("buy", [])
        provider_names = [p["provider_name"] for p in buy]
        if provider_names:
            providers_buy[country] = provider_names
    # -------------------------------------------------------------------

    record = {
        # ID
        "id": data.get("id"),
        "type": "tv_series",
        "imdb_id": data.get("imdb_id"),
        # 포스터
        "poster_path": data.get("poster_path"),
        # 제목
        "title": data.get("title"),
        "original_title": data.get("original_title"),
        # 언어
        "original_language": data.get("original_language"),
        "spoken_languages": list_to_str(data.get("spoken_languages", [])),
        # 줄거리
        "overview": data.get("overview"),
        "tagline": data.get("tagline"),
        # 상영 정보
        "status": data.get("status"),
        "release_date": data.get("release_date"),
        "runtime": data.get("runtime"),
        "adult": data.get("adult"),
        # 재무 정보
        "budget": data.get("budget"),
        "revenue": data.get("revenue"),
        # 후기 정보
        "vote_count": data.get("vote_count"),
        "vote_average": data.get("vote_average"),
        "popularity": data.get("popularity"),
        # 세부 정보
        ## 장르
        "genres": list_to_str(genres_data),
        "genre_ids": genre_ids,
        ## 키워드
        "keywords": ", ".join(keywords_list),
        # 제작 참여자
        "top_cast": ", ".join(top_cast),
        "directors": ", ".join(directors),
        "writers": ", ".join(writers),
        # 제작사 및 국가
        "production_companies": list_to_str(data.get("production_companies", [])),
        "production_countries": list_to_str(data.get("production_countries", [])),
        # 공급자
        "providers_flatrate": providers_flatrate,
        "providers_rent": providers_rent,
        "providers_buy": providers_buy,
    }

    return record