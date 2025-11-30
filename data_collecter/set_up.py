from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import requests
from dotenv import load_dotenv
import os
from tqdm import tqdm
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
from concurrent.futures import ThreadPoolExecutor, as_completed

HEADERS = {"accept": "application/json"}

def create_session():
    """재시도 로직과 연결 풀이 있는 세션 생성"""
    session = requests.Session()
    retry = Retry(
        total=3,
        backoff_factor=0.3,
        status_forcelist=[500, 502, 503, 504]
    )
    adapter = HTTPAdapter(
        max_retries=retry,
        pool_connections=50,
        pool_maxsize=50
    )
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    return session

# 기본 준비
## 환경변수에서 API_KEY 읽어오기
load_dotenv()
API_KEY = os.getenv("TMDB_API_KEY")
session = create_session()