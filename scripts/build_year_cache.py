# scripts/build_year_cache.py
import argparse
import json
import math
import os
import time
from typing import Dict, Any, List

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

KOFIC_BASE = "https://www.kobis.or.kr/kobisopenapi/webservice/rest/movie/searchMovieList.json"
OUT_DIR = "docs/data/years"
PER_PAGE = 100

def make_session() -> requests.Session:
    """
    requests.Session에 재시도 정책을 붙여서 타임아웃/일시오류에 강하게.
    - 429, 5xx에 대해 재시도
    - 연결/읽기 타임아웃은 (10, 60)
    - 백오프 계수 1.5로 점진 대기
    """
    s = requests.Session()
    retries = Retry(
        total=8,
        connect=5,
        read=5,
        backoff_factor=1.5,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
        raise_on_status=False,
        respect_retry_after_header=True,
    )
    adapter = HTTPAdapter(max_retries=retries, pool_connections=1, pool_maxsize=1)
    s.mount("https://", adapter)
    s.mount("http://", adapter)
    return s

def fetch_page(session: requests.Session, key: str, year: int, page: int) -> Dict[str, Any]:
    params = {
        "key": key,
        "openStartDt": str(year),
        "openEndDt": str(year),
        "itemPerPage": str(PER_PAGE),
        "curPage": str(page),
    }
    r = session.get(KOFIC_BASE, params=params, timeout=(10, 60))
    r.raise_for_status()
    js = r.json()
    # KOFIC에서 fault 응답이 오면 예외 던져 상위에서 재시도/중단 판단
    if js.get("faultInfo") or js.get("faultResult"):
        raise RuntimeError(f"KOBIS fault: {js.get('faultInfo') or js.get('faultResult')}")
    return js

def fetch_year(session: requests.Session, key: str, year: int) -> Dict[str, Any]:
    print(f"[year-cache] fetching {year} ...", flush=True)
    # 1페이지 먼저 호출
    js1 = fetch_page(session, key, year, 1)
    result = js1.get("movieListResult") or {}
    tot_cnt = int(result.get("totCnt") or 0)
    movies: List[Dict[str, Any]] = result.get("movieList") or []

    if tot_cnt == 0:
        # 데이터가 없는 연도일 수 있음
        return {"year": year, "movieList": [], "totCnt": 0}

    # 남은 페이지 수
    total_pages = math.ceil(tot_cnt / PER_PAGE)
    for p in range(2, total_pages + 1):
        # 페이지 사이에 살짝 쉬면서 서버에 부담 완화
        time.sleep(0.6)
        # 개별 페이지 재시도 래핑(최대 3회)
        for attempt in range(1, 4):
            try:
                jsp = fetch_page(session, key, year, p)
                part = (jsp.get("movieListResult") or {}).get("movieList") or []
                movies.extend(part)
                break
            except Exception as e:
                print(f"[year-cache] page {p} attempt {attempt} failed: {e}")
                if attempt == 3:
                    raise
                time.sleep(2 * attempt)  # 2s, 4s 대기 후 재시도

    return {"year": year, "movieList": movies, "totCnt": tot_cnt}

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--year-start", type=int, required=True)
    ap.add_argument("--year-end", type=int, required=True)
    args = ap.parse_args()

    key = os.environ.get("KOFIC_API_KEY", "").strip()
    if not key:
        raise SystemExit("KOFIC_API_KEY not set")

    os.makedirs(OUT_DIR, exist_ok=True)
    session = make_session()

    for y in range(args.year_start, args.year_end + 1):
        out_path = os.path.join(OUT_DIR, f"year-{y}.json")

        # 이미 만들어져 있으면 건너뛰기(재실행/증분에 유리)
        if os.path.exists(out_path):
            print(f"[year-cache] skip existing {out_path}")
            continue

        try:
            data = fetch_year(session, key, y)
        except Exception as e:
            print(f"[year-cache] ERROR for {y}: {e}")
            raise

        # 저장
        with open(out_path, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        print(f"[year-cache] saved {out_path} (totCnt={data.get('totCnt')})")

if __name__ == "__main__":
    main()
