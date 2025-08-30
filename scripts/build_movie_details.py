#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
연도 캐시(또는 API)를 바탕으로 영화 상세를 저장합니다.
저장 경로: docs/data/movies/<year|unknown>/<movieCd>.json

- 기존 평평한 경로(docs/data/movies/<movieCd>.json)가 있으면
  API 호출 없이 적절한 연도 폴더로 "이동" 합니다(쿼터 절약).
- 연도 판단은 상세 JSON의 openDt(YYYYMMDD) 기준, 없으면 hint year 또는 'unknown'.

환경:
  KOFIC_API_KEY (GitHub Secrets)

실행 예:
  python scripts/build_movie_details.py --year-start 2023 --year-end 2024 --max 999999
"""
import os, sys, json, time, shutil, argparse
from pathlib import Path
import requests

ROOT = Path(__file__).resolve().parents[1]  # repo root
DOCS = ROOT / "docs" / "data"
MOVIES_DIR = DOCS / "movies"
YEARS_DIR = DOCS / "years"
SESSION = requests.Session()
SESSION.headers.update({"User-Agent":"MyMovieProject/1.0 (+github-actions)"})

API_KEY = os.environ.get("KOFIC_API_KEY","").strip()
API_BASE = "https://www.kobis.or.kr/kobisopenapi/webservice/rest"

def ensure_dir(p: Path):
    p.mkdir(parents=True, exist_ok=True)

def load_json(p: Path, default=None):
    if not p.exists():
        return default
    try:
        with p.open("r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return default

def save_json(p: Path, data):
    ensure_dir(p.parent)
    with p.open("w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

def get_year_from_open_dt(open_dt: str, fallback: str):
    s = (open_dt or "").replace("-","")
    if len(s) >= 4 and s[:4].isdigit():
        return s[:4]
    return fallback if fallback else "unknown"

def kobis_get(url, params, timeout=30):
    for _ in range(3):
        try:
            r = SESSION.get(url, params=params, timeout=timeout)
            if r.status_code == 200:
                return r.json()
            # 4xx/5xx도 그대로 종료
            return {"__http__": r.status_code, "__text__": r.text}
        except requests.exceptions.RequestException:
            time.sleep(1.0)
    return {"__error__":"timeout"}

def fetch_detail(movie_cd: str):
    url = f"{API_BASE}/movie/searchMovieInfo.json"
    params = {"key": API_KEY, "movieCd": movie_cd}
    return kobis_get(url, params)

def fetch_codes_for_year(year: int, hard_cap=999999):
    """
    연도 캐시(year-YYYY.json)에 movieCds가 있으면 사용.
    없으면 searchMovieList를 페이징으로 수집(쿼터 소모).
    """
    yfile = YEARS_DIR / f"year-{year}.json"
    data = load_json(yfile, {})
    movie_cds = []

    if isinstance(data, dict):
        # 유연성: movieCds 또는 movieList[*].movieCd
        if "movieCds" in data and isinstance(data["movieCds"], list):
            movie_cds = [str(x) for x in data["movieCds"] if str(x)]
        elif "movieList" in data and isinstance(data["movieList"], list):
            for m in data["movieList"]:
                cd = str(m.get("movieCd","")).strip()
                if cd:
                    movie_cds.append(cd)

    if movie_cds:
        return movie_cds[:hard_cap]

    # 연도 캐시에 없으면 API로 페이징 수집
    print(f"[year {year}] cache empty → fetch via API ...")
    cur = 1
    item_per_page = 100
    acc = []
    while True and len(acc) < hard_cap:
        url = f"{API_BASE}/movie/searchMovieList.json"
        params = {
            "key": API_KEY,
            "openStartDt": str(year),
            "openEndDt": str(year),
            "itemPerPage": str(item_per_page),
            "curPage": str(cur),
        }
        js = kobis_get(url, params)
        lst = js.get("movieListResult", {}).get("movieList", []) or []
        for m in lst:
            cd = str(m.get("movieCd","")).strip()
            if cd:
                acc.append(cd)
        tot = int(js.get("movieListResult",{}).get("totCnt", 0))
        if cur * item_per_page >= tot:
            break
        cur += 1
        time.sleep(0.1)
    # 연도 파일에 결과 저장(다음 번부터는 캐시 사용)
    out = {
        "year": year,
        "totCnt": len(acc),
        "movieCds": acc,
        "movieList": []
    }
    save_json(yfile, out)
    return acc[:hard_cap]

def migrate_if_flat_exists(movie_cd: str, hint_year: str) -> bool:
    """
    예전 평평한 파일(docs/data/movies/<cd>.json)을
    docs/data/movies/<year>/ 로 이동.
    반환: True(이동함) / False(없거나 이동 못함)
    """
    flat = MOVIES_DIR / f"{movie_cd}.json"
    if not flat.exists():
        return False
    data = load_json(flat, {})
    open_dt = ""
    info = (data.get("movieInfoResult") or {}).get("movieInfo")
    if info:
        open_dt = info.get("openDt","")
    year = get_year_from_open_dt(open_dt, hint_year)
    dst = MOVIES_DIR / year / f"{movie_cd}.json"
    ensure_dir(dst.parent)
    try:
        shutil.move(str(flat), str(dst))
        print(f"[migrate] {flat.name} → {year}/{flat.name}")
        return True
    except Exception as e:
        print(f"[migrate] failed {flat}: {e}")
        return False

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--year-start", type=int, required=True)
    ap.add_argument("--year-end", type=int, required=True)
    ap.add_argument("--max", type=int, default=999999)
    args = ap.parse_args()

    if not API_KEY:
        print("ERROR: KOFIC_API_KEY is empty.")
        sys.exit(1)

    ensure_dir(MOVIES_DIR / "unknown")
    saved = 0
    for year in range(args.year_start, args.year_end+1):
        codes = fetch_codes_for_year(year, hard_cap=args.max)
        print(f"[{year}] total candidates: {len(codes)}")
        for i, cd in enumerate(codes, 1):
            # 경로 판단(연도별)
            # 1) 이미 연도 폴더에 있으면 skip
            # 2) 평평한 곳에 있으면 이동
            # 3) 없으면 API로 받아 저장
            #   - openDt로 연도 폴더 결정
            #   - openDt가 없으면 hint year 폴더 사용
            # check already sharded
            already = False
            for sub in list(MOVIES_DIR.iterdir()):
                if sub.is_dir():
                    dst = sub / f"{cd}.json"
                    if dst.exists():
                        already = True
                        break
            if already:
                continue

            if migrate_if_flat_exists(cd, str(year)):
                saved += 1
                continue

            # fetch
            js = fetch_detail(cd)
            info = (js.get("movieInfoResult") or {}).get("movieInfo")
            open_dt = ""
            if info:
                open_dt = info.get("openDt","")
            y = get_year_from_open_dt(open_dt, str(year))
            dst = MOVIES_DIR / y / f"{cd}.json"
            if dst.exists():
                continue
            save_json(dst, js)
            print(f"[OK] {year} {cd} -> {dst.relative_to(MOVIES_DIR)}")
            saved += 1
            # 너무 빠른 호출 방지
            time.sleep(0.1)
    print(f"[DONE] saved/migrated details: {saved}")

if __name__ == "__main__":
    main()
