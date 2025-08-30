#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
매일 생성되는 일자 파일(docs/data/YYYYMMDD.json)을 읽어
새로 발견된 movieCd에 대해 상세를 만들어 연도 폴더에 저장하고,
검색 인덱스를 갱신한다.

환경:
  KOFIC_API_KEY (GitHub Actions)

주의: 일자 파일 포맷은 기존 스크립트에서 만드는 구조를 따른다고 가정.
"""
import os, json, time
from pathlib import Path
import requests
import subprocess

ROOT = Path(__file__).resolve().parents[1]
DOCS = ROOT / "docs" / "data"
MOVIES = DOCS / "movies"
SEARCH = DOCS / "search"

API_KEY = os.environ.get("KOFIC_API_KEY","").strip()
API_BASE = "https://www.kobis.or.kr/kobisopenapi/webservice/rest"
S = requests.Session()

def load_json(p: Path, default=None):
    if not p.exists():
        return default
    try:
        with p.open("r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return default

def save_json(p: Path, data):
    p.parent.mkdir(parents=True, exist_ok=True)
    with p.open("w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

def get_detail(cd):
    url = f"{API_BASE}/movie/searchMovieInfo.json"
    params = {"key": API_KEY, "movieCd": cd}
    r = S.get(url, params=params, timeout=30)
    r.raise_for_status()
    return r.json()

def get_year(open_dt, fallback):
    s = (open_dt or "").replace("-","")
    if len(s)>=4 and s[:4].isdigit():
        return s[:4]
    return fallback

def newest_day_file():
    # docs/data/2025xxxx.json 중 최신 파일
    cands = sorted(DOCS.glob("20*.json"))
    return cands[-1] if cands else None

def main():
    day = newest_day_file()
    if not day:
        print("No daily file.")
        return
    js = load_json(day, {})
    items = js.get("list") or js.get("boxOfficeResult",{}).get("dailyBoxOfficeList") or []
    movie_cds = []
    for it in items:
        cd = str(it.get("movieCd") or it.get("movieCd2") or "").strip()
        if cd:
            movie_cds.append(cd)
    movie_cds = list(dict.fromkeys(movie_cds))  # uniq
    print("new codes:", movie_cds)

    for cd in movie_cds:
        # 이미 있으면 skip(연도 하위 폴더 포함)
        exists = any((p / f"{cd}.json").exists() for p in MOVIES.glob("*"))
        if exists:
            continue
        detail = get_detail(cd)
        info = (detail.get("movieInfoResult") or {}).get("movieInfo") or {}
        y = get_year(info.get("openDt",""), "unknown")
        save_json(MOVIES / y / f"{cd}.json", detail)
        time.sleep(0.2)

    # 인덱스 갱신
    subprocess.check_call([ "python", str(ROOT / "scripts" / "build_indices.py") ])

if __name__ == "__main__":
    main()
