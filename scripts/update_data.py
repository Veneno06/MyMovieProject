#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import os, json, time
from pathlib import Path
import requests
import subprocess
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

ROOT = Path(__file__).resolve().parents[1]
DOCS = ROOT / "docs" / "data"
MOVIES = DOCS / "movies"
SEARCH = DOCS / "search"

API_KEY = os.environ.get("KOFIC_API_KEY","").strip()
API_BASE = "https://www.kobis.or.kr/kobisopenapi/webservice/rest"

def make_session() -> requests.Session:
    s = requests.Session()
    retries = Retry(
        total=5, connect=3, read=3, backoff_factor=1,
        status_forcelist=[429, 500, 502, 503, 504]
    )
    adapter = HTTPAdapter(max_retries=retries)
    s.mount("https://", adapter)
    s.mount("http://", adapter)
    return s

S = make_session() 

def load_json(p: Path, default=None):
    if not p.exists(): return default
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
    r = S.get(url, params=params, timeout=60)
    r.raise_for_status()
    return r.json()

def get_year(open_dt, fallback):
    s = (open_dt or "").replace("-","").replace(".","")
    if len(s)>=4 and s[:4].isdigit():
        return s[:4]
    return fallback

def newest_day_file():
    cands = sorted(DOCS.glob("20*.json"))
    return cands[-1] if cands else None

def main():
    day = newest_day_file()
    if not day:
        print("No daily file found to process.")
        return
        
    print(f"Processing newest daily file: {day.name}")
    js = load_json(day, {})
    items = js.get("boxOfficeResult",{}).get("dailyBoxOfficeList") or []
    
    if not items:
        print("No movies found in the daily box office list.")
        return

    movie_cds = list(dict.fromkeys(
        str(it.get("movieCd") or "").strip() for it in items if str(it.get("movieCd") or "").strip()
    ))
    print("New movie codes found:", movie_cds)

    for cd in movie_cds:
        year_folders = [p for p in MOVIES.glob("*") if p.is_dir()]
        exists = any((p / f"{cd}.json").exists() for p in year_folders)
        
        if exists:
            print(f"Detail file for {cd} already exists. Skipping.")
            continue
            
        try:
            print(f"Fetching details for new movie code: {cd}")
            detail = get_detail(cd)
            info = (detail.get("movieInfoResult") or {}).get("movieInfo") or {}
            y = get_year(info.get("openDt",""), "unknown")
            save_json(MOVIES / y / f"{cd}.json", detail)
            print(f"Saved details for {cd} in year folder '{y}'")
            time.sleep(0.2)
        except requests.exceptions.RequestException as e:
            print(f"Failed to fetch details for {cd} after multiple retries: {e}")
            continue

    print("Updating search indexes...")
    subprocess.check_call([ "python", str(ROOT / "scripts" / "build_indices.py") ])
    print("All tasks completed.")

if __name__ == "__main__":
    main()
