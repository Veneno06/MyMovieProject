#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os, json, sys
from datetime import datetime

ROOT = os.path.dirname(os.path.dirname(__file__))
DOCS_DIR = os.path.join(ROOT, "docs", "data")
MOVIE_DIR = os.path.join(DOCS_DIR, "movies")
SEARCH_DIR = os.path.join(DOCS_DIR, "search")
os.makedirs(SEARCH_DIR, exist_ok=True)

def load_json(path):
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return None

def norm_open(dt):
    if not dt: return ""
    s = str(dt).strip().replace(".", "").replace("-", "")
    return f"{s[0:4]}-{s[4:6]}-{s[6:8]}" if len(s)==8 and s.isdigit() else ""

def is_korean(nm): 
    nm = (nm or "").strip()
    return ("한국" in nm) or ("대한민국" in nm)

def first_or_empty(arr, key):
    if isinstance(arr, list) and arr:
        v = arr[0].get(key, "")
        return v if isinstance(v, str) else ""
    return ""

def scan_detail_files():
    files = []
    for root, _, names in os.walk(MOVIE_DIR):
        for n in names:
            if n.endswith(".json"):
                files.append(os.path.join(root, n))
    return files

def main():
    files = scan_detail_files()
    print(f"[scan] detail files: {len(files)}")

    movies = []
    people_map = {}
    movie_audi_map = {} # [추가] 영화코드별 관객수 조회를 위한 맵

    # --- 1차 스캔: 영화 정보 및 관객수 맵 생성 ---
    for fp in files:
        d = load_json(fp)
        if not d: 
            continue
        mi = (d.get("movieInfoResult") or {}).get("movieInfo") or {}
        if not mi: 
            continue

        movieCd = (mi.get("movieCd") or "").strip()
        movieNm = (mi.get("movieNm") or "").strip()
        openDt  = norm_open(mi.get("openDt", ""))
        prdtYear = str(mi.get("prdtYear", "")).strip()

        nations = mi.get("nations") or []
        repNation = "K" if any(is_korean(x.get("nationNm")) for x in nations) else "F"

        grade = first_or_empty(mi.get("audits") or [], "watchGradeNm")
        genres = [ (g.get("genreNm") or "").strip() for g in (mi.get("genres") or []) if (g.get("genreNm") or "").strip() ]
        
        # [수정] 상세 정보 파일에서 audiAcc 값을 읽어옴
        audiAcc = mi.get("audiAcc") 

        if not movieCd or not movieNm:
            continue

        movies.append({
            "movieCd": movieCd,
            "movieNm": movieNm,
            "openDt": openDt,
            "prdtYear": prdtYear,
            "repNation": repNation,
            "grade": grade,
            "genres": genres,
            "audiAcc": audiAcc, # [수정] None 대신 읽어온 audiAcc 값을 저장
        })
        
        # [추가] 관객수 맵에 저장
        if audiAcc is not None:
            movie_audi_map[movieCd] = audiAcc

    # --- 2차 스캔: 배우 정보에 관객수 추가 ---
    for fp in files:
        d = load_json(fp)
        if not d: continue
        mi = (d.get("movieInfoResult") or {}).get("movieInfo") or {}
        if not mi: continue
        
        movieCd = (mi.get("movieCd") or "").strip()
        movieNm = (mi.get("movieNm") or "").strip()
        openDt  = norm_open(mi.get("openDt", ""))
        
        def add_person(p, role):
            if not isinstance(p, dict): return
            peopleCd = (p.get("peopleCd") or "").strip()
            peopleNm = (p.get("peopleNm") or "").strip()
            if not peopleCd and not peopleNm: return
            key = peopleCd if peopleCd else f"n::{peopleNm}::{role}"
            rec = people_map.get(key)
            if not rec:
                rec = {"peopleCd": peopleCd, "peopleNm": peopleNm, "repRoleNm": role, "films": []}
                people_map[key] = rec
            
            # [수정] 영화 정보에 관객수(audiAcc)도 함께 추가
            film_info = {
                "movieCd": movieCd, 
                "movieNm": movieNm, 
                "openDt": openDt, 
                "part": "",
                "audiAcc": movie_audi_map.get(movieCd) # 맵에서 관객수 조회
            }
            rec["films"].append(film_info)

        for x in (mi.get("directors") or []): add_person(x, "감독")
        for x in (mi.get("actors") or []):    add_person(x, "배우")

    # 정렬
    movies.sort(key=lambda m: m.get("openDt") or "9999-99-99")
    for rec in people_map.values():
        rec["films"].sort(key=lambda f: f.get("openDt") or "9999-99-99", reverse=True)
    people = list(people_map.values())

    print(f"[index] movies: {len(movies)} / people: {len(people)}")

    if len(movies) == 0:
        print("[ERROR] Parsed movie count is 0. Will NOT overwrite search indexes.")
        sys.exit(2)

    out_movies = {"generatedAt": int(datetime.utcnow().timestamp()), "count": len(movies), "movies": movies}
    out_people = {"generatedAt": int(datetime.utcnow().timestamp()), "count": len(people), "people": people}

    with open(os.path.join(SEARCH_DIR, "movies.json"), "w", encoding="utf-8") as f:
        json.dump(out_movies, f, ensure_ascii=False, indent=2)
    with open(os.path.join(SEARCH_DIR, "people.json"), "w", encoding="utf-8") as f:
        json.dump(out_people, f, ensure_ascii=False, indent=2)

    print("[write] search indexes saved.")

if __name__ == "__main__":
    main()
