#!/usr/-bin/env python3
# -*- coding: utf-8 -*-

import os, json, sys
from datetime import datetime, timezone

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
    movie_extra_data_map = {}

    for fp in files:
        d = load_json(fp)
        if not d: continue
        mi = (d.get("movieInfoResult") or {}).get("movieInfo") or {}
        if not mi: continue

        movieCd = (mi.get("movieCd") or "").strip()
        movieNm = (mi.get("movieNm") or "").strip()
        openDt  = norm_open(mi.get("openDt", ""))
        prdtYear = str(mi.get("prdtYear", "")).strip()
        nations = mi.get("nations") or []
        repNation = "K" if any(is_korean(x.get("nationNm")) for x in nations) else "F"
        grade = first_or_empty(mi.get("audits") or [], "watchGradeNm")
        genres = [ (g.get("genreNm") or "").strip() for g in (mi.get("genres") or []) if (g.get("genreNm") or "").strip() ]
        audiAcc = mi.get("audiAcc")
        actorCount = len(mi.get("actors", []))

        if not movieCd or not movieNm: continue

        movies.append({
            "movieCd": movieCd, "movieNm": movieNm, "openDt": openDt, "prdtYear": prdtYear,
            "repNation": repNation, "grade": grade, "genres": genres, "audiAcc": audiAcc,
        })
        
        movie_extra_data_map[movieCd] = { "audiAcc": audiAcc, "actorCount": actorCount }

    processed_movie_people = set()

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

            # [수정] 동명이인을 구분하기 위한 고유 키 생성
            person_key = peopleCd if peopleCd else f"name::{peopleNm}::{role}"
            
            # [수정] 한 영화에 동일 인물이 여러 역할로 중복 추가되는 것 방지
            if (movieCd, person_key) in processed_movie_people:
                return
            processed_movie_people.add((movieCd, person_key))

            rec = people_map.get(person_key)
            if not rec:
                rec = {
                    "personKey": person_key, # [추가] 프론트엔드에서 사용할 고유 키
                    "peopleCd": peopleCd, 
                    "peopleNm": peopleNm, 
                    "repRoleNm": role, 
                    "films": []
                }
                people_map[person_key] = rec
            
            if any(f.get("movieCd") == movieCd for f in rec["films"]):
                return

            extra_data = movie_extra_data_map.get(movieCd, {})
            film_info = {
                "movieCd": movieCd, "movieNm": movieNm, "openDt": openDt, "part": p.get("cast", ""),
                "audiAcc": extra_data.get("audiAcc"),
                "actorCount": extra_data.get("actorCount")
            }
            rec["films"].append(film_info)

        for x in (mi.get("directors") or []): add_person(x, "감독")
        for x in (mi.get("actors") or []):    add_person(x, "배우")

    movies.sort(key=lambda m: m.get("openDt") or "9999-99-99")
    for rec in people_map.values():
        rec["films"].sort(key=lambda f: f.get("openDt") or "9999-99-99", reverse=True)
    people = list(people_map.values())

    print(f"[index] movies: {len(movies)} / people: {len(people)}")

    # [수정] utcnow() 대신 timezone-aware 객체 사용
    now_ts = int(datetime.now(timezone.utc).timestamp())
    out_movies = {"generatedAt": now_ts, "count": len(movies), "movies": movies}
    out_people = {"generatedAt": now_ts, "count": len(people), "people": people}

    with open(os.path.join(SEARCH_DIR, "movies.json"), "w", encoding="utf-8") as f:
        json.dump(out_movies, f, ensure_ascii=False, indent=2)
    with open(os.path.join(SEARCH_DIR, "people.json"), "w", encoding="utf-8") as f:
        json.dump(out_people, f, ensure_ascii=False, indent=2)

    print("[write] search indexes saved.")

if __name__ == "__main__":
    main()
