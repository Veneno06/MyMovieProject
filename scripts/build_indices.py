#!/usr/bin/env python3
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

def get_movie_info_from_data(data):
    if not isinstance(data, dict): return None
    if "movieInfoResult" in data and "movieInfo" in data["movieInfoResult"]:
        return data["movieInfoResult"]["movieInfo"]
    if "movieCd" in data:
        return data
    return None

def main():
    files = scan_detail_files()
    print(f"[scan] detail files: {len(files)}")

    movies = []
    people_map = {}
    movie_extra_data_map = {}

    for fp in files:
        d = load_json(fp)
        mi = get_movie_info_from_data(d)
        if not mi: continue

        movieCd = (mi.get("movieCd") or "").strip()
        if not movieCd: continue
        
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

    for fp in files:
        d = load_json(fp)
        mi = get_movie_info_from_data(d)
        if not mi: continue
        
        movieCd = (mi.get("movieCd") or "").strip()
        if not movieCd: continue
        
        movieNm = (mi.get("movieNm") or "").strip()
        openDt  = norm_open(mi.get("openDt", ""))
        
        def add_person(p, role):
            if not isinstance(p, dict): return
            peopleCd = (p.get("peopleCd") or "").strip()
            peopleNm = (p.get("peopleNm") or "").strip()
            if not peopleCd and not peopleNm: return

            person_key = peopleCd
            if not person_key:
                # [수정] ID가 없는 동명이인을 구분하기 위해, 출연작 중 하나를 키에 포함
                first_film_part = (p.get("cast") or movieNm)[:5]
                person_key = f"name::{peopleNm}::{role}::{first_film_part}"

            rec = people_map.get(person_key)
            if not rec:
                rec = {
                    "personKey": person_key,
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
