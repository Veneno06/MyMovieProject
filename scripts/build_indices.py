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
    files.sort()
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
    
    # [최종 수정] 인물 중심의 통합을 위한 조회용 맵
    cd_to_key_map = {}
    name_role_to_cd_map = {}

    # 1차 스캔: 모든 인물 정보를 수집하고 고유 ID(peopleCd)를 기준으로 통합
    for fp in files:
        d = load_json(fp)
        mi = get_movie_info_from_data(d)
        if not mi: continue

        def process_person_for_map(p, role):
            if not isinstance(p, dict): return
            peopleCd = (p.get("peopleCd") or "").strip()
            peopleNm = (p.get("peopleNm") or "").strip()
            if not peopleCd or not peopleNm: return
            
            name_role_key = f"{peopleNm}::{role}"
            name_role_to_cd_map[name_role_key] = peopleCd

        for x in (mi.get("directors") or []): process_person_for_map(x, "감독")
        for x in (mi.get("actors") or []):    process_person_for_map(x, "배우")

    # 2차 스캔: 영화 정보 및 최종 인물 정보 구축
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
        
        def add_person(p, role):
            if not isinstance(p, dict): return
            peopleCd = (p.get("peopleCd") or "").strip()
            peopleNm = (p.get("peopleNm") or "").strip()
            if not peopleNm: return

            # [최종 수정] 가장 정확한 고유 키 찾기
            name_role_key = f"{peopleNm}::{role}"
            if not peopleCd and name_role_key in name_role_to_cd_map:
                peopleCd = name_role_to_cd_map[name_role_key]

            person_key = peopleCd if peopleCd else f"name::{name_role_key}"

            rec = people_map.get(person_key)
            if not rec:
                rec = {"personKey": person_key, "peopleCd": peopleCd, "peopleNm": peopleNm, "repRoleNm": role, "films": []}
                people_map[person_key] = rec
            
            if any(f.get("movieCd") == movieCd for f in rec["films"]):
                return

            film_info = {
                "movieCd": movieCd, "movieNm": movieNm, "openDt": openDt, "part": p.get("cast", ""),
                "audiAcc": audiAcc,
                "actorCount": actorCount
            }
            rec["films"].append(film_info)

        for x in (mi.get("directors") or []): add_person(x, "감독")
        for x in (mi.get("actors") or []):    add_person(x, "배우")

    movies.sort(key=lambda m: m.get("openDt") or "9999-9-99")
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
