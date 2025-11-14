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
    
    cd_to_key_map = {}
    name_role_to_cd_map = {} # 이 변수는 선언만 해두고, 위험한 추측 로직에는 사용하지 않음
    
    processed_movie_cds = set()

    for fp in files:
        d = load_json(fp)
        mi = get_movie_info_from_data(d)
        if not mi: continue

        movieCd = (mi.get("movieCd") or "").strip()
        if not movieCd: continue
        
        if movieCd in processed_movie_cds:
            continue
        processed_movie_cds.add(movieCd)
        
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
        
        # [핵심 수정] 동명이인 분리 로직
        def add_person(p, role):
            if not isinstance(p, dict): return
            
            # 1. 영화 데이터에서 원본 peopleCd를 가져옵니다.
            original_peopleCd = (p.get("peopleCd") or "").strip()
            peopleNm = (p.get("peopleNm") or "").strip()
            if not peopleNm: return

            # 2. personKey를 엄격하게 결정합니다.
            #    peopleCd가 있으면, personKey는 *반드시* peopleCd입니다.
            #    peopleCd가 없으면, personKey는 *반드시* name::{이름}::{역할}입니다.
            
            person_key = ""
            if original_peopleCd:
                person_key = original_peopleCd
            else:
                name_role_key = f"{peopleNm}::{role}"
                person_key = f"name::{name_role_key}"


            # 3. 결정된 person_key를 기준으로 레코드를 가져오거나 생성합니다.
            rec = people_map.get(person_key)
            if not rec:
                # 새 레코드를 생성할 때, peopleCd 필드는 *원본* peopleCd가 있을 때만 설정합니다.
                rec = {
                    "personKey": person_key, 
                    "peopleCd": original_peopleCd, # (key fix) name:: 키일 경우 이 값은 ""가 됩니다.
                    "peopleNm": peopleNm, 
                    "repRoleNm": role, 
                    "films": []
                }
                people_map[person_key] = rec
                
                # 4. cd_to_key_map은 원본 peopleCd가 있을 때만 업데이트합니다.
                if original_peopleCd:
                    cd_to_key_map[original_peopleCd] = person_key
            
            # 5. 해당 레코드에 영화 정보를 추가합니다.
            if any(f.get("movieCd") == movieCd for f in rec["films"]):
                return

            film_info = {
                "movieCd": movieCd, "movieNm": movieNm, "openDt": openDt, "part": p.get("cast", ""),
                "audiAcc": audiAcc, "actorCount": actorCount
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
