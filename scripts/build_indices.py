#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os, json, sys
from datetime import datetime, timezone

# 경로 설정
ROOT = os.path.dirname(os.path.dirname(__file__))
DOCS_DIR = os.path.join(ROOT, "docs", "data")
MOVIE_DIR = os.path.join(DOCS_DIR, "movies")
SEARCH_DIR = os.path.join(DOCS_DIR, "search")
os.makedirs(SEARCH_DIR, exist_ok=True)

# --- 유틸리티 함수 ---

def load_json(path):
    """JSON 파일을 안전하게 로드합니다."""
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return None

def norm_open(dt):
    """날짜 문자열을 YYYY-MM-DD 형식으로 정규화합니다."""
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
    """movies/ 폴더 내의 모든 .json 파일을 스캔합니다."""
    files = []
    for root, _, names in os.walk(MOVIE_DIR):
        for n in names:
            if n.endswith(".json"):
                files.append(os.path.join(root, n))
    files.sort()
    return files

def get_movie_info_from_data(data):
    """다양한 JSON 구조(flat/raw)에서 'movieInfo' 객체를 추출합니다."""
    if not isinstance(data, dict): return None
    if "movieInfoResult" in data and "movieInfo" in data["movieInfoResult"]:
        return data["movieInfoResult"]["movieInfo"]
    if "movieCd" in data: # build_movie_details.py가 생성한 flat 구조
        return data
    return None

# --- 메인 로직 ---

def main():
    files = scan_detail_files()
    print(f"[scan] detail files: {len(files)}")

    movies = [] # 영화 인덱스 (movies.json)
    people_map = {} # 인물 인덱스 (people.json)
    
    processed_movie_cds = set() # 영화 중복 스캔 방지

    # [1/2] 모든 영화 상세 JSON 파일을 순회
    for fp in files:
        d = load_json(fp)
        mi = get_movie_info_from_data(d)
        if not mi: continue

        movieCd = (mi.get("movieCd") or "").strip()
        if not movieCd: continue
        
        if movieCd in processed_movie_cds:
            continue
        processed_movie_cds.add(movieCd)
        
        # --- 영화 정보 추출 ---
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
        
        # --- [핵심] 인물 정보 추출 및 분리 ---
        def add_person(p_info, role):
            """
            인물 정보를 people_map에 추가합니다.
            peopleCd(고유 코드) 유무에 따라 'personKey'를 엄격하게 분리합니다.
            """
            if not isinstance(p_info, dict): return
            
            original_peopleCd = (p_info.get("peopleCd") or "").strip()
            peopleNm = (p_info.get("peopleNm") or "").strip()
            if not peopleNm: return

            # --- [API 고갈 임시 패치] '황정민' 수동 분리 ---
            # 원본 데이터(backfill)가 고쳐질 때까지 임시로 인덱싱 단계에서 분리합니다.
            if peopleNm == "황정민" and role == "배우" and not original_peopleCd:
                current_movieCd = (mi.get("movieCd") or "").strip()
                
                # 1969년생 (10061386) 출연작: 1987 (19871227), 7호실 (20171115) 등
                # 이 영화들은 1969년생 '황정민'의 코드를 강제로 할당합니다.
                if current_movieCd in ["19871227", "20171115"]:
                    original_peopleCd = "10061386" # 1969년생 코드
                
                # 나머지 '황정민'은 1970년생 (10051139)으로 간주합니다.
                elif current_movieCd: # 영화 코드가 있는 경우만
                    original_peopleCd = "10051139" # 1970년생 코드
            # --- [임시 패치 끝] ---


            # [최종 로직] personKey를 엄격하게 결정합니다.
            person_key = ""
            if original_peopleCd:
                person_key = original_peopleCd # 예: "10051139"
            else:
                name_role_key = f"{peopleNm}::{role}"
                person_key = f"name::{name_role_key}" # 예: "name::이순신::감독"

            # 3. 이 personKey로 인물 레코드를 찾거나 새로 생성합니다.
            rec = people_map.get(person_key)
            if not rec:
                rec = {
                    "personKey": person_key,
                    "peopleCd": original_peopleCd, # 코드가 없으면 "" (빈 문자열)
                    "peopleNm": peopleNm,
                    "repRoleNm": role,
                    "films": [] # 이 인물의 필모그래피
                }
                people_map[person_key] = rec
            
            # 4. 이 인물의 필모그래피에 현재 영화 정보를 추가합니다.
            if any(f.get("movieCd") == movieCd for f in rec["films"]):
                return

            film_info = {
                "movieCd": movieCd,
                "movieNm": movieNm,
                "openDt": openDt,
                "part": p_info.get("cast", ""),
                "audiAcc": audiAcc,
                "actorCount": actorCount
            }
            rec["films"].append(film_info)

        for x in (mi.get("directors") or []): add_person(x, "감독")
        for x in (mi.get("actors") or []):    add_person(x, "배우")

    # [2/2] 인덱스 파일 생성
    movies.sort(key=lambda m: m.get("openDt") or "9999-99-99")
    for rec in people_map.values():
        rec["films"].sort(key=lambda f: f.get("openDt") or "9999-99-99", reverse=True)
    
    people = list(people_map.values())
    print(f"[index] movies: {len(movies)} / people: {len(people)}")

    # 파일 저장
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
