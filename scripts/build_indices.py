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

        # [수정] 배우 목록 순서대로 추출 (최대 10명 저장)
        # 이 순서가 바로 '영화 상세 정보'의 순서입니다.
        cast_list = []
        for act in (mi.get("actors") or [])[:10]:
            nm = (act.get("peopleNm") or "").strip()
            if nm: cast_list.append(nm)

        # movies.json에 영화 추가 (cast 필드 추가)
        movies.append({
            "movieCd": movieCd, "movieNm": movieNm, "openDt": openDt, "prdtYear": prdtYear,
            "repNation": repNation, "grade": grade, "genres": genres, "audiAcc": audiAcc,
            "cast": cast_list # [신규] 배우 목록 저장
        })
        
        # --- [핵심] 인물 정보 추출 및 분리 ---
        def add_person(p_info, role):
            if not isinstance(p_info, dict): return
            
            original_peopleCd = (p_info.get("peopleCd") or "").strip()
            peopleNm = (p_info.get("peopleNm") or "").strip()
            if not peopleNm: return

            # [최종 로직] personKey 결정
            person_key = ""
            if original_peopleCd:
                person_key = original_peopleCd
            else:
                name_role_key = f"{peopleNm}::{role}"
                person_key = f"name::{name_role_key}"

            # 레코드 찾기/생성
            rec = people_map.get(person_key)
            if not rec:
                rec = {
                    "personKey": person_key,
                    "peopleCd": original_peopleCd,
                    "peopleNm": peopleNm,
                    "repRoleNm": role,
                    "films": []
                }
                people_map[person_key] = rec
            
            # 영화 중복 체크
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
