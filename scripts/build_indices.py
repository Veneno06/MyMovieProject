#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import json
import sys
from datetime import datetime, timezone
from collections import defaultdict

# --- 경로 설정 ---
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
    """날짜를 'YYYY-MM-DD' 형식으로 정규화합니다."""
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
    """모든 영화 상세 JSON 파일 경로를 스캔합니다."""
    files = []
    for root, _, names in os.walk(MOVIE_DIR):
        for n in names:
            if n.endswith(".json"):
                files.append(os.path.join(root, n))
    files.sort()
    return files

def get_movie_info_from_data(data):
    """JSON 데이터에서 실제 영화 정보(mi) 객체를 추출합니다."""
    if not isinstance(data, dict): return None
    if "movieInfoResult" in data and "movieInfo" in data["movieInfoResult"]:
        return data["movieInfoResult"]["movieInfo"]
    if "movieCd" in data:
        return data
    return None

# --- [추가] 동명이인 분리(Clustering)를 위한 Disjoint Set Union (DSU) 클래스 ---
# (이 클래스는 "병합"에 실패한, '진짜' 알 수 없는 인물들을 분리할 때만 사용됩니다.)
class DSU:
    def __init__(self, films):
        self.parent = {f['movieCd']: f['movieCd'] for f in films}
        self.film_map = {f['movieCd']: f for f in films}

    def find(self, movieCd):
        if self.parent[movieCd] == movieCd:
            return movieCd
        self.parent[movieCd] = self.find(self.parent[movieCd])
        return self.parent[movieCd]

    def union(self, cd1, cd2):
        root1 = self.find(cd1)
        root2 = self.find(cd2)
        if root1 != root2:
            self.parent[root2] = root1

    def get_groups(self):
        groups = defaultdict(list)
        for movieCd in self.parent:
            root = self.find(movieCd)
            groups[root].append(self.film_map[movieCd])
        return groups.values()

# --- 메인 로직 ---

def main():
    files = scan_detail_files()
    print(f"[scan] detail files: {len(files)}")

    movies = []
    people_map = {} # (1) 최종 인물 맵
    movie_to_cast_map = defaultdict(set) # (2) 영화별 출연진(personKey) 맵
    processed_movie_cds = set() # (3) 영화 중복 처리 방지

    # --- 1단계: 모든 영화/인물 스캔 및 '임시' 맵 생성 ---
    # [오류 수정] print 문을 한 줄로 수정
    print("[pass 1] 영화 정보 및 인물 데이터 스캔 중...")
    temp_people_map = {} # peopleCd가 있든 없든 모두 저장하는 임시 맵
    
    for i, fp in enumerate(files):
        print(f"  -> Processing ({i+1}/{len(files)}): {os.path.basename(fp)}", end='\r')
        d = load_json(fp)
        mi = get_movie_info_from_data(d)
        if not mi: continue

        movieCd = (mi.get("movieCd") or "").strip()
        if not movieCd: continue
        
        # 영화 중복 방지
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
        
        current_movie_cast_keys = set()
        all_persons = (mi.get("directors") or []) + (mi.get("actors") or [])
        
        for p in all_persons:
            if not isinstance(p, dict): continue
            peopleCd = (p.get("peopleCd") or "").strip()
            peopleNm = (p.get("peopleNm") or "").strip()
            role = "감독" if p in (mi.get("directors") or []) else "배우"
            if not peopleNm: continue

            # [핵심] peopleCd가 있으면 그것을, 없으면 '이름::역할'을 키로 사용
            person_key = peopleCd if peopleCd else f"name::{peopleNm}::{role}"
            
            rec = temp_people_map.get(person_key)
            if not rec:
                rec = {"personKey": person_key, "peopleCd": peopleCd, "peopleNm": peopleNm, "repRoleNm": role, "films": []}
                temp_people_map[person_key] = rec
            
            if not any(f.get("movieCd") == movieCd for f in rec["films"]):
                rec["films"].append({
                    "movieCd": movieCd, "movieNm": movieNm, "openDt": openDt, "part": p.get("cast", ""),
                    "audiAcc": audiAcc, "actorCount": actorCount
                })
            
            current_movie_cast_keys.add(person_key)
        
        movie_to_cast_map[movieCd] = current_movie_cast_keys
    
    print(f"\n[pass 1] 완료. {len(movies)}개 영화, {len(temp_people_map)}명(중복 포함) 인물 발견.")

    # --- 2단계: "미확인 인물"을 "확정된 인물"에 병합 시도 ---
    # [오류 수정] print 문을 한 줄로 수정
    print("[pass 2] 미확인 인물을 확정된 인물 프로필로 병합 중...")
    
    # "확정된 인물"의 전체 공동 출연자 맵 생성
    career_costars_map = defaultdict(set)
    verified_profiles_by_name = defaultdict(list)
    
    for key, person in temp_people_map.items():
        if person.get("peopleCd"): # peopleCd가 있는 "확정된 인물"
            verified_profiles_by_name[person["peopleNm"]].append(key)
            for film in person["films"]:
                costars = movie_to_cast_map[film["movieCd"]] - {key}
                career_costars_map[key].update(costars)

    unverified_keys = [key for key in temp_people_map if key.startswith("name::")]
    merged_film_count = 0

    for key in unverified_keys:
        person = temp_people_map[key]
        candidates = verified_profiles_by_name.get(person["peopleNm"], [])
        if not candidates:
            # 병합할 대상(동명이인)이 없으면, 최종 맵으로 이동
            people_map[key] = person
            continue

        films_to_move = []
        films_to_keep = []

        for film in person["films"]:
            film_costars = movie_to_cast_map[film["movieCd"]] - {key}
            
            best_score = 0
            best_candidate_key = None

            for candidate_key in candidates:
                candidate_costars = career_costars_map[candidate_key]
                score = len(film_costars.intersection(candidate_costars))
                
                if score > best_score:
                    best_score = score
                    best_candidate_key = candidate_key

            if best_score > 0 and best_candidate_key:
                # [병합!] 겹치는 배우가 있으므로 이 영화를 "확정된 인물"에게 이동
                films_to_move.append((best_candidate_key, film))
                merged_film_count += 1
            else:
                # 겹치는 배우가 없어 병합 실패.
                films_to_keep.append(film)
        
        # 실제 영화 이동
        for target_key, film in films_to_move:
            if not any(f["movieCd"] == film["movieCd"] for f in temp_people_map[target_key]["films"]):
                temp_people_map[target_key]["films"].append(film)
        
        # "미확인" 프로필에 병합 실패한 영화만 남김
        person["films"] = films_to_keep
        if films_to_keep:
            people_map[key] = person # 남은 영화가 있으면 최종 맵에 추가
            
    print(f"[pass 2] 완료. {merged_film_count}개 미확인 필모그래피 병합 성공.")

    # --- 3단계: 병합 후 남은 "미확인 인물"들 분리 ---
    # [오류 수정] print 문을 한 줄로 수정
    print("[pass 3] 병합에 실패한 나머지 동명이인 분리 중...")
    
    final_people_list = []
    separated_count = 0

    for key, person in temp_people_map.items():
        if person.get("peopleCd"): # 1단계에서 peopleCd가 있던 인물들
            final_people_list.append(person)

    for key, person in people_map.items(): # 2단계에서 병합 후 남은 인물들
        if person.get("peopleCd"): continue # (이 경우는 없어야 함)

        films = person.get("films", [])
        if len(films) <= 1:
            final_people_list.append(person) # 1편 이하면 그냥 추가
            continue
        
        # [DSU 실행] 병합 실패한 영화가 2편 이상이면, 이들끼리 공동출연자 비교
        dsu = DSU(films)
        for i in range(len(films)):
            for j in range(i + 1, len(films)):
                film1_cd = films[i]["movieCd"]
                film2_cd = films[j]["movieCd"]
                costars1 = movie_to_cast_map[film1_cd] - {key}
                costars2 = movie_to_cast_map[film2_cd] - {key}
                if not costars1.isdisjoint(costars2):
                    dsu.union(film1_cd, film2_cd)
        
        clusters = dsu.get_groups()
        if len(clusters) > 1:
            separated_count += 1
            print(f"  -> '{person['peopleNm']}' ({person['repRoleNm']}) 님이 {len(clusters)}명으로 분리되었습니다.")
            for i, film_group in enumerate(clusters):
                new_person = person.copy()
                new_person['films'] = sorted(film_group, key=lambda f: f.get("openDt") or "0", reverse=True)
                new_person['personKey'] = f"{key}_{i+1}"
                final_people_list.append(new_person)
        else:
            final_people_list.append(person) # 그룹이 1개면 그냥 추가

    print(f"[pass 3] 완료. {separated_count}개 그룹 분리.")
    print("="*40)
    
    # --- 4단계: 최종 저장 ---
    print(f"[index] movies: {len(movies)} / people: {len(final_people_list)}")

    movies.sort(key=lambda m: m.get("openDt") or "9999-99-99")
    for rec in final_people_list:
        rec["films"].sort(key=lambda f: f.get("openDt") or "9999-99-99", reverse=True)
    
    people = sorted(final_people_list, key=lambda p: p['peopleNm'])

    now_ts = int(datetime.now(timezone.utc).timestamp())
    out_movies = {"generatedAt": now_ts, "count": len(movies), "movies": movies}
    out_people = {"generatedAt": now_ts, "count": len(people), "people": people}

    with open(os.path.join(SEARCH_DIR, "movies.json"), "w", encoding="utf-8") as f:
        json.dump(out_movies, f, ensure_ascii=False, indent=2)
    with open(os.path.join(SEARCH_DIR, "people.json"), "w", encoding="utf-8") as f:
        json.dump(out_people, f, ensure_ascii=False, indent=2)

    print("[write] search indexes saved.")
    print("="*40)

if __name__ == "__main__":
    main()
