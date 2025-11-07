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
class DSU:
    def __init__(self, films):
        # 각 영화(movieCd)가 자신의 부모를 가리키도록 초기화
        self.parent = {f['movieCd']: f['movieCd'] for f in films}
        self.film_map = {f['movieCd']: f for f in films}

    def find(self, movieCd):
        """특정 영화의 최종 부모(루트)를 찾습니다."""
        if self.parent[movieCd] == movieCd:
            return movieCd
        self.parent[movieCd] = self.find(self.parent[movieCd])
        return self.parent[movieCd]

    def union(self, cd1, cd2):
        """두 영화를 하나의 그룹으로 묶습니다."""
        root1 = self.find(cd1)
        root2 = self.find(cd2)
        if root1 != root2:
            self.parent[root2] = root1

    def get_groups(self):
        """모든 영화를 그룹별로 묶어서 반환합니다."""
        groups = defaultdict(list)
        for movieCd in self.parent:
            root = self.find(movieCd)
            groups[root].append(self.film_map[movieCd])
        return groups

# --- 메인 로직 ---

def main():
    files = scan_detail_files()
    print(f"[scan] detail files: {len(files)}")

    movies = []
    people_map = {} # (1) 임시 인물 맵 (동명이인 합쳐진 상태)
    movie_to_cast_map = defaultdict(set) # (2) 영화별 출연진 맵
    
    # [핵심 수정] 영화 중복 추가를 방지하기 위한 Set
    processed_movie_cds = set()

    # [오류 수정] print 문을 한 줄로 합쳤습니다.
    print("[pass 1] 영화 정보 및 인물 데이터 스캔 중...")
    
    # --- 1단계: 모든 영화/인물 스캔 및 임시 맵 생성 ---
    for i, fp in enumerate(files):
        print(f"  -> Processing ({i+1}/{len(files)}): {os.path.basename(fp)}", end='\r')
        d = load_json(fp)
        mi = get_movie_info_from_data(d)
        if not mi: continue

        movieCd = (mi.get("movieCd") or "").strip()
        if not movieCd: continue
        
        # [핵심 수정] 이미 처리된 movieCd인지 확인
        if movieCd in processed_movie_cds:
            continue # 이미 추가된 영화이므로 스캔 건너뛰기
        processed_movie_cds.add(movieCd) # 새로 처리 목록에 추가
        # --- [수정 끝] ---
        
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

        def add_person_to_map(p, role):
            """인물을 임시 맵에 추가하고, 이 영화의 출연진 목록에도 추가"""
            if not isinstance(p, dict): return
            peopleCd = (p.get("peopleCd") or "").strip()
            peopleNm = (p.get("peopleNm") or "").strip()
            if not peopleNm: return

            # [핵심] peopleCd가 있으면 그것을, 없으면 '이름::역할'을 키로 사용
            person_key = peopleCd if peopleCd else f"name::{peopleNm}::{role}"
            
            # (1) 임시 인물 맵에 추가
            rec = people_map.get(person_key)
            if not rec:
                rec = {"personKey": person_key, "peopleCd": peopleCd, "peopleNm": peopleNm, "repRoleNm": role, "films": []}
                people_map[person_key] = rec
            
            if not any(f.get("movieCd") == movieCd for f in rec["films"]):
                film_info = {
                    "movieCd": movieCd, "movieNm": movieNm, "openDt": openDt, "part": p.get("cast", ""),
                    "audiAcc": audiAcc, "actorCount": actorCount
                }
                rec["films"].append(film_info)
            
            # (2) 영화별 출연진 맵에 추가
            current_movie_cast_keys.add(person_key)

        for x in (mi.get("directors") or []): add_person_to_map(x, "감독")
        for x in (mi.get("actors") or []):    add_person_to_map(x, "배우")
        
        movie_to_cast_map[movieCd] = current_movie_cast_keys
    
    print(f"\n[pass 1] 완료. {len(movies)}개 영화, {len(people_map)}명(중복 포함) 인물 발견.")

    # --- 2단계: 동명이인 분리(Clustering) 작업 ---
    # [오류 수정] print 문을 한 줄로 합쳤습니다.
    print("[pass 2] 동명이인 분리 작업 중... (API 사용 없음)")
    
    final_people_list = []
    separated_count = 0

    for personKey, personData in people_map.items():
        # peopleCd가 있는 인물(name:: 아님)은 이미 고유하므로 바로 추가
        if personData.get("peopleCd"):
            final_people_list.append(personData)
            continue
        
        # 'name::' 키를 가진 동명이인 후보
        films = personData.get("films", [])
        if len(films) <= 1:
            # 영화가 1편 이하면 비교 대상이 없으므로 그대로 추가
            final_people_list.append(personData)
            continue

        # DSU 알고리즘으로 영화들을 그룹화
        dsu = DSU(films)
        
        # 모든 영화 쌍(i, j)을 비교
        for i in range(len(films)):
            for j in range(i + 1, len(films)):
                film1_cd = films[i]["movieCd"]
                film2_cd = films[j]["movieCd"]

                # 각 영화의 '공동 출연 배우' 목록 (본인 제외)
                # [핵심] 이 공동 출연 배우들은 'peopleCd'를 가진 경우가 많음
                costars1 = movie_to_cast_map[film1_cd] - {personKey}
                costars2 = movie_to_cast_map[film2_cd] - {personKey}

                # 두 영화가 1명이라도 공동 출연 배우를 공유한다면,
                if not costars1.isdisjoint(costars2):
                    dsu.union(film1_cd, film2_cd) # 같은 그룹으로 묶음

        # 그룹화 결과 확인
        clusters = dsu.get_groups()
        
        if len(clusters) > 1:
            # 2개 이상의 그룹(클러스터)으로 나뉘었다 = 동명이인이다.
            print(f"  -> '{personData['peopleNm']}' ({personData['repRoleNm']}) 님이 {len(clusters)}명으로 분리되었습니다.")
            separated_count += 1
            
            for i, film_group in enumerate(clusters.values()):
                # 분리된 새 인물 생성
                new_person = personData.copy()
                new_person['films'] = sorted(film_group, key=lambda f: f.get("openDt") or "0", reverse=True)
                # "name::황정민::배우_1", "name::황정민::배우_2" 처럼 새 키 부여
                new_person['personKey'] = f"{personKey}_{i+1}"
                final_people_list.append(new_person)
        else:
            # 1개 그룹 = 동명이인이 아님
            final_people_list.append(personData)

    print(f"\n[pass 2] 완료. 총 {separated_count}개의 동명이인 그룹 분리.")
    print("="*40)
    
    # --- 3단계: 최종 저장 ---
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
