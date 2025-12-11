# scripts/backfill_people.py
from __future__ import annotations
import os
import json
import time
import glob
import argparse
import sys
import unicodedata
from pathlib import Path
from urllib.parse import urlencode
import requests
from collections import defaultdict

# 모듈 경로 추가
CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
if CURRENT_DIR not in sys.path:
    sys.path.append(CURRENT_DIR)

try:
    from kofic_api import get_session, API_KEYS
except ImportError:
    print(f"[오류] 'kofic_api' 모듈을 찾을 수 없습니다.")
    exit(1)

HERE = Path(__file__).resolve()
ROOT = HERE.parents[1] if HERE.parents[1].name == "MyMovieProject" else HERE.parents[2]
DETAIL_DIR = ROOT / "docs" / "data" / "movies"
# [변경] 영화 상세 대신 '영화인 목록' 검색 API 사용
PEOPLE_LIST_URL = "https://www.kobis.or.kr/kobisopenapi/webservice/rest/people/searchPeopleList.json"

CURRENT_KEY_INDEX = 0

def load_json(p: Path):
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except:
        return None

def save_json(p: Path, data: dict):
    p.parent.mkdir(parents=True, exist_ok=True)
    with open(p, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

def is_h_name(name: str) -> bool:
    if not name: return False
    norm_name = unicodedata.normalize('NFC', name)
    first_char = norm_name[0]
    return '\ud558' <= first_char <= '\ud7a3'

def normalize_title(title):
    """영화 제목 매칭을 위해 공백/특수문자 제거"""
    if not title: return ""
    return "".join(c for c in title if c.isalnum()).lower()

def get_next_key_session():
    global CURRENT_KEY_INDEX
    if not API_KEYS: return None, None
    CURRENT_KEY_INDEX = (CURRENT_KEY_INDEX + 1) % len(API_KEYS)
    api_key = API_KEYS[CURRENT_KEY_INDEX]
    session = requests.Session()
    print(f"[system] 🔄 API Key switched to index {CURRENT_KEY_INDEX}")
    return session, api_key

def fetch_people_list_smart(peopleNm):
    global CURRENT_KEY_INDEX
    if not API_KEYS: raise RuntimeError("No API Keys")
    
    api_key = API_KEYS[CURRENT_KEY_INDEX]
    session = requests.Session()
    max_retries = len(API_KEYS)
    
    for attempt in range(max_retries + 1):
        try:
            # 동명이인 처리를 위해 한 번에 100명까지 검색
            qs = urlencode({"key": api_key, "peopleNm": peopleNm, "itemPerPage": 100})
            url = f"{PEOPLE_LIST_URL}?{qs}"
            r = session.get(url, timeout=10)
            r.raise_for_status()
            j = r.json()
            
            fault = j.get("faultInfo") or j.get("faultResult")
            if fault:
                err_code = fault.get("errorCode")
                if err_code == '320011':
                    print(f"[warning] Key exhausted. Switching...")
                    session, api_key = get_next_key_session()
                    continue
                else:
                    raise RuntimeError(f"KOBIS fault: {fault.get('message')}")
            return j
        except Exception as e:
            if attempt == max_retries: raise e
            time.sleep(1)
    raise RuntimeError("All API keys exhausted.")

def backfill(budget: int, rate_sleep_ms: int):
    # 1. 로컬 파일 스캔 및 타겟(코드 없는 'ㅎ' 배우) 수집
    files = sorted([Path(p) for p in glob.glob(str(DETAIL_DIR / "**" / "*.json"), recursive=True)], reverse=True)
    
    print(f"[Step 1] 전체 파일({len(files)}개) 스캔 중... 'ㅎ'씨 배우 타겟팅")
    
    # name -> list of {path: Path, movieNm: str}
    target_map = defaultdict(list)
    
    for p in files:
        raw = load_json(p)
        if not raw: continue
        
        info = raw if raw.get("movieCd") else ((raw.get("movieInfoResult") or {}).get("movieInfo") or {})
        movieNm = info.get("movieNm")
        if not movieNm: continue

        # 한국 영화만 대상
        nations = info.get("nations") or []
        if not any(n.get("nationNm") == "한국" for n in nations):
            continue
            
        actors = info.get("actors") or []
        for a in actors:
            nm = a.get("peopleNm", "").strip()
            cd = a.get("peopleCd", "").strip()
            
            # 코드가 없고 'ㅎ'으로 시작하는 경우 수집
            if nm and (not cd) and is_h_name(nm):
                target_map[nm].append({
                    "path": p,
                    "movieNm": movieNm,
                    "cleanNm": normalize_title(movieNm)
                })

    target_names = sorted(target_map.keys(), key=lambda k: len(target_map[k]), reverse=True)
    print(f"[Step 1 완료] 총 {len(target_names)}명의 'ㅎ'씨 배우(코드 미보유) 발견.")
    if target_names:
        print(f" -> 주요 타겟: {target_names[:5]} 등...")

    # 2. 배우 이름별로 API 검색 및 매칭
    used = 0
    updated_files_count = 0
    
    for name in target_names:
        if used >= budget:
            print(f"[Stop] 예산 소진 ({used}회).")
            break
            
        occurrences = target_map[name]
        print(f"[API 검색] 배우 '{name}' (관련 영화 {len(occurrences)}편) 찾는 중...")
        
        try:
            # API 호출: 배우 이름으로 검색
            res = fetch_people_list_smart(name)
            used += 1
            if rate_sleep_ms > 0: time.sleep(rate_sleep_ms / 1000.0)
            
            people_list = (res.get("peopleListResult") or {}).get("peopleList") or []
            
            # 검색 결과가 없으면 패스
            if not people_list:
                print(f" -> 결과 없음.")
                continue

            # 3. 매칭 로직: API에서 나온 배우들의 '필모그래피'와 내 로컬 영화 제목 비교
            matched_movies_for_actor = 0
            
            # 로컬 파일을 매번 다시 읽어서 업데이트 (한 파일에 여러 배우가 있을 수 있으므로)
            # 효율을 위해: path별로 업데이트 할 내용을 모은 뒤 한 번에 쓰면 좋겠지만,
            # 코드 단순화를 위해 발견 즉시 업데이트 시도.
            
            for person in people_list:
                pid = person.get("peopleCd")
                filmos = person.get("filmoNames", "") # "영화1|영화2|..."
                if not filmos or not pid: continue
                
                filmo_set = set(normalize_title(t) for t in filmos.split("|"))
                
                # 이 'person'이 출연한 영화가 내 로컬 타겟 목록에 있는지 확인
                for target in occurrences:
                    if target["cleanNm"] in filmo_set:
                        # 매칭 성공! 파일 업데이트
                        f_path = target["path"]
                        f_data = load_json(f_path)
                        if not f_data: continue
                        
                        f_info = f_data if f_data.get("movieCd") else ((f_data.get("movieInfoResult") or {}).get("movieInfo") or {})
                        actors_list = f_info.get("actors") or []
                        
                        changed = False
                        for ac in actors_list:
                            # 이름이 같고 코드가 비어있으면 채워넣기
                            if ac.get("peopleNm") == name and not ac.get("peopleCd"):
                                ac["peopleCd"] = pid
                                changed = True
                        
                        if changed:
                            # 구조 유지하며 저장
                            if f_data.get("movieCd"):
                                f_data["actors"] = actors_list
                            elif "movieInfoResult" in f_data:
                                f_data["movieInfoResult"]["movieInfo"]["actors"] = actors_list
                                
                            save_json(f_path, f_data)
                            matched_movies_for_actor += 1
                            updated_files_count += 1
            
            if matched_movies_for_actor > 0:
                print(f" -> ✅ {matched_movies_for_actor}편의 영화에 코드({name}) 주입 완료!")
            
        except Exception as e:
            print(f"[Error] {name} 처리 중 오류: {e}")
            time.sleep(1)

    print(f"=== 최종 완료: 총 {updated_files_count}번의 파일 업데이트 발생, API {used}회 사용 ===")

if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--budget", type=int, default=3000)
    ap.add_argument("--rate-sleep-ms", type=int, default=100)
    args, unknown = ap.parse_known_args()
    
    if not API_KEYS:
        print("[error] API 키가 없습니다.")
        exit(1)
        
    backfill(args.budget, args.rate_sleep_ms)
