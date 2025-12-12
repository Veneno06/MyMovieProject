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

def normalize_title(title):
    if not title: return ""
    return "".join(c for c in title if c.isalnum()).lower()

# [핵심] 자음 필터링 함수
def is_target_consonant(name: str) -> bool:
    if not name: return False
    nm = unicodedata.normalize('NFC', name)
    first_char = nm[0]
    
    if not ('\uAC00' <= first_char <= '\uD7A3'):
        return False
    
    # 초성 인덱스: (유니코드 - 0xAC00) // 588
    # 0:ㄱ, 1:ㄲ, 2:ㄴ, 3:ㄷ, 4:ㄸ, 5:ㄹ ...
    idx = (ord(first_char) - 0xAC00) // 588
    
    # ==========================================
    # [설정] 여기를 수정해서 타겟을 변경합니다.
    # 1차 작업용: ㄱ, ㄲ, ㄴ (0, 1, 2)
    # ==========================================
    return idx in [0, 1, 2]

# [스마트 키 교체] 다음 키 가져오기
def get_next_key_session():
    global CURRENT_KEY_INDEX
    if not API_KEYS: return None, None
    CURRENT_KEY_INDEX = (CURRENT_KEY_INDEX + 1) % len(API_KEYS)
    api_key = API_KEYS[CURRENT_KEY_INDEX]
    session = requests.Session()
    print(f"[system] 🔄 API Key switched to index {CURRENT_KEY_INDEX}")
    return session, api_key

# [스마트 키 교체] API 호출 및 자동 교체
def fetch_people_list_smart(peopleNm):
    global CURRENT_KEY_INDEX
    if not API_KEYS: raise RuntimeError("No API Keys")
    api_key = API_KEYS[CURRENT_KEY_INDEX]
    session = requests.Session()
    max_retries = len(API_KEYS)
    
    for attempt in range(max_retries + 1):
        try:
            qs = urlencode({"key": api_key, "peopleNm": peopleNm, "itemPerPage": 100})
            url = f"{PEOPLE_LIST_URL}?{qs}"
            r = session.get(url, timeout=10)
            r.raise_for_status()
            j = r.json()
            
            fault = j.get("faultInfo") or j.get("faultResult")
            if fault:
                if str(fault.get("errorCode")) == '320011':
                    print(f"[warning] Key exhausted. Switching...")
                    session, api_key = get_next_key_session()
                    continue
                else: raise RuntimeError(f"KOBIS fault: {fault.get('message')}")
            return j
        except Exception as e:
            if attempt == max_retries: raise e
            time.sleep(1)
    raise RuntimeError("All API keys exhausted.")

def backfill(budget: int, rate_sleep_ms: int):
    files = sorted([Path(p) for p in glob.glob(str(DETAIL_DIR / "**" / "*.json"), recursive=True)], reverse=True)
    
    print(f"[Step 1] 전체 파일({len(files)}개) 스캔 중... 'ㄱ, ㄴ' 배우 타겟팅")
    
    target_map = defaultdict(list)
    
    for p in files:
        raw = load_json(p)
        if not raw: continue
        info = raw if raw.get("movieCd") else ((raw.get("movieInfoResult") or {}).get("movieInfo") or {})
        movieNm = info.get("movieNm")
        if not movieNm: continue

        nations = info.get("nations") or []
        if not any(n.get("nationNm") == "한국" for n in nations):
            continue
            
        actors = info.get("actors") or []
        for a in actors:
            nm = a.get("peopleNm", "").strip()
            cd = a.get("peopleCd", "").strip()
            
            # 코드가 없고 + 타겟 자음(ㄱ,ㄴ)인 경우만 수집
            if nm and (not cd) and is_target_consonant(nm):
                target_map[nm].append({
                    "path": p, "movieNm": movieNm, "cleanNm": normalize_title(movieNm)
                })

    target_names = sorted(target_map.keys(), key=lambda k: len(target_map[k]), reverse=True)
    print(f"[Step 1 완료] 총 {len(target_names)}명의 대상 배우(코드 미보유) 발견.")
    if target_names: print(f" -> 주요 타겟: {target_names[:10]}")

    used = 0
    updated_files_count = 0
    
    for name in target_names:
        if used >= budget:
            print(f"[Stop] 예산 소진 ({used}회).")
            break
            
        occurrences = target_map[name]
        try:
            res = fetch_people_list_smart(name)
            used += 1
            if rate_sleep_ms > 0: time.sleep(rate_sleep_ms / 1000.0)
            
            people_list = (res.get("peopleListResult") or {}).get("peopleList") or []
            if not people_list: continue

            matched = 0
            for person in people_list:
                pid = person.get("peopleCd")
                filmos = person.get("filmoNames", "")
                if not filmos or not pid: continue
                filmo_set = set(normalize_title(t) for t in filmos.split("|"))
                
                for target in occurrences:
                    if target["cleanNm"] in filmo_set:
                        f_path = target["path"]
                        f_data = load_json(f_path)
                        if not f_data: continue
                        f_info = f_data if f_data.get("movieCd") else ((f_data.get("movieInfoResult") or {}).get("movieInfo") or {})
                        actors_list = f_info.get("actors") or []
                        
                        changed = False
                        for ac in actors_list:
                            if ac.get("peopleNm") == name and not ac.get("peopleCd"):
                                ac["peopleCd"] = pid
                                changed = True
                        
                        if changed:
                            if f_data.get("movieCd"): f_data["actors"] = actors_list
                            elif "movieInfoResult" in f_data: f_data["movieInfoResult"]["movieInfo"]["actors"] = actors_list
                            save_json(f_path, f_data)
                            matched += 1
                            updated_files_count += 1
            if matched > 0:
                print(f" -> ✅ 배우 '{name}': {matched}편 업데이트 성공")
        except Exception as e:
            print(f"[Error] {name}: {e}")
            time.sleep(1)

    print(f"=== 최종 완료: 총 {updated_files_count}번 파일 업데이트, API {used}회 사용 ===")

if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--budget", type=int, default=3000)
    ap.add_argument("--rate-sleep-ms", type=int, default=100)
    args, unknown = ap.parse_known_args()
    if not API_KEYS:
        print("[error] No API Keys.")
        exit(1)
    backfill(args.budget, args.rate_sleep_ms)
