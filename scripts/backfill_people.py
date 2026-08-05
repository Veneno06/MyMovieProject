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
    except: return None

def save_json(p: Path, data: dict):
    p.parent.mkdir(parents=True, exist_ok=True)
    with open(p, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

def normalize_title(title):
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

def backfill(budget: int, rate_sleep_ms: int, target_names_input: str):
    try:
        limit_file = ROOT / "budget_limit.txt"
        limit_file.write_text(str(budget), encoding="utf-8")
    except: pass

    # 수동 타겟 이름 파싱
    manual_targets = [n.strip() for n in target_names_input.split(",") if n.strip()]
    if manual_targets:
        print(f"[System] 🚀 수동 타겟 지정됨: {manual_targets} (이 인물들만 최우선 스캔합니다)")

    files = sorted([Path(p) for p in glob.glob(str(DETAIL_DIR / "**" / "*.json"), recursive=True)], reverse=True)
    
    print(f"[Step 1] 전체 파일({len(files)}개) 스캔 중... 누락된 인물 매핑")
    
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
            
        # 배우와 감독 모두 스캔 (과거 코드의 감독 누락 버그 해결)
        for role_key in ["actors", "directors"]:
            people_list = info.get(role_key) or []
            for a in people_list:
                nm = a.get("peopleNm", "").strip()
                cd = a.get("peopleCd", "").strip()
                
                # 코드가 없는 사람만 추려냄
                if nm and not cd:
                    target_map[nm].append({
                        "path": p, "movieNm": movieNm, "cleanNm": normalize_title(movieNm), "role_key": role_key
                    })

    # 빈도수(참여 영화 수)가 많은 사람부터 먼저 채우도록 정렬 (스마트 알고리즘 적용)
    all_missing_names = sorted(target_map.keys(), key=lambda k: len(target_map[k]), reverse=True)
    
    # 만약 수동 지정된 이름이 있다면, 전체 목록 대신 지정된 이름만 사용
    if manual_targets:
        target_names = [n for n in manual_targets if n in target_map]
        print(f" -> 지정된 타겟 중 누락이 확인된 인물: {target_names}")
    else:
        target_names = all_missing_names
        print(f"[Step 1 완료] 총 {len(target_names)}명의 대상 인물 발견. 최다 출연 누락 타겟: {target_names[:10]}")

    used = 0
    updated_files_count = 0
    
    for name in target_names:
        if used >= budget:
            print(f"[Stop] 예산 소진 ({used}회). 다음 업데이트를 기약합니다.")
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
                        role_key = target["role_key"]
                        f_data = load_json(f_path)
                        if not f_data: continue
                        
                        f_info = f_data if f_data.get("movieCd") else ((f_data.get("movieInfoResult") or {}).get("movieInfo") or {})
                        people_list_in_movie = f_info.get(role_key) or []
                        
                        changed = False
                        for ac in people_list_in_movie:
                            if ac.get("peopleNm") == name and not ac.get("peopleCd"):
                                ac["peopleCd"] = pid
                                changed = True
                        
                        if changed:
                            if f_data.get("movieCd"): f_data[role_key] = people_list_in_movie
                            elif "movieInfoResult" in f_data: f_data["movieInfoResult"]["movieInfo"][role_key] = people_list_in_movie
                            save_json(f_path, f_data)
                            matched += 1
                            updated_files_count += 1
            if matched > 0:
                print(f" -> ✅ 인물 '{name}': {matched}편 업데이트 성공")
        except Exception as e:
            print(f"[Error] {name}: {e}")
            time.sleep(1)

    print(f"=== 최종 완료: 총 {updated_files_count}번 파일 업데이트, API {used}회 사용 ===")

if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--budget", type=int, default=3000)
    ap.add_argument("--rate-sleep-ms", type=int, default=100)
    ap.add_argument("--target-names", type=str, default="")
    args, unknown = ap.parse_known_args()
    
    if not API_KEYS:
        print("[error] No API Keys.")
        exit(1)
        
    backfill(args.budget, args.rate_sleep_ms, args.target_names)
