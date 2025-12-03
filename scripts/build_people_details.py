#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import json
import time
import sys
from pathlib import Path

# 모듈 경로 설정
CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
if CURRENT_DIR not in sys.path:
    sys.path.append(CURRENT_DIR)

try:
    import kofic_api
except ImportError:
    print("[오류] kofic_api.py가 필요합니다.")
    exit(1)

ROOT = Path(__file__).resolve().parents[1]
SEARCH_DIR = ROOT / "docs" / "data" / "search"
PEOPLE_INDEX_PATH = SEARCH_DIR / "people.json"
PEOPLE_DETAILS_PATH = SEARCH_DIR / "people_details.json"

SEARCH_PEOPLE_INFO_URL = "https://www.kobis.or.kr/kobisopenapi/webservice/rest/people/searchPeopleInfo.json"

def load_json(p):
    if not p.exists(): return {}
    try:
        with open(p, "r", encoding="utf-8") as f: return json.load(f)
    except: return {}

def save_json(p, data):
    with open(p, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

def run_details(budget=3000, rate_sleep_ms=250):
    print(">>> 배우 상세정보(성별) 수집 시작...")
    
    # 1. 인덱스 로드
    people_data = load_json(PEOPLE_INDEX_PATH)
    people_list = people_data.get("people", [])
    
    # 2. 기존 상세정보 로드
    details_db = load_json(PEOPLE_DETAILS_PATH)
    
    used = 0
    updated = 0
    
    # peopleCd가 있는 배우만 대상
    targets = [p for p in people_list if p.get("peopleCd")]
    
    for p in targets:
        code = p.get("peopleCd")
        name = p.get("peopleNm")
        
        if code in details_db:
            continue # 이미 있음
            
        if used >= budget:
            print(f"[info] Budget reached ({used}). Stopping.")
            break
            
        try:
            print(f"  Fetching {name} ({code})...")
            res = kofic_api.fetch(SEARCH_PEOPLE_INFO_URL, {"peopleCd": code})
            info = res.get("peopleInfoResult", {}).get("peopleInfo")
            
            if info:
                details_db[code] = {
                    "sex": info.get("sex"),
                    "repRoleNm": info.get("repRoleNm"),
                    "updateAt": int(time.time())
                }
                updated += 1
            
            used += 1
            time.sleep(rate_sleep_ms / 1000.0)
            
        except Exception as e:
            print(f"    -> Error: {e}")
            time.sleep(1)

    # 저장
    save_json(PEOPLE_DETAILS_PATH, details_db)
    print(f">>> 저장 완료. 신규 수집: {updated}명, API 사용: {used}회")

if __name__ == "__main__":
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("--budget", type=int, default=3000)
    ap.add_argument("--rate-sleep-ms", type=int, default=250)
    args, _ = ap.parse_known_args()
    
    run_details(args.budget, args.rate_sleep_ms)
