#!/usr/bin/env python3
# -*- coding: utf-8 -*-

#
# build_people_details.py
#
# docs/data/search/people.json을 읽어, peopleCd가 있는 모든 인물에 대해
# KOFIC '인물 상세' API를 호출하여 성별(sex) 등의 추가 정보를 가져옵니다.
#
import os
import json
import time
from pathlib import Path
from urllib.parse import urlencode

# [수정] 누락되었던 requests 라이브러리 임포트
import requests

try:
    from kofic_api import get_session, API_KEYS, KoficApiError
except ImportError:
    print("kofic_api.py가 필요합니다. 동일 디렉토리에 있는지 확인하세요.")
    exit(1)

# 경로 설정
ROOT = Path(__file__).resolve().parents[1]
SEARCH_DIR = ROOT / "docs" / "data" / "search"
PEOPLE_INDEX_PATH = SEARCH_DIR / "people.json"
OUTPUT_PATH = SEARCH_DIR / "people_details.json" # 최종 결과물

PEOPLE_INFO_URL = "https://www.kobis.or.kr/kobisopenapi/webservice/rest/people/searchPeopleInfo.json"

def load_json(p: Path, default=None):
    if not p.exists(): return default
    try:
        with p.open("r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return default

def save_json(p: Path, data: dict):
    p.parent.mkdir(parents=True, exist_ok=True)
    tmp = p.with_suffix(".json.tmp")
    tmp.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp.replace(p)

def fetch_people_info(session: requests.Session, api_key: str, peopleCd: str):
    """KOFIC 인물 상세 API 호출"""
    qs = urlencode({"key": api_key, "peopleCd": peopleCd})
    url = f"{PEOPLE_INFO_URL}?{qs}"
    r = session.get(url, timeout=(10, 60))
    r.raise_for_status()
    j = r.json()
    if j.get("faultInfo") or j.get("faultResult"):
        raise KoficApiError(f"KOBIS fault: {j.get('faultInfo') or j.get('faultResult')}")
    return j

def build_details(budget: int, rate_sleep_ms: int):
    # 1. 원본 people.json 로드
    people_index = load_json(PEOPLE_INDEX_PATH)
    if not people_index or "people" not in people_index:
        print(f"[error] {PEOPLE_INDEX_PATH} 파일을 찾을 수 없거나 형식이 잘못되었습니다.")
        return
    
    all_people = people_index.get("people", [])
    
    # 2. peopleCd가 있는 인물만 추출
    target_people = [p for p in all_people if p.get("peopleCd")]
    target_count = len(target_people)
    print(f"[info] 총 {len(all_people)}명 중 peopleCd가 있는 {target_count}명을 대상으로 상세 정보를 수집합니다.")

    # 3. 기존 상세 정보가 있다면 로드 (증분 업데이트용)
    details_map = load_json(OUTPUT_PATH, {})
    
    # 4. API 호출 및 정보 수집
    updated_count = 0
    used_api = 0
    
    for i, person in enumerate(target_people):
        peopleCd = person["peopleCd"]
        
        # 이미 수집한 정보가 있으면 건너뛰기
        if peopleCd in details_map:
            continue

        if used_api >= budget:
            print(f"[info] API budget({budget})에 도달하여 중단합니다.")
            break
            
        print(f"  -> ({i+1}/{target_count}) Fetching {peopleCd} ({person.get('peopleNm', 'N/A')})...", end="\r")

        try:
            session, api_key = get_session()
            info_data = fetch_people_info(session, api_key, peopleCd)
            used_api += 1

            p_info = (info_data.get("peopleInfoResult") or {}).get("peopleInfo") or {}
            
            # 필요한 정보만 추출 (성별, 필모 수)
            sex = p_info.get("sex", "")
            filmos = p_info.get("filmos", [])
            
            details_map[peopleCd] = {
                "peopleCd": peopleCd,
                "peopleNm": p_info.get("peopleNm", person.get("peopleNm")), # 원본 이름 우선
                "sex": "남" if sex == "남자" else ("여" if sex == "여자" else ""),
                "filmoCount": len(filmos)
            }
            updated_count += 1
            
            if rate_sleep_ms > 0:
                time.sleep(rate_sleep_ms / 1000.0)

        except KoficApiError as e:
            # API 키 만료 등 심각한 오류 시 중단
            print(f"\n[error] API 오류 발생 (peopleCd: {peopleCd}): {e}. 작업을 중단합니다.")
            break
        except Exception as e:
            # 타임아웃 등 일반 오류 시 건너뛰기
            print(f"\n[warn] 건너뛰기 (peopleCd: {peopleCd}): {e}")
            time.sleep(1) # 오류 발생 시 1초 대기
            continue
            
    print(f"\n[done] 총 {updated_count}명의 신규 인물 정보를 수집했습니다. (API {used_api}회 사용)")
    
    # 5. 최종 파일 저장
    save_json(OUTPUT_PATH, details_map)
    print(f"[save] {OUTPUT_PATH} 파일에 총 {len(details_map)}명의 상세 정보를 저장했습니다.")


if __name__ == "__main__":
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("--budget", type=int, default=3000, help="API 호출 예산 (인물 상세는 1일 3000회)")
    ap.add_argument("--rate-sleep-ms", type=int, default=350, help="호출 간 대기(ms) (인물 상세는 300ms 권장)")
    args = ap.parse_args()

    if not API_KEYS:
        print("[build_people_details] API 키가 로드되지 않았습니다. GitHub Secrets를 확인하세요.")
        exit(1)
        
    build_details(args.budget, args.rate_sleep_ms)
