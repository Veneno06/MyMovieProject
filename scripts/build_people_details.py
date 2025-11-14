#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os, json, time
from pathlib import Path
from urllib.parse import urlencode

# API 키 관리자 임포트 (중요)
try:
    from kofic_api import get_session
except ImportError:
    print("kofic_api.py가 필요합니다. 동일 디렉토리에 있는지 확인하세요.")
    exit(1)

# 경로 설정
ROOT = Path(__file__).resolve().parents[1]
SEARCH_DIR = ROOT / "docs" / "data" / "search"
PEOPLE_INDEX_PATH = SEARCH_DIR / "people.json"
PEOPLE_DETAILS_PATH = SEARCH_DIR / "people_details.json"

# KOFIC API 엔드포인트
PEOPLE_INFO_URL = "https://www.kobis.or.kr/kobisopenapi/webservice/rest/people/searchPeopleInfo.json"

def load_json(p: Path, default=None):
    if not p.exists(): return default
    try:
        with p.open("r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return default

def save_json(p: Path, data):
    p.parent.mkdir(parents=True, exist_ok=True)
    tmp = p.with_suffix(".json.tmp")
    tmp.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp.replace(p)

def fetch_people_info(session: requests.Session, api_key: str, peopleCd: str):
    """searchPeopleInfo API를 호출하여 성별 등의 상세 정보를 가져옵니다."""
    params = urlencode({"key": api_key, "peopleCd": peopleCd})
    url = f"{PEOPLE_INFO_URL}?{params}"
    
    r = session.get(url, timeout=(10, 60))
    r.raise_for_status()
    j = r.json()
    if j.get("faultInfo") or j.get("faultResult"):
        raise RuntimeError(f"KOBIS fault: {j.get('faultInfo') or j.get('faultResult')}")
    
    return j.get("peopleInfoResult", {}).get("peopleInfo", {})

def main():
    print(f"[build_people_details] 인물 상세 정보(성별 등) 수집을 시작합니다.")
    
    # 1. 기존 people.json 인덱스 로드
    people_index = load_json(PEOPLE_INDEX_PATH)
    if not people_index or "people" not in people_index:
        print(f"[오류] {PEOPLE_INDEX_PATH} 파일을 찾을 수 없거나 형식이 잘못되었습니다.")
        return

    all_people = people_index["people"]
    
    # 2. 이미 수집한 상세 정보가 있다면 로드 (증분 수집)
    existing_details = load_json(PEOPLE_DETAILS_PATH, {})
    
    # 3. KOFIC 코드가 있는(peopleCd) 모든 인물 추출
    # (코드가 없는 'name::' 인물은 이 API를 호출할 수 없음)
    targets = [p for p in all_people if p.get("peopleCd")]
    
    print(f"총 {len(all_people)}명 인물 중, KOFIC 코드가 있는 {len(targets)}명을 대상으로 수집/업데이트합니다.")
    
    rate_sleep_ms = 250 # API 호출 간 대기
    updated_count = 0
    
    for i, person in enumerate(targets):
        peopleCd = person["peopleCd"]
        
        # 4. 이미 수집한 정보가 있으면 건너뛰기
        if peopleCd in existing_details:
            continue
            
        print(f"  -> ({i+1}/{len(targets)}) {peopleCd} ({person['peopleNm']}) 정보 수집 중...", end="\r")
        
        try:
            # 5. API 키 로테이터에서 새 세션과 키 가져오기
            session, api_key = get_session()
            
            # 6. API 호출
            info = fetch_people_info(session, api_key, peopleCd)
            
            if info:
                # 7. 필요한 정보(성별 등)만 추출하여 저장
                detail_data = {
                    "peopleCd": info.get("peopleCd"),
                    "peopleNm": info.get("peopleNm"),
                    "sex": info.get("sex"),
                    # KOFIC은 생년월일을 제공하지 않습니다.
                }
                existing_details[peopleCd] = detail_data
                updated_count += 1
            
            time.sleep(rate_sleep_ms / 1000.0)
            
        except Exception as e:
            print(f"\n[경고] {peopleCd} ({person['peopleNm']}) 정보 수집 실패: {e}")
            # 오류 발생 시 5초 대기 (API 키 제한 등일 수 있음)
            time.sleep(5) 

    print(f"\n[build_people_details] 총 {updated_count}명의 신규 인물 정보를 수집했습니다.")
    
    # 8. 최종본 저장
    save_json(PEOPLE_DETAILS_PATH, existing_details)
    print(f"[build_people_details] {PEOPLE_DETAILS_PATH} 파일 저장을 완료했습니다. (총 {len(existing_details)}명)")

if __name__ == "__main__":
    main()
