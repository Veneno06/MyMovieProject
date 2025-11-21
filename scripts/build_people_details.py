import os
import json
import time
from pathlib import Path
try:
    import kofic_api
except ImportError:
    import sys
    sys.path.append(os.path.dirname(os.path.abspath(__file__)))
    import kofic_api

ROOT = Path(__file__).resolve().parents[1]
PEOPLE_INDEX = ROOT / "docs" / "data" / "search" / "people.json"
PEOPLE_DETAILS = ROOT / "docs" / "data" / "search" / "people_details.json"
SEARCH_PEOPLE_INFO_URL = "https://www.kobis.or.kr/kobisopenapi/webservice/rest/people/searchPeopleInfo.json"

def load_json(p):
    if not p.exists(): return {}
    with open(p, "r", encoding="utf-8") as f: return json.load(f)

def save_json(p, data):
    with open(p, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

def run_details(limit=3000):
    print(">>> 배우 상세정보(성별) 수집 시작...")
    
    # 1. 기존 인덱스 로드
    index_data = load_json(PEOPLE_INDEX)
    people_list = index_data.get("people", [])
    
    # 2. 기존 상세정보 로드 (이미 받은 건 건너뛰기 위해)
    details_db = load_json(PEOPLE_DETAILS)
    
    count = 0
    for p in people_list:
        code = p.get("peopleCd")
        name = p.get("peopleNm")
        
        # 코드가 없거나, 이미 성별 정보를 가지고 있으면 패스
        if not code or code in details_db:
            continue
            
        print(f"  [{count+1}] 상세정보 조회: {name} ({code})")
        
        try:
            res = kofic_api.fetch(SEARCH_PEOPLE_INFO_URL, {"peopleCd": code})
            info = res.get("peopleInfoResult", {}).get("peopleInfo")
            
            if info:
                # 필요한 정보만 저장
                details_db[code] = {
                    "sex": info.get("sex"),
                    "repRoleNm": info.get("repRoleNm"),
                    "homepages": info.get("homepages"),
                    "updateAt": int(time.time())
                }
                print(f"    -> 성별: {info.get('sex')}")
            
            count += 1
            if count >= limit:
                print(">>> API 호출 한도 도달.")
                break
            
            time.sleep(0.2)
            
        except Exception as e:
            print(f"    -> 오류: {e}")

    # 저장 (기존 데이터 + 새 데이터)
    save_json(PEOPLE_DETAILS, details_db)
    print(f">>> 저장 완료. 총 {len(details_db)}명 정보 보유.")

if __name__ == "__main__":
    import sys
    limit = int(sys.argv[1]) if len(sys.argv) > 1 else 3000
    run_details(limit)
