import os
import json
import time
import glob
import sys
from pathlib import Path
from urllib.parse import urlencode
import requests

# 모듈 경로 추가
CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
if CURRENT_DIR not in sys.path:
    sys.path.append(CURRENT_DIR)

try:
    from kofic_api import get_session, API_KEYS
except ImportError:
    # 모듈이 없을 경우 더미 처리
    API_KEYS = []
    def get_session(): return None, None

HERE = Path(__file__).resolve()
ROOT = HERE.parents[1] if HERE.parents[1].name == "MyMovieProject" else HERE.parents[2]
MOVIE_DIR = ROOT / "docs" / "data" / "movies"
PEOPLE_DIR = ROOT / "docs" / "data" / "people"
PEOPLE_INFO_URL = "https://www.kobis.or.kr/kobisopenapi/webservice/rest/people/searchPeopleInfo.json"

def load_json(p: Path):
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except:
        return None

def save_json(p: Path, data: dict):
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")

def is_korean_movie(data: dict) -> bool:
    info = data if data.get("movieCd") else ((data.get("movieInfoResult") or {}).get("movieInfo") or {})
    nations = info.get("nations") or []
    for n in nations:
        if n.get("nationNm") == "한국":
            return True
    return False

def fetch_people_info(session, api_key, peopleCd):
    qs = urlencode({"key": api_key, "peopleCd": peopleCd})
    url = f"{PEOPLE_INFO_URL}?{qs}"
    r = session.get(url, timeout=10)
    r.raise_for_status()
    return r.json()

def build_details():
    if not API_KEYS:
        print("[build_people] No API Keys. Skipping.")
        return

    # 1. 영화 파일에서 '한국 영화'의 배우 코드 수집
    files = sorted(glob.glob(str(MOVIE_DIR / "**" / "*.json"), recursive=True))
    target_people = set()

    print(f"[scan] Scanning {len(files)} movies for Korean actors...")
    for p in files:
        data = load_json(Path(p))
        if not data: continue
        
        if not is_korean_movie(data):
            continue

        info = data if data.get("movieCd") else ((data.get("movieInfoResult") or {}).get("movieInfo") or {})
        
        for key in ["directors", "actors"]:
            for person in (info.get(key) or []):
                code = person.get("peopleCd")
                if code:
                    target_people.add(code)

    print(f"[info] Found {len(target_people)} people codes from Korean movies.")

    # 2. 각 배우별 상세 정보(성별 등) 수집
    count = 0
    # [수정 1] 하드코딩된 limit 제거 (전체 스캔)
    
    sorted_people = sorted(list(target_people))
    
    for i, code in enumerate(sorted_people):
        person_file = PEOPLE_DIR / f"{code}.json"
        
        # 이미 파일이 있고, 내용이 유효하면 건너뜀
        if person_file.exists():
            # 빈 파일인지 체크
            if person_file.stat().st_size > 10:
                continue

        try:
            session, api_key = get_session()
            data = fetch_people_info(session, api_key, code)
            
            # 유효성 검사
            p_result = data.get("peopleInfoResult")
            p_info = p_result.get("peopleInfo") if p_result else None
            
            if p_info:
                # [수정 2] data 전체(Wrapper 포함)를 저장하여 reindex_search.py와 구조 일치시킴
                save_json(person_file, data)
                sex = p_info.get('sex') or 'Unknown'
                print(f"[{i+1}/{len(sorted_people)}] Saved {p_info.get('peopleNm')} ({sex})")
                count += 1
                time.sleep(0.1) 
            else:
                print(f"[skip] No info for {code}")
                
        except Exception as e:
            print(f"[error] {code}: {e}")
            time.sleep(1)

    print(f"[done] Processed {count} new people details.")

if __name__ == "__main__":
    build_details()
