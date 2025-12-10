# scripts/build_people_details.py
import os
import json
import time
import glob
import sys
import unicodedata
from pathlib import Path
from urllib.parse import urlencode
import requests

CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
if CURRENT_DIR not in sys.path:
    sys.path.append(CURRENT_DIR)

try:
    from kofic_api import get_session, API_KEYS
except ImportError:
    API_KEYS = []
    def get_session(): return None, None

HERE = Path(__file__).resolve()
ROOT = HERE.parents[1] if HERE.parents[1].name == "MyMovieProject" else HERE.parents[2]
MOVIE_DIR = ROOT / "docs" / "data" / "movies"
PEOPLE_DIR = ROOT / "docs" / "data" / "people"
PEOPLE_INFO_URL = "https://www.kobis.or.kr/kobisopenapi/webservice/rest/people/searchPeopleInfo.json"

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

def is_korean_movie(data: dict) -> bool:
    info = data if data.get("movieCd") else ((data.get("movieInfoResult") or {}).get("movieInfo") or {})
    nations = info.get("nations") or []
    for n in nations:
        if n.get("nationNm") == "한국":
            return True
    return False

def is_h_name(name: str) -> bool:
    if not name: return False
    norm_name = unicodedata.normalize('NFC', name)
    first_char = norm_name[0]
    return '\ud558' <= first_char <= '\ud7a3'

def get_next_key_session():
    global CURRENT_KEY_INDEX
    if not API_KEYS: return None, None
    CURRENT_KEY_INDEX = (CURRENT_KEY_INDEX + 1) % len(API_KEYS)
    api_key = API_KEYS[CURRENT_KEY_INDEX]
    session = requests.Session()
    print(f"[system] 🔄 API Key switched to index {CURRENT_KEY_INDEX}")
    return session, api_key

def fetch_people_info_smart(peopleCd):
    global CURRENT_KEY_INDEX
    if not API_KEYS: raise RuntimeError("No API Keys")
    
    api_key = API_KEYS[CURRENT_KEY_INDEX]
    session = requests.Session()
    max_retries = len(API_KEYS)
    
    for attempt in range(max_retries + 1):
        try:
            qs = urlencode({"key": api_key, "peopleCd": peopleCd})
            url = f"{PEOPLE_INFO_URL}?{qs}"
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

def build_details():
    if not API_KEYS:
        print("[build_people] No API Keys. Skipping.")
        return

    files = sorted(glob.glob(str(MOVIE_DIR / "**" / "*.json"), recursive=True))
    target_people = set()

    print(f"[scan] 'ㅎ'씨 배우 상세정보 수집 대상을 찾습니다...")
    
    korean_movie_cnt = 0
    for p in files:
        data = load_json(Path(p))
        if not data: continue
        
        if not is_korean_movie(data):
            continue

        korean_movie_cnt += 1
        info = data if data.get("movieCd") else ((data.get("movieInfoResult") or {}).get("movieInfo") or {})
        
        for key in ["directors", "actors"]:
            for person in (info.get(key) or []):
                code = person.get("peopleCd", "").strip()
                name = person.get("peopleNm", "").strip()
                
                if code and name and is_h_name(name):
                    target_people.add(code)

    print(f"[info] 한국 영화 {korean_movie_cnt}편 분석 결과: 'ㅎ'씨 배우/감독 {len(target_people)}명 코드 발견.")

    count = 0
    sorted_people = sorted(list(target_people))
    
    needed_people = []
    for code in sorted_people:
        person_file = PEOPLE_DIR / f"{code}.json"
        if not (person_file.exists() and person_file.stat().st_size > 50):
            needed_people.append(code)

    print(f"[info] 신규 수집 필요 인원: {len(needed_people)}명")

    for i, code in enumerate(needed_people):
        person_file = PEOPLE_DIR / f"{code}.json"
        
        try:
            data = fetch_people_info_smart(code)
            
            p_result = data.get("peopleInfoResult")
            p_info = p_result.get("peopleInfo") if p_result else None
            
            if p_info:
                save_json(person_file, data)
                sex = p_info.get('sex') or 'Unknown'
                name = p_info.get('peopleNm')
                print(f"[{i+1}/{len(needed_people)}] Saved {name} ({sex}) - {code}")
                count += 1
                time.sleep(0.1) 
            else:
                print(f"[skip] No info for {code}")
                
        except RuntimeError as re:
            if "All API keys exhausted" in str(re):
                print("[STOP] 모든 키 소진. 저장을 진행합니다.")
                break
            print(f"[error] {code}: {re}")
        except Exception as e:
            print(f"[error] {code}: {e}")
            time.sleep(1)

    print(f"[done] 'ㅎ'씨 배우 데이터 신규 저장: {count}건")

if __name__ == "__main__":
    build_details()
