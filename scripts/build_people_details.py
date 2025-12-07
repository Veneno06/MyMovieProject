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

# [신규 기능] 이름의 첫 글자가 'ㅎ' (히읗) 자음으로 시작하는지 확인
def is_h_name(name: str) -> bool:
    if not name: return False
    # 유니코드 범위: '하'(U+D558) ~ '힣'(U+D7A3)
    first_char = name[0]
    return '\ud558' <= first_char <= '\ud7a3'

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

    print(f"[scan] Scanning {len(files)} movies for actors...")
    print(f"[filter] 적용된 필터: 한국 영화 + 이름이 'ㅎ'으로 시작하는 배우")

    korean_movie_cnt = 0

    for p in files:
        data = load_json(Path(p))
        if not data: continue
        
        # [기존 로직 유지] 한국 영화만 대상
        if not is_korean_movie(data):
            continue

        korean_movie_cnt += 1
        info = data if data.get("movieCd") else ((data.get("movieInfoResult") or {}).get("movieInfo") or {})
        
        for key in ["directors", "actors"]:
            for person in (info.get(key) or []):
                code = person.get("peopleCd", "").strip()
                name = person.get("peopleNm", "").strip()
                
                # [수정된 로직] 코드가 있고 + 이름이 'ㅎ'으로 시작하는 경우만 수집
                if code and name and is_h_name(name):
                    target_people.add(code)

    print(f"[info] 한국 영화 {korean_movie_cnt}편에서 'ㅎ'씨 배우 {len(target_people)}명 코드 수집 완료.")

    # 2. 각 배우별 상세 정보(성별 등) 수집
    count = 0
    sorted_people = sorted(list(target_people))
    
    # 디버깅용 로그
    print(f"[debug] 수집 대상(상위 5명): {sorted_people[:5]} ...")

    for i, code in enumerate(sorted_people):
        person_file = PEOPLE_DIR / f"{code}.json"
        
        # [기존 로직 유지] 이미 파일이 있고, 내용이 유효하면 건너뜀 (API 절약 핵심)
        if person_file.exists():
            if person_file.stat().st_size > 50: # 빈 파일이 아니면 패스
                continue

        try:
            session, api_key = get_session()
            if not session:
                print("[stop] API 키 소진 또는 세션 오류.")
                break

            data = fetch_people_info(session, api_key, code)
            
            p_result = data.get("peopleInfoResult")
            p_info = p_result.get("peopleInfo") if p_result else None
            
            if p_info:
                # [중요] 원본 데이터 전체 저장 (reindex_search.py 호환성 유지)
                save_json(person_file, data)
                
                sex = p_info.get('sex') or 'Unknown'
                name = p_info.get('peopleNm')
                print(f"[{i+1}/{len(sorted_people)}] Saved {name} ({sex})")
                
                count += 1
                time.sleep(0.1) 
            else:
                print(f"[skip] No info for {code}")
                
        except Exception as e:
            print(f"[error] {code}: {e}")
            time.sleep(1)

    print(f"[done] 'ㅎ'씨 배우 데이터 신규 저장: {count}건")

if __name__ == "__main__":
    build_details()
