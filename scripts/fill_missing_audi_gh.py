# scripts/fill_missing_audi_gh.py
import os
import json
import glob
import time
import requests
import sys
import argparse
from pathlib import Path

# 인코딩 설정
sys.stdout.reconfigure(encoding='utf-8')

HERE = Path(__file__).resolve()
ROOT = HERE.parents[1] if HERE.parents[1].name == "MyMovieProject" else HERE.parents[2]
MOVIE_DIR = ROOT / "docs" / "data" / "movies"

# API URL
DETAIL_URL = "http://www.kobis.or.kr/kobisopenapi/webservice/rest/movie/searchMovieInfo.json"
LIST_URL = "http://www.kobis.or.kr/kobisopenapi/webservice/rest/movie/searchMovieList.json"

API_KEYS = []
for i in range(1, 10):
    k = os.environ.get(f"KOFIC_API_KEY_{i}" if i > 1 else "KOFIC_API_KEY")
    if k: API_KEYS.append(k)

CURRENT_KEY_INDEX = 0
CHOSUNG_LIST = ['ㄱ', 'ㄲ', 'ㄴ', 'ㄷ', 'ㄸ', 'ㄹ', 'ㅁ', 'ㅂ', 'ㅃ', 'ㅅ', 'ㅆ', 'ㅇ', 'ㅈ', 'ㅉ', 'ㅊ', 'ㅋ', 'ㅌ', 'ㅍ', 'ㅎ']

def get_next_key():
    global CURRENT_KEY_INDEX
    if not API_KEYS: return None
    key = API_KEYS[CURRENT_KEY_INDEX]
    CURRENT_KEY_INDEX = (CURRENT_KEY_INDEX + 1) % len(API_KEYS)
    return key

def load_json(p: Path):
    try: return json.loads(p.read_text(encoding="utf-8"))
    except: return None

def save_json(p: Path, data: dict):
    with open(p, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

def get_initial_sound(char):
    if not char: return ""
    if char in CHOSUNG_LIST: return char
    if '가' <= char <= '힣':
        return CHOSUNG_LIST[(ord(char) - 0xAC00) // 588]
    return char

# 영화 상세 정보 조회 (관객수 확인용)
def fetch_movie_detail(movie_cd):
    for _ in range(len(API_KEYS) + 1):
        api_key = get_next_key()
        if not api_key: return None
        try:
            params = { "key": api_key, "movieCd": movie_cd }
            r = requests.get(DETAIL_URL, params=params, timeout=5)
            data = r.json()
            if "faultInfo" in data: continue
            return data
        except: time.sleep(1)
    return None

# [신규 기능] 영화 목록 검색 (이름으로 다른 코드 찾기)
def search_movie_list(movie_nm):
    for _ in range(len(API_KEYS) + 1):
        api_key = get_next_key()
        if not api_key: return None
        try:
            # 이름으로 검색
            params = { "key": api_key, "movieNm": movie_nm, "itemPerPage": 10 }
            r = requests.get(LIST_URL, params=params, timeout=5)
            data = r.json()
            if "faultInfo" in data: continue
            return data
        except: time.sleep(1)
    return None

def run_fill(pattern=None):
    target_initial = get_initial_sound(pattern[0]) if pattern else None
    print(f"[Step 1] 관객수 누락 영화 스캔... (패턴: '{pattern}' -> 초성: '{target_initial or '전체'})")
    
    files = glob.glob(str(MOVIE_DIR / "**" / "*.json"), recursive=True)
    target_files = []
    
    for p in files:
        data = load_json(Path(p))
        if not data: continue
        info = data if data.get("movieCd") else ((data.get("movieInfoResult") or {}).get("movieInfo") or {})
        
        movie_nm = info.get("movieNm", "")
        if not movie_nm: continue

        if target_initial:
            movie_initial = get_initial_sound(movie_nm[0])
            if movie_initial.upper() != target_initial.upper():
                continue

        audi = info.get("audiAcc")
        # 관객수가 없거나 0인 경우만 타겟
        if not audi or str(audi).strip() in ["0", "", "None"]:
            target_files.append((Path(p), info.get("movieCd"), movie_nm, info.get("openDt", "")))

    print(f" -> '{target_initial or '전체'}' 해당 누락 영화: {len(target_files)}개 발견. 정밀 조회 시작...")
    
    success_count = 0
    # 타임아웃 방지: 1회 최대 200개 (정밀 조회는 API 호출이 많음)
    # 개당 최대 10번 호출 가능하므로 안전하게 줄임
    LIMIT = 200 
    
    for idx, (f_path, original_cd, movie_nm, original_open_dt) in enumerate(target_files[:LIMIT]):
        print(f"[{idx+1}/{min(len(target_files), LIMIT)}] '{movie_nm}' 확인 중...", end="", flush=True)
        
        # 1. 기존 코드로 먼저 조회
        res = fetch_movie_detail(original_cd)
        final_audi = "0"
        
        if res and "movieInfoResult" in res:
            final_audi = res["movieInfoResult"]["movieInfo"].get("audiAcc", "0")

        # 2. 기존 코드가 0명이면 -> 이름으로 다른 코드 찾기 (Deep Search)
        if str(final_audi) == "0":
            print(" (기존코드 0명 -> 대체코드 검색)", end="")
            list_res = search_movie_list(movie_nm)
            
            if list_res and "movieListResult" in list_res:
                candidates = list_res["movieListResult"]["movieList"]
                
                # 후보군 순회
                for cand in candidates:
                    # 이름이 정확히 같아야 함
                    if cand["movieNm"].strip() != movie_nm.strip():
                        continue
                    
                    # 이미 조회한 코드면 패스
                    if cand["movieCd"] == original_cd:
                        continue

                    # 후보의 상세 정보 조회
                    cand_res = fetch_movie_detail(cand["movieCd"])
                    if cand_res and "movieInfoResult" in cand_res:
                        cand_info = cand_res["movieInfoResult"]["movieInfo"]
                        cand_audi = cand_info.get("audiAcc", "0")
                        
                        # 유의미한 관객수를 찾으면 즉시 채택!
                        if cand_audi and str(cand_audi) != "0":
                            final_audi = cand_audi
                            print(f" -> 찾음! ({cand_audi}명)", end="")
                            break
                            
        # 3. 결과 업데이트
        if str(final_audi) != "0":
            file_data = load_json(f_path)
            # 구조에 맞춰 업데이트
            if file_data.get("movieCd"): 
                file_data["audiAcc"] = final_audi
            elif "movieInfoResult" in file_data: 
                file_data["movieInfoResult"]["movieInfo"]["audiAcc"] = final_audi
            
            save_json(f_path, file_data)
            print(" -> ✅ 저장 완료")
            success_count += 1
        else:
            print(" -> ❌ 데이터 없음")
            
        time.sleep(0.1) # API 보호

    print(f"=== 작업 종료: {success_count}건 복구 완료 ===")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--pattern", type=str, default="", help="검색할 시작 글자 (예: 가, 나, A)")
    args = parser.parse_args()
    run_fill(args.pattern)
