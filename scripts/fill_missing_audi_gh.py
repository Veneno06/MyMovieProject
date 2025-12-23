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
DETAIL_URL = "http://www.kobis.or.kr/kobisopenapi/webservice/rest/movie/searchMovieInfo.json"

API_KEYS = []
for i in range(1, 10):
    k = os.environ.get(f"KOFIC_API_KEY_{i}" if i > 1 else "KOFIC_API_KEY")
    if k: API_KEYS.append(k)

CURRENT_KEY_INDEX = 0

# 한글 초성 리스트
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

# [핵심 로직] 문자의 초성(자음)을 추출하는 함수
def get_initial_sound(char):
    if not char: return ""
    
    # 1. 이미 자음만 입력된 경우 ('ㄱ')
    if char in CHOSUNG_LIST:
        return char
        
    # 2. 완성형 한글인 경우 ('가', '고' -> 'ㄱ')
    if '가' <= char <= '힣':
        char_code = ord(char) - 0xAC00
        chosung_index = char_code // 588
        return CHOSUNG_LIST[chosung_index]
        
    # 3. 영어/숫자/특수문자인 경우 그대로 반환 ('A' -> 'A')
    return char

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

def run_fill(pattern=None):
    # 패턴에서 대표 초성 추출 (예: "가" -> "ㄱ", "A" -> "A")
    target_initial = get_initial_sound(pattern[0]) if pattern else None
    
    print(f"[Step 1] 관객수 누락 영화 스캔 중... (입력: '{pattern}' -> 타겟 초성: '{target_initial or '전체'}')")
    
    files = glob.glob(str(MOVIE_DIR / "**" / "*.json"), recursive=True)
    target_files = []
    
    for p in files:
        data = load_json(Path(p))
        if not data: continue
        info = data if data.get("movieCd") else ((data.get("movieInfoResult") or {}).get("movieInfo") or {})
        
        movie_nm = info.get("movieNm", "")
        if not movie_nm: continue

        # [필터링 로직 변경] 초성이 같은지 비교
        if target_initial:
            # 영화 제목 첫 글자의 초성 추출
            movie_initial = get_initial_sound(movie_nm[0])
            
            # 다르면 건너뛰기 (대소문자 구분 없이 비교하기 위해 upper 사용)
            if movie_initial.upper() != target_initial.upper():
                continue

        audi = info.get("audiAcc")
        # 관객수가 없거나 0인 경우만 타겟
        if not audi or str(audi).strip() in ["0", "", "None"]:
            target_files.append((Path(p), info.get("movieCd")))

    print(f" -> '{target_initial or '전체'}'로 시작하는 누락 영화: {len(target_files)}개 발견.")
    
    success_count = 0
    # 타임아웃 방지를 위해 한 번에 최대 1000개만 처리
    for idx, (f_path, movie_cd) in enumerate(target_files[:1000]):
        if idx % 50 == 0: print(f" ... {idx}/{len(target_files)} 진행 중")
        
        res = fetch_movie_detail(movie_cd)
        if res and "movieInfoResult" in res:
            detail = res["movieInfoResult"]["movieInfo"]
            new_audi = detail.get("audiAcc")
            
            if new_audi and str(new_audi) != "0":
                file_data = load_json(f_path)
                if file_data.get("movieCd"): file_data["audiAcc"] = new_audi
                elif "movieInfoResult" in file_data: file_data["movieInfoResult"]["movieInfo"]["audiAcc"] = new_audi
                save_json(f_path, file_data)
                print(f" [복구] {detail.get('movieNm')} -> {new_audi}명")
                success_count += 1
        time.sleep(0.05)

    print(f"=== 작업 종료: {success_count}건 복구 완료 ===")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--pattern", type=str, default="", help="검색할 시작 글자 (예: 가, 나, A)")
    args = parser.parse_args()
    run_fill(args.pattern)
