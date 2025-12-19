# scripts/fill_missing_audi_gh.py
import os
import json
import glob
import time
import requests
import sys
from pathlib import Path

# 인코딩 설정
sys.stdout.reconfigure(encoding='utf-8')

HERE = Path(__file__).resolve()
ROOT = HERE.parents[1] if HERE.parents[1].name == "MyMovieProject" else HERE.parents[2]
MOVIE_DIR = ROOT / "docs" / "data" / "movies"
DETAIL_URL = "http://www.kobis.or.kr/kobisopenapi/webservice/rest/movie/searchMovieInfo.json"

# GitHub Secrets에서 키 가져오기
API_KEYS = []
for i in range(1, 10):
    k = os.environ.get(f"KOFIC_API_KEY_{i}" if i > 1 else "KOFIC_API_KEY")
    if k: API_KEYS.append(k)

CURRENT_KEY_INDEX = 0

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

def fetch_movie_detail(movie_cd):
    for _ in range(len(API_KEYS) + 1): # 키 개수만큼 재시도
        api_key = get_next_key()
        if not api_key: return None
        try:
            params = { "key": api_key, "movieCd": movie_cd }
            r = requests.get(DETAIL_URL, params=params, timeout=10) # 타임아웃 10초
            data = r.json()
            if "faultInfo" in data: continue # 키 에러시 다음 키
            return data
        except:
            time.sleep(1)
    return None

def run_fill():
    print("[Step 1] 관객수 누락된 영화 스캔 중...")
    files = glob.glob(str(MOVIE_DIR / "**" / "*.json"), recursive=True)
    target_files = []
    
    for p in files:
        data = load_json(Path(p))
        if not data: continue
        info = data if data.get("movieCd") else ((data.get("movieInfoResult") or {}).get("movieInfo") or {})
        
        # 관객수가 없거나 0인 경우 타겟
        audi = info.get("audiAcc")
        if not audi or str(audi).strip() in ["0", "", "None"]:
            target_files.append((Path(p), info.get("movieCd")))

    print(f" -> 총 {len(target_files)}개의 누락 영화 발견. 복구 시작...")
    
    success_count = 0
    # API 부하 고려하여 한번에 최대 2000개까지만 처리 (워크플로우 타임아웃 방지)
    # 남은건 다음에 또 돌리면 됨
    LIMIT = 3000 
    
    for idx, (f_path, movie_cd) in enumerate(target_files[:LIMIT]):
        if not movie_cd: continue
        
        # 진행상황 로깅 (50개마다)
        if idx % 50 == 0:
            print(f" ... {idx}/{len(target_files)} 진행 중")

        res = fetch_movie_detail(movie_cd)
        if res and "movieInfoResult" in res:
            detail_info = res["movieInfoResult"]["movieInfo"]
            new_audi = detail_info.get("audiAcc")
            
            # 0명이 아닌 유의미한 데이터가 있다면 업데이트
            if new_audi and str(new_audi) != "0":
                file_data = load_json(f_path)
                if file_data.get("movieCd"): file_data["audiAcc"] = new_audi
                elif "movieInfoResult" in file_data: file_data["movieInfoResult"]["movieInfo"]["audiAcc"] = new_audi
                
                save_json(f_path, file_data)
                print(f" [복구성공] {detail_info.get('movieNm')} -> {new_audi}명")
                success_count += 1
        
        time.sleep(0.05) # 서버 보호용 미세 딜레이

    print(f"=== 작업 종료: 총 {success_count}건 복구 완료 ===")

if __name__ == "__main__":
    run_fill()
