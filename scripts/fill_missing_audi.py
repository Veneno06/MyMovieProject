# scripts/fill_missing_audi.py
import os
import json
import glob
import time
import requests
import sys
from pathlib import Path

# 인코딩 설정
sys.stdout.reconfigure(encoding='utf-8')

CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
if CURRENT_DIR not in sys.path:
    sys.path.append(CURRENT_DIR)

try:
    from kofic_api import API_KEYS
except ImportError:
    print("[Error] kofic_api.py를 찾을 수 없습니다.")
    exit(1)

HERE = Path(__file__).resolve()
ROOT = HERE.parents[1] if HERE.parents[1].name == "MyMovieProject" else HERE.parents[2]
MOVIE_DIR = ROOT / "docs" / "data" / "movies"
DETAIL_URL = "http://www.kobis.or.kr/kobisopenapi/webservice/rest/movie/searchMovieInfo.json"

CURRENT_KEY_INDEX = 0

def get_next_key():
    global CURRENT_KEY_INDEX
    if not API_KEYS: return None
    key = API_KEYS[CURRENT_KEY_INDEX]
    CURRENT_KEY_INDEX = (CURRENT_KEY_INDEX + 1) % len(API_KEYS)
    return key

def load_json(p: Path):
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except: return None

def save_json(p: Path, data: dict):
    with open(p, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

def fetch_movie_detail(movie_cd):
    # 키 로테이션하며 시도
    for _ in range(len(API_KEYS) + 1):
        api_key = get_next_key()
        try:
            params = { "key": api_key, "movieCd": movie_cd }
            r = requests.get(DETAIL_URL, params=params, timeout=5)
            data = r.json()
            
            # 에러 체크
            if "faultInfo" in data:
                print(f"   -> ⚠️ Key Error ({api_key[:5]}...): Switching...")
                continue
                
            return data
        except Exception as e:
            print(f"   -> ⚠️ Connection Error. Retrying...")
            time.sleep(1)
            
    return None

def run_fill():
    print("[Step 1] 관객수 누락 영화 탐색 중...")
    files = glob.glob(str(MOVIE_DIR / "**" / "*.json"), recursive=True)
    
    target_files = []
    
    for p in files:
        data = load_json(Path(p))
        if not data: continue
        
        info = data if data.get("movieCd") else ((data.get("movieInfoResult") or {}).get("movieInfo") or {})
        
        # 관객수가 없거나, 0이거나, 비어있는 경우
        audi = info.get("audiAcc")
        if not audi or str(audi).strip() == "0" or str(audi).strip() == "":
            target_files.append((Path(p), info.get("movieCd")))
            
    print(f" -> 총 {len(target_files)}개의 영화가 관객수 데이터가 없습니다. API 조회를 시작합니다.")
    
    updated_count = 0
    
    for idx, (f_path, movie_cd) in enumerate(target_files):
        if not movie_cd: continue
        
        print(f"[{idx+1}/{len(target_files)}] 조회 중: {movie_cd} ... ", end="", flush=True)
        
        res = fetch_movie_detail(movie_cd)
        if res and "movieInfoResult" in res:
            detail_info = res["movieInfoResult"]["movieInfo"]
            new_audi = detail_info.get("audiAcc")
            
            # 유효한 관객수가 있으면 업데이트
            if new_audi:
                # 파일 다시 로드 (안전하게)
                file_data = load_json(f_path)
                
                # 구조에 맞춰 업데이트
                if file_data.get("movieCd"): 
                    file_data["audiAcc"] = new_audi
                elif "movieInfoResult" in file_data:
                    file_data["movieInfoResult"]["movieInfo"]["audiAcc"] = new_audi
                
                save_json(f_path, file_data)
                print(f"✅ 성공! ({new_audi}명)")
                updated_count += 1
            else:
                print("데이터 없음 (0명)")
        else:
            print("API 실패")
            
        # API 과부하 방지용 미세 딜레이 (키가 많아서 괜찮지만 안전하게)
        time.sleep(0.1)

    print(f"\n=== 최종 완료: 총 {updated_count}개의 영화 관객수 복구 완료 ===")

if __name__ == "__main__":
    run_fill()
