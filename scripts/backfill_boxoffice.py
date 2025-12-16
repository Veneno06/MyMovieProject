# scripts/backfill_boxoffice.py
import os
import json
import time
import glob
import argparse
import sys
import requests
from datetime import datetime, timedelta
from urllib.parse import urlencode
from pathlib import Path

# 인코딩 설정
sys.stdout.reconfigure(encoding='utf-8')

CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
if CURRENT_DIR not in sys.path:
    sys.path.append(CURRENT_DIR)

try:
    from kofic_api import API_KEYS  # kofic_api.py에서 키 가져오기
except ImportError:
    print("[Error] kofic_api.py를 찾을 수 없습니다.")
    exit(1)

HERE = Path(__file__).resolve()
ROOT = HERE.parents[1] if HERE.parents[1].name == "MyMovieProject" else HERE.parents[2]
DETAIL_DIR = ROOT / "docs" / "data" / "movies"
BOXOFFICE_URL = "http://www.kobis.or.kr/kobisopenapi/webservice/rest/boxoffice/searchDailyBoxOfficeList.json"

CURRENT_KEY_INDEX = 0

def get_next_key_session():
    global CURRENT_KEY_INDEX
    if not API_KEYS: return None, None
    CURRENT_KEY_INDEX = (CURRENT_KEY_INDEX + 1) % len(API_KEYS)
    api_key = API_KEYS[CURRENT_KEY_INDEX]
    session = requests.Session()
    return session, api_key

def fetch_boxoffice_by_date(target_dt, session, api_key):
    global CURRENT_KEY_INDEX
    
    # 키 로테이션 로직
    max_retries = len(API_KEYS) + 1
    for _ in range(max_retries):
        try:
            params = {
                "key": api_key,
                "targetDt": target_dt,
                "itemPerPage": 10  # 10위까지만 봐도 충분 (대부분의 영화는 개봉 때 10위 안에 듬)
            }
            r = session.get(BOXOFFICE_URL, params=params, timeout=5)
            r.raise_for_status()
            data = r.json()
            
            # 에러 체크
            if "faultInfo" in data:
                print(f" -> ⚠️ Key Error ({api_key[:5]}...): Switching key...")
                session, api_key = get_next_key_session()
                continue
                
            return data, session, api_key
            
        except Exception as e:
            print(f" -> ⚠️ Connection Error: {e}. Retrying...")
            time.sleep(1)
            session, api_key = get_next_key_session()
            
    return None, session, api_key

def load_json(p: Path):
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except: return None

def save_json(p: Path, data: dict):
    with open(p, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

def run_backfill(start_date_str, days_count):
    # 1. 파일 경로 캐싱 (영화코드 -> 파일경로)
    print("[Step 1] 로컬 영화 데이터 매핑 중...")
    movie_path_map = {}
    files = glob.glob(str(DETAIL_DIR / "**" / "*.json"), recursive=True)
    for p in files:
        # 파일명 자체가 영화코드인 경우가 많음 (12345678.json)
        code = Path(p).stem 
        if code.isdigit():
            movie_path_map[code] = Path(p)
    print(f" -> 총 {len(movie_path_map)}개의 영화 파일 매핑 완료.")

    # 2. 날짜 순회 (과거로 이동)
    current_date = datetime.strptime(start_date_str, "%Y%m%d")
    session, api_key = get_next_key_session()
    
    print(f"[Step 2] API 박스오피스 조회 시작 ({start_date_str} 부터 {days_count}일간 과거로)")
    
    updated_movies = set()
    total_updates = 0
    
    for i in range(days_count):
        target_dt = current_date.strftime("%Y%m%d")
        
        # API 호출
        data, session, api_key = fetch_boxoffice_by_date(target_dt, session, api_key)
        
        if data and "boxOfficeResult" in data:
            daily_list = data["boxOfficeResult"].get("dailyBoxOfficeList", [])
            
            for item in daily_list:
                movie_cd = item.get("movieCd")
                audi_acc = item.get("audiAcc") # 누적 관객수
                
                # 내 프로젝트에 있는 영화인지 확인
                if movie_cd in movie_path_map and audi_acc:
                    f_path = movie_path_map[movie_cd]
                    
                    # 파일 로드 및 업데이트
                    f_data = load_json(f_path)
                    if not f_data: continue
                    
                    # 데이터 구조 파악
                    info_node = None
                    if f_data.get("movieCd"): info_node = f_data
                    elif "movieInfoResult" in f_data: info_node = f_data["movieInfoResult"]["movieInfo"]
                    
                    if info_node:
                        current_acc = int(info_node.get("audiAcc") or 0)
                        new_acc = int(audi_acc)
                        
                        # [중요] 더 큰 값으로만 업데이트 (과거 날짜를 조회하더라도 최신 누적값이 유지되도록)
                        if new_acc > current_acc:
                            info_node["audiAcc"] = new_acc
                            save_json(f_path, f_data)
                            if movie_cd not in updated_movies:
                                print(f" -> ✅ 업데이트: {item.get('movieNm')} ({new_acc:,}명) [{target_dt}]")
                                updated_movies.add(movie_cd)
                                total_updates += 1
        
        # 하루 전으로 이동
        current_date -= timedelta(days=1)
        
        # 진행 상황 출력 (10일마다)
        if i % 10 == 0:
            print(f" ... {target_dt} 완료 (누적 업데이트: {total_updates}건)")
            
    print(f"=== 최종 완료: 총 {total_updates}건의 관객수 업데이트 ===")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    # 기본값: 어제부터 시작해서 365일(1년)치 조회
    yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y%m%d")
    parser.add_argument("--start-date", type=str, default=yesterday)
    parser.add_argument("--days", type=int, default=365) 
    
    args = parser.parse_args()
    
    if not API_KEYS:
        print("[Error] API 키가 없습니다.")
        exit(1)
        
    run_backfill(args.start_date, args.days)
