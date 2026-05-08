import os
import json
import argparse
from datetime import datetime, timedelta
from pathlib import Path
import requests
import time

HERE = Path(__file__).resolve()
ROOT = HERE.parents[1] if HERE.parents[1].name == "MyMovieProject" else HERE.parents[2]
SEARCH_INDEX_PATH = ROOT / "docs" / "data" / "search_index.json"
MOVIES_DIR = ROOT / "docs" / "data" / "movies"

# 🌟 GitHub Actions에서 사용할 환경변수 KOFIC 키 로드
API_KEYS = []
for key_name in ["KOFIC_API_KEY"] + [f"KOFIC_API_KEY_{i}" for i in range(2, 11)]:
    val = os.environ.get(key_name)
    if val and val.strip():
        API_KEYS.append(val.strip())

CURRENT_KEY_INDEX = 0

def get_kofic_daily(target_date):
    """특정 날짜의 박스오피스 Top 10을 가져옴 (키 로테이션 적용)"""
    global CURRENT_KEY_INDEX
    if not API_KEYS:
        print("❌ KOFIC_API_KEY가 없습니다.")
        return []

    while CURRENT_KEY_INDEX < len(API_KEYS):
        key = API_KEYS[CURRENT_KEY_INDEX]
        url = f"http://www.kobis.or.kr/kobisopenapi/webservice/rest/boxoffice/searchDailyBoxOfficeList.json?key={key}&targetDt={target_date}"
        
        try:
            resp = requests.get(url, timeout=10)
            if resp.status_code == 200:
                data = resp.json()
                if "boxOfficeResult" in data:
                    return data["boxOfficeResult"].get("dailyBoxOfficeList", [])
                elif "faultInfo" in data: # 키 할당량 초과 시
                    print(f"⚠️ API Key {CURRENT_KEY_INDEX + 1} 한도 초과! 다음 키로 교체합니다.")
                    CURRENT_KEY_INDEX += 1
                    continue
            else:
                return []
        except:
            return []
            
    print("🚨 모든 KOFIC API 키가 소진되었습니다.")
    return []

def process_actor(actor_name):
    """특정 배우의 필모그래피를 검색하여 개봉 1주차(7일) 성적을 누적 계산"""
    print(f"\n🎬 배우 '{actor_name}' 타겟팅 수집 시작...")
    
    # 1. 배우의 영화 목록 찾기
    if not SEARCH_INDEX_PATH.exists():
        print("❌ search_index.json 파일이 없습니다.")
        return
        
    with open(SEARCH_INDEX_PATH, 'r', encoding='utf-8') as f:
        all_movies = json.load(f)
        
    target_movies = []
    for m in all_movies:
        actors = [a.get('name', '') for a in m.get('actors', [])]
        if actor_name in actors and m.get('openDt'):
            target_movies.append(m)
            
    if not target_movies:
        print(f"❌ '{actor_name}'의 개봉일이 있는 영화를 찾을 수 없습니다.")
        return
        
    print(f"🔍 총 {len(target_movies)}편의 영화 발견. 1주차 성적 수집 돌입!")
    
    for m in target_movies:
        movie_cd = m['movieCd']
        open_dt_str = m['openDt'].replace('-', '')
        
        if len(open_dt_str) != 8:
            continue
            
        try:
            open_date = datetime.strptime(open_dt_str, "%Y%m%d")
        except:
            continue
            
        # 2. 개봉일부터 7일간 일일 박스오피스 조회
        week_audi_total = 0
        found_in_top10 = False
        
        print(f"  -> 영화 '{m['movieNm']}' ({open_dt_str}) 수집 중...", end="")
        
        for i in range(7): # 1주차 (7일)
            target_date = (open_date + timedelta(days=i)).strftime("%Y%m%d")
            daily_list = get_kofic_daily(target_date)
            time.sleep(0.3) # API 서버 과부하 방지
            
            for item in daily_list:
                if item['movieCd'] == movie_cd:
                    week_audi_total += int(item.get('audiCnt', 0))
                    found_in_top10 = True
                    break
        
        # 3. 수집된 결과를 해당 영화 JSON 파일에 업데이트 (audiWeek1)
        if found_in_top10:
            print(f" ✅ 1주차: {week_audi_total:,}명")
            year_folder = open_dt_str[:4]
            movie_json_path = MOVIES_DIR / year_folder / f"{movie_cd}.json"
            
            if movie_json_path.exists():
                with open(movie_json_path, 'r', encoding='utf-8') as f:
                    movie_data = json.load(f)
                
                movie_data['audiWeek1'] = week_audi_total
                
                with open(movie_json_path, 'w', encoding='utf-8') as f:
                    json.dump(movie_data, f, ensure_ascii=False, indent=2)
        else:
            print(f" ⚠️ 데이터 없음 (해당 주차 Top 10 미진입)")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--actor", type=str, help="수집할 배우 이름 (예: 조우진)")
    args = parser.parse_args()
    
    if args.actor:
        process_actor(args.actor)
    else:
        print("💡 사용법: python scripts/fetch_weekly_audi.py --actor 황정민")
