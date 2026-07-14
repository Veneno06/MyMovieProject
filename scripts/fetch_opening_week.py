import os
import json
import requests
from datetime import datetime, timedelta
from pathlib import Path
  
# 🌟 환경변수에서 KOFIC 키 6번, 7번 로드
API_KEYS = [
    os.environ.get('KOFIC_API_KEY_6'),
    os.environ.get('KOFIC_API_KEY_7')
]
# 비어있는 키 필터링
API_KEYS = [k for k in API_KEYS if k and k.strip()]

# 파일 경로 동적 설정
HERE = Path(__file__).resolve()
ROOT = HERE.parents[1] if HERE.parents[1].name == "MyMovieProject" else HERE.parents[2]
SEARCH_INDEX_PATH = ROOT / "docs" / "data" / "search_index.json"
OUTPUT_PATH = ROOT / "docs" / "data" / "opening_week.json"

# 🌟 키 1개당 일일 한도의 3/5 (60%) = 1800번으로 제한
MAX_CALLS_PER_KEY = 1800  

def fetch_opening_week():
    if not API_KEYS:
        print("❌ KOFIC_API_KEY_6 또는 7이 설정되지 않았습니다.")
        return

    if not SEARCH_INDEX_PATH.exists():
        print("❌ search_index.json 파일을 찾을 수 없습니다.")
        return

    with open(SEARCH_INDEX_PATH, 'r', encoding='utf-8') as f:
        movies = json.load(f)

    # 기존 수집 데이터 불러오기 (중복 호출 방지)
    opening_data = {}
    if OUTPUT_PATH.exists():
        with open(OUTPUT_PATH, 'r', encoding='utf-8') as f:
            opening_data = json.load(f)

    key_idx = 0
    calls_for_current_key = 0
    updates = 0

    print("🚀 개봉 첫 주 관객수 수집을 시작합니다...")

    for m in movies:
        movie_cd = m.get('movieCd')
        open_dt = str(m.get('openDt', '')).replace('-', '')
        
        # 2003년 이전이거나 이미 수집된 영화는 패스
        if not open_dt or len(open_dt) < 8 or int(open_dt[:4]) < 2003 or movie_cd in opening_data:
            continue

        try:
            start_date = datetime.strptime(open_dt, '%Y%m%d')
            target_date = (start_date + timedelta(days=6)).strftime('%Y%m%d') # 개봉 7일차
            
            url = f"http://www.kobis.or.kr/kobisopenapi/webservice/rest/boxoffice/searchDailyBoxOfficeList.json?key={API_KEYS[key_idx]}&targetDt={target_date}"
            res = requests.get(url, timeout=10).json()
            calls_for_current_key += 1
            
            # 🌟 에러 발생 또는 60%(1800번) 도달 시 다음 키로 교체
            if 'faultInfo' in res or calls_for_current_key >= MAX_CALLS_PER_KEY:
                if 'faultInfo' in res:
                    print(f"⚠️ API Key {key_idx + 6} 서버 에러/초과. 다음 키로 교체합니다.")
                else:
                    print(f"🔄 API Key {key_idx + 6} 설정 한도(1800회) 도달. 다음 키로 교체합니다.")
                
                key_idx += 1
                calls_for_current_key = 0
                if key_idx >= len(API_KEYS): 
                    print("🚨 허용된 모든 API 키를 소진했습니다. 스크립트를 종료합니다.")
                    break
                if 'faultInfo' in res:
                    continue # 이번 영화는 다음 실행 시 재수집
            
            daily_list = res.get('boxOfficeResult', {}).get('dailyBoxOfficeList', [])
            target_movie = next((item for item in daily_list if item['movieCd'] == movie_cd), None)
            
            if target_movie:
                # audiAcc는 해당 날짜(개봉 7일차)까지의 누적 관객수
                opening_data[movie_cd] = int(target_movie.get('audiAcc', 0))
            else:
                opening_data[movie_cd] = 0 # 7일차 Top10 차트 아웃
                
            updates += 1
            if updates % 100 == 0:
                print(f"   ... {updates}건 수집 완료")

        except Exception as e:
            continue

    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(OUTPUT_PATH, 'w', encoding='utf-8') as f:
        json.dump(opening_data, f, ensure_ascii=False)
        
    print(f"✅ 작업 완료: 이번 실행으로 {updates}편의 첫 주 관객수를 추가 수집했습니다.")

if __name__ == "__main__":
    fetch_opening_week()
