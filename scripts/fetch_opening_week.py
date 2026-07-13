import os
import json
import requests
from datetime import datetime, timedelta

# KOFIC API 키 6, 7번 교차 사용
API_KEYS = [os.environ.get('KOFIC_API_KEY_6'), os.environ.get('KOFIC_API_KEY_7')]
SEARCH_INDEX_PATH = 'docs/data/search_index.json'
OUTPUT_PATH = 'docs/data/opening_week.json'

def fetch_opening_week():
    if not os.path.exists(SEARCH_INDEX_PATH):
        return

    with open(SEARCH_INDEX_PATH, 'r', encoding='utf-8') as f:
        movies = json.load(f)

    # 기존 데이터 로드 (API 중복 호출 방지)
    opening_data = {}
    if os.path.exists(OUTPUT_PATH):
        with open(OUTPUT_PATH, 'r', encoding='utf-8') as f:
            opening_data = json.load(f)

    key_idx = 0
    updates = 0

    for m in movies:
        movie_cd = m.get('movieCd')
        open_dt = str(m.get('openDt', '')).replace('-', '')
        
        # 2003년 이전이거나 이미 수집된 영화는 패스
        if not open_dt or len(open_dt) < 8 or int(open_dt[:4]) < 2003 or movie_cd in opening_data:
            continue

        try:
            start_date = datetime.strptime(open_dt, '%Y%m%d')
            target_date = (start_date + timedelta(days=6)).strftime('%Y%m%d') # 개봉 7일차(첫 주)
            
            url = f"http://www.kobis.or.kr/kobisopenapi/webservice/rest/boxoffice/searchDailyBoxOfficeList.json?key={API_KEYS[key_idx]}&targetDt={target_date}"
            res = requests.get(url, timeout=10).json()
            
            # API 제한 도달 시 키 교체
            if 'faultInfo' in res:
                key_idx += 1
                if key_idx >= len(API_KEYS): break
                continue

            daily_list = res.get('boxOfficeResult', {}).get('dailyBoxOfficeList', [])
            target_movie = next((item for item in daily_list if item['movieCd'] == movie_cd), None)
            
            if target_movie:
                # audiAcc는 해당 날짜(개봉 7일차)까지의 누적 관객수 = 개봉 첫 주 관객수
                opening_data[movie_cd] = int(target_movie.get('audiAcc', 0))
            else:
                opening_data[movie_cd] = 0 # 7일차에 박스오피스 아웃된 경우 0 또는 미상 처리
                
            updates += 1
            if updates >= 40: break # 하루 API 할당량 조절 (안전치)

        except Exception as e:
            continue

    with open(OUTPUT_PATH, 'w', encoding='utf-8') as f:
        json.dump(opening_data, f, ensure_ascii=False)

if __name__ == "__main__":
    fetch_opening_week()
