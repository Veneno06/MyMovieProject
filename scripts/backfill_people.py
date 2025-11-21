import os
import json
import glob
import time
from pathlib import Path
# [중요] 위에서 만든 모듈 임포트
try:
    import kofic_api
except ImportError:
    # 로컬 실행 시 경로 문제 해결용
    import sys
    sys.path.append(os.path.dirname(os.path.abspath(__file__)))
    import kofic_api

# 경로 설정
ROOT = Path(__file__).resolve().parents[1]
MOVIES_DIR = ROOT / "docs" / "data" / "movies"
SEARCH_MOVIE_INFO_URL = "https://www.kobis.or.kr/kobisopenapi/webservice/rest/movie/searchMovieInfo.json"

def load_json(p):
    try:
        with open(p, "r", encoding="utf-8") as f: return json.load(f)
    except: return None

def save_json(p, data):
    with open(p, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

def is_missing_code(person_list):
    """배우/감독 리스트 중 코드가 없는 사람이 있는지 확인"""
    if not person_list: return False
    for p in person_list:
        # 이름은 있는데 코드가 비어있으면 True
        if p.get("peopleNm") and not p.get("peopleCd"):
            return True
    return False

def run_backfill(limit=5000):
    print(">>> 영화인 코드 보강(Backfill) 시작...")
    files = sorted(glob.glob(str(MOVIES_DIR / "**" / "*.json"), recursive=True))
    print(f"  - 스캔 대상 파일: {len(files)}개")
    
    count = 0
    updated = 0
    
    for p in files:
        data = load_json(p)
        if not data: continue
        
        # 데이터 구조 파악 (flat or raw)
        movie_info = None
        if "movieInfoResult" in data:
            movie_info = data["movieInfoResult"].get("movieInfo")
        elif "movieCd" in data:
            movie_info = data
            
        if not movie_info: continue
        
        # 보강 필요 여부 확인
        actors = movie_info.get("actors", [])
        directors = movie_info.get("directors", [])
        
        if not (is_missing_code(actors) or is_missing_code(directors)):
            continue # 이미 코드가 다 있으면 건너뜀 (기존 데이터 보존)

        # API 호출
        movie_cd = movie_info.get("movieCd")
        print(f"  [{count+1}] 보강 필요: {movie_cd} ({movie_info.get('movieNm')})")
        
        try:
            res = kofic_api.fetch(SEARCH_MOVIE_INFO_URL, {"movieCd": movie_cd})
            new_info = res.get("movieInfoResult", {}).get("movieInfo")
            
            if new_info:
                # 기존 파일 형식 유지하며 덮어쓰기
                if "movieInfoResult" in data:
                    data["movieInfoResult"]["movieInfo"] = new_info
                else:
                    data = new_info
                
                save_json(p, data)
                updated += 1
                print(f"    -> 업데이트 완료! (배우: {len(new_info.get('actors', []))}명)")
            else:
                print("    -> API 데이터 없음")

            count += 1
            if count >= limit:
                print(">>> 일일 작업 한도 도달. 종료합니다.")
                break
            
            time.sleep(0.2) # API 부하 방지
            
        except Exception as e:
            print(f"    -> 오류 발생: {e}")

    print(f">>> 작업 종료. 총 {updated}개 파일 업데이트됨.")

if __name__ == "__main__":
    # 명령어 인자로 한도 설정 가능 (기본 2000회)
    import sys
    limit = int(sys.argv[1]) if len(sys.argv) > 1 else 2000
    run_backfill(limit)
