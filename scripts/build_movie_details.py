# scripts/build_movie_details.py
#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import os
import json
import time
import argparse
from pathlib import Path

# kofic_api.py에서 키 로테이션 및 요청 모듈 가져오기
try:
    from kofic_api import fetch
except ImportError:
    print("[Error] kofic_api.py 모듈을 찾을 수 없습니다.")
    exit(1)

ROOT = Path(__file__).resolve().parents[1]
YEARS_DIR = ROOT / "docs" / "data" / "years"
MOVIES_DIR = ROOT / "docs" / "data" / "movies"
DETAIL_URL = "http://www.kobis.or.kr/kobisopenapi/webservice/rest/movie/searchMovieInfo.json"

def load_json(p: Path):
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except:
        return None

def save_json(p: Path, data: dict):
    p.parent.mkdir(parents=True, exist_ok=True)
    with open(p, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--year-start", type=int, required=True, help="시작 연도")
    parser.add_argument("--year-end", type=int, required=True, help="종료 연도")
    parser.add_argument("--max", type=int, default=999999, help="최대 수집 개수")
    parser.add_argument("--audiacc", choices=['off', 'missing', 'all'], default='missing', help="관객수 복구 모드")
    args = parser.parse_args()

    count = 0
    print(f"🎬 영화 상세 정보 수집 시작 ({args.year_start} ~ {args.year_end})")

    for year in range(args.year_start, args.year_end + 1):
        year_file = YEARS_DIR / f"year-{year}.json"
        
        if not year_file.exists():
            print(f"[Warning] {year_file.name} 파일이 없습니다. build_year_cache.py를 먼저 실행했는지 확인하세요.")
            continue

        year_data = load_json(year_file)
        movie_list = year_data.get("movieList", []) if year_data else []

        for movie in movie_list:
            if count >= args.max:
                print(f"[Stop] 설정된 최대 수집 개수({args.max})에 도달했습니다.")
                return

            movie_cd = movie.get("movieCd")
            movie_nm = movie.get("movieNm")
            if not movie_cd: continue

            target_path = MOVIES_DIR / str(year) / f"{movie_cd}.json"

            # 기존 파일 덮어쓰기 여부 결정 (audiacc 모드에 따라 다름)
            needs_fetch = True
            if target_path.exists():
                if args.audiacc == 'off':
                    needs_fetch = False
                elif args.audiacc == 'missing':
                    existing_data = load_json(target_path)
                    info = existing_data if existing_data.get("movieCd") else ((existing_data.get("movieInfoResult") or {}).get("movieInfo") or {})
                    audi = info.get("audiAcc")
                    # 이미 관객수가 채워져 있다면 굳이 API를 다시 호출하지 않음
                    if audi and str(audi).strip() not in ["", "0", "None"]:
                        needs_fetch = False
                # 'all'인 경우는 무조건 다시 덮어씀

            if not needs_fetch:
                continue

            print(f"[{year}] 상세 정보 조회 중: {movie_nm} ({movie_cd})")
            
            try:
                # kofic_api의 fetch 함수를 이용해 API 자동 로테이션 적용
                res = fetch(DETAIL_URL, {"movieCd": movie_cd})
                if res and "movieInfoResult" in res:
                    save_json(target_path, res)
                    count += 1
                    time.sleep(0.1) # KOFIC 서버 과부하 방지
                else:
                    print(f" -> ⚠️ API 응답에 영화 상세 정보가 없습니다.")
            except Exception as e:
                print(f" -> ❌ 에러 발생: {e}")

    print(f"\n✅ 작업 완료: 총 {count}개 영화의 상세 정보를 새롭게 수집/업데이트 했습니다.")

if __name__ == "__main__":
    main()
