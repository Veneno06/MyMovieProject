#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import json
from collections import Counter

# 이 스크립트 파일의 위치를 기준으로 경로 설정
# scripts 폴더 안에 있으므로, 상위 폴더(ROOT)로 이동해서 경로를 구성
try:
    ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    SEARCH_DIR = os.path.join(ROOT, "docs", "data", "search")
    MOVIES_JSON_PATH = os.path.join(SEARCH_DIR, "movies.json")
except NameError:
    # 대화형 환경 등 __file__ 변수가 없는 경우를 위한 예외 처리
    ROOT = os.getcwd() # 현재 작업 디렉토리를 기준으로 설정
    SEARCH_DIR = os.path.join(ROOT, "docs", "data", "search")
    MOVIES_JSON_PATH = os.path.join(SEARCH_DIR, "movies.json")


def analyze_movie_data():
    """
    docs/data/search/movies.json 파일을 읽고 분석하여 리포트를 출력합니다.
    """
    print(f"분석 대상 파일: {MOVIES_JSON_PATH}")

    # 1. JSON 파일 로드
    try:
        with open(MOVIES_JSON_PATH, "r", encoding="utf-8") as f:
            data = json.load(f)
        movies = data.get("movies", [])
    except FileNotFoundError:
        print("\n[오류] 파일을 찾을 수 없습니다. 경로를 확인해주세요.")
        return
    except json.JSONDecodeError:
        print("\n[오류] JSON 파일 형식이 잘못되었습니다.")
        return
    except Exception as e:
        print(f"\n[오류] 파일을 읽는 중 문제가 발생했습니다: {e}")
        return

    if not movies:
        print("\n[정보] 분석할 영화 데이터가 없습니다.")
        return

    # 2. 데이터 분석
    total_movies = len(movies)
    movies_with_date = []
    missing_date_movies = []

    for movie in movies:
        # 개봉일(openDt) 값이 비어있지 않은 경우만 유효 데이터로 간주
        if movie.get("openDt"):
            movies_with_date.append(movie)
        else:
            missing_date_movies.append(movie)

    # 데이터 범위 (가장 이른 날짜, 가장 늦은 날짜)
    dates = [m['openDt'] for m in movies_with_date]
    earliest_date = min(dates) if dates else "N/A"
    latest_date = max(dates) if dates else "N/A"

    # 연도별 영화 개수
    years = [d.split('-')[0] for d in dates]
    year_counts = Counter(years)

    # 3. 리포트 출력
    print("\n" + "="*40)
    print(" K-Movie Archive 데이터 분석 리포트")
    print("="*40)
    print(f"\n- 총 영화 수: {total_movies}편")
    print(f"- 개봉일 데이터 범위: {earliest_date} ~ {latest_date}")
    print(f"- 개봉일 정보가 없는 영화: {len(missing_date_movies)}편")

    if missing_date_movies:
        # 개봉일 없는 영화가 너무 많을 수 있으므로 최대 5개만 예시로 출력
        print("  (예시: " + ", ".join([f"'{m['movieNm']}'" for m in missing_date_movies[:5]]) + ")")

    print("\n- 연도별 영화 개봉 수:")
    # 연도순으로 정렬하여 출력
    for year, count in sorted(year_counts.items()):
        print(f"  {year}년: {count}편")

    print("\n" + "="*40)
    print("분석이 완료되었습니다.")


if __name__ == "__main__":
    analyze_movie_data()
