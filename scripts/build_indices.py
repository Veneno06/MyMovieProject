#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
build_indices.py
- docs/data/movies/YYYY/*.json 을 읽어 검색 인덱스 생성 (docs/data/search/{movies,people}.json)
- 선택적으로 audiAcc(누적 관객수)를 선계산:
  * off   : 계산 안 함 (API 0회, 가장 빠름)
  * recent: 최근 N일 내 개봉작만 주간박스오피스 API로 최대 6~8회 탐색해 대략의 누적을 추정
  * all   : 모든 타이틀에 대해 동일 계산 (권장 X)
- 과도한 API 호출을 막기 위해 --audiacc-budget 으로 한 실행에서 허용할 최대 호출 수를 제한
"""

import os, sys, json, re, time, math, glob
from datetime import datetime, timedelta
from collections import defaultdict
import argparse
import requests

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DOCS = os.path.join(ROOT, "docs")
DATA = os.path.join(DOCS, "data")
MOVIES_DIR = os.path.join(DATA, "movies")
SEARCH_DIR = os.path.join(DATA, "search")
PEOPLE_DIR = os.path.join(DATA, "people")

KOFIC_KEY = os.environ.get("KOFIC_API_KEY", "").strip()

# --------- 공통 유틸 ---------
def ymd_to_date(ymd: str):
    if not ymd:
        return None
    s = re.sub(r"[^0-9]", "", str(ymd))
    if len(s) != 8:
        return None
    try:
        return datetime(int(s[:4]), int(s[4:6]), int(s[6:8]))
    except:
        return None

def ensure_dir(p):
    if not os.path.isdir(p):
        os.makedirs(p, exist_ok=True)

def read_json(path):
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except:
        return None

def write_json(path, obj):
    ensure_dir(os.path.dirname(path))
    with open(path, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=2)

def list_movie_detail_files():
    years = []
    for d in sorted(glob.glob(os.path.join(MOVIES_DIR, "*"))):
        if os.path.isdir(d) and os.path.basename(d).isdigit():
            years.append(os.path.basename(d))
    files = []
    for y in years:
        files += sorted(glob.glob(os.path.join(MOVIES_DIR, y, "*.json")))
    return files

# --------- audiAcc 계산(선택) ---------
class AudiAccEstimator:
    """
    최근작만 대략 누적관객을 얻기 위해 KOFIC 주간 박스오피스 API를
    (openDt + 3일)부터 최대 8주치 조회하며 같은 movieCd가 보이면 audiAcc를 기록.
    - 호출 사이에 작은 sleep으로 속도 제한
    - HTTP 에러/쿼터 핸들링(간단 백오프)
    - 예산(budget) 소진 시 즉시 종료
    """
    BASE = "https://www.kobis.or.kr/kobisopenapi/webservice/rest/boxoffice/searchWeeklyBoxOfficeList.json"

    def __init__(self, mode: str, days: int, budget: int, rate_sleep_ms: int):
        self.mode = mode   # 'off'|'recent'|'all'
        self.days = int(days)
        self.budget = int(budget)
        self.rate_sleep = max(0, int(rate_sleep_ms)) / 1000.0
        self.session = requests.Session()

    def should_try(self, open_dt_str: str) -> bool:
        if self.mode == "off":
            return False
        if self.mode == "all":
            return True
        # recent
        d = ymd_to_date(open_dt_str)
        if not d:
            return False
        return (datetime.utcnow() - d).days <= self.days

    def fetch_week(self, target_dt: str):
        if self.budget <= 0:
            return None, "budget_exhausted"
        params = {
            "key": KOFIC_KEY,
            "targetDt": target_dt,
            "weekGb": "0"
        }
        url = self.BASE
        tries = 0
        while True:
            tries += 1
            if self.rate_sleep > 0:
                time.sleep(self.rate_sleep)
            try:
                self.budget -= 1
                resp = self.session.get(url, params=params, timeout=15)
                if resp.status_code == 200:
                    return resp.json(), None
                # 429/5xx → 간단 백오프
                if resp.status_code >= 500 or resp.status_code == 429:
                    time.sleep(min(30, 2 ** min(tries, 6)))
                    continue
                return None, f"http_{resp.status_code}"
            except requests.RequestException as e:
                # 네트워크 오류 → 백오프 재시도
                time.sleep(min(30, 2 ** min(tries, 6)))
                if tries >= 5:
                    return None, "network_error"

    def estimate(self, movie_cd: str, open_dt: str) -> int | None:
        if not self.should_try(open_dt) or not KOFIC_KEY:
            return None
        d = ymd_to_date(open_dt)
        if not d:
            return None
        # 기준일: 개봉일 + 3일 (ECMA 주간 커트오프 보정)
        base = d + timedelta(days=3)
        max_weeks = 8
        best = None
        for w in range(max_weeks):
            target = base + timedelta(days=7 * w)
            js, err = self.fetch_week(target.strftime("%Y%m%d"))
            if err == "budget_exhausted":
                # 전체 실행 시간을 막기 위해 즉시 포기
                return None
            if not js:
                continue
            lst = (js.get("boxOfficeResult") or {}).get("weeklyBoxOfficeList") or []
            for row in lst:
                if row.get("movieCd") == movie_cd:
                    acc_str = row.get("audiAcc")
                    try:
                        acc = int(str(acc_str).replace(",", ""))
                        best = acc if best is None else max(best, acc)
                    except:
                        pass
            # 누적이 한번이라도 잡히면 다음 1~2주만 더 확인하고 종료(짧게)
            if best is not None and w >= 2:
                break
        return best

# --------- 인덱스 빌드 ---------
def build_indices(audiacc_mode="off", audiacc_days=540, audiacc_budget=800, rate_sleep_ms=250):
    ensure_dir(SEARCH_DIR)
    ensure_dir(PEOPLE_DIR)

    estimator = AudiAccEstimator(
        mode=audiacc_mode,
        days=int(audiacc_days),
        budget=int(audiacc_budget),
        rate_sleep_ms=int(rate_sleep_ms),
    )

    movies_out = []
    people_map = defaultdict(lambda: {"peopleCd":"", "peopleNm":"", "repRoleNm":"", "films":[]})

    detail_files = list_movie_detail_files()
    now = int(time.time())

    print(f"[index] scanning {len(detail_files)} detail files ...")
    for i, path in enumerate(detail_files, 1):
        js = read_json(path)
        if not js:
            continue
        # detail.json 구조 가정
        movie_cd = js.get("movieCd") or ""
        movie_nm = js.get("movieNm") or ""
        open_dt = js.get("openDt") or ""
        prdt_year = js.get("prdtYear") or ""
        rep_nation = js.get("repNationNm") or ""
        grade = js.get("audits", [{}])[0].get("watchGradeNm", "")
        # 장르
        genres = [g.get("genreNm") for g in (js.get("genres") or []) if g.get("genreNm")]

        # audiAcc (선택)
        audi_acc = None
        if estimator.mode != "off":
            audi_acc = estimator.estimate(movie_cd, open_dt)

        movies_out.append({
            "movieCd": movie_cd,
            "movieNm": movie_nm,
            "openDt": open_dt,
            "prdtYear": prdt_year,
            "repNation": rep_nation,
            "grade": grade or "",
            "genres": genres,
            "audiAcc": audi_acc if isinstance(audi_acc, int) else None,
        })

        # 인물 인덱스
        for a in js.get("actors") or []:
            nm = a.get("peopleNm") or ""
            if not nm:
                continue
            key = a.get("peopleCd") or ("NM:" + nm)  # peopleCd 없으면 이름키
            entry = people_map[key]
            if not entry["peopleNm"]:
                entry["peopleNm"] = nm
                entry["peopleCd"] = a.get("peopleCd","")
                entry["repRoleNm"] = "배우"
            # 대표작 리스트에 추가(간단히)
            if len(entry["films"]) < 6:
                entry["films"].append({
                    "movieCd": movie_cd,
                    "movieNm": movie_nm,
                    "openDt": open_dt,
                    "part": a.get("cast","") or a.get("castNm","") or ""
                })

        if i % 500 == 0:
            print(f"[index] {i}/{len(detail_files)} ... budget_left={estimator.budget}")

        # 예산이 다 떨어졌다면 이후는 audiAcc 없이 진행
        if estimator.budget <= 0 and estimator.mode != "off":
            print("[index] audiAcc budget exhausted → rest will be written with null")
            estimator.mode = "off"

    # 정렬(개봉일, 없으면 뒤로)
    def sort_key(m):
        d = ymd_to_date(m.get("openDt",""))
        return (datetime.min if not d else d)
    movies_out.sort(key=sort_key)

    movies_json = {
        "generatedAt": now,
        "count": len(movies_out),
        "movies": movies_out,
    }
    people_list = list(people_map.values())
    people_json = {
        "generatedAt": now,
        "count": len(people_list),
        "people": people_list,
    }

    write_json(os.path.join(SEARCH_DIR, "movies.json"), movies_json)
    write_json(os.path.join(SEARCH_DIR, "people.json"), people_json)
    print(f"[index] done → movies: {len(movies_out)} / people: {len(people_list)}")

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--audiacc-mode", default="off", choices=["off","recent","all"])
    ap.add_argument("--audiacc-days", default=540, type=int)
    ap.add_argument("--audiacc-budget", default=800, type=int,
                    help="이번 실행에서 audiAcc 계산에 허용할 최대 API 호출 수(초과 시 중단)")
    ap.add_argument("--rate-sleep-ms", default=250, type=int,
                    help="KOFIC 호출 간 슬립(ms) — 과한 호출로 인한 슬로틀링을 완화")
    args = ap.parse_args()

    build_indices(
        audiacc_mode=args.audiacc_mode,
        audiacc_days=args.audiacc_days,
        audiacc_budget=args.audiacc_budget,
        rate_sleep_ms=args.rate_sleep_ms,
    )

if __name__ == "__main__":
    main()
