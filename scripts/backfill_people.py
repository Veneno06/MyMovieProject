# scripts/backfill_people.py
# 목적: 기존 데이터에 영화인 코드가 없으면 API로 조회하여 채워 넣음
# 타겟: 'ㅎ'씨 배우(황정민) 집중 공략 + 한국 영화
from __future__ import annotations
import os
import json
import time
import glob
import argparse
import sys
from pathlib import Path
from urllib.parse import urlencode
import requests

# 모듈 경로 추가
CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
if CURRENT_DIR not in sys.path:
    sys.path.append(CURRENT_DIR)

try:
    from kofic_api import get_session, API_KEYS
except ImportError:
    print(f"[오류] 'kofic_api' 모듈을 찾을 수 없습니다.")
    exit(1)

# 경로 설정
HERE = Path(__file__).resolve()
ROOT = HERE.parents[1] if HERE.parents[1].name == "MyMovieProject" else HERE.parents[2]
DETAIL_DIR = ROOT / "docs" / "data" / "movies"
MOVIE_INFO_URL = "https://www.kobis.or.kr/kobisopenapi/webservice/rest/movie/searchMovieInfo.json"

def load_json(p: Path):
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except:
        return None

def save_json(p: Path, data: dict):
    p.parent.mkdir(parents=True, exist_ok=True)
    tmp = p.with_suffix(".json.tmp")
    tmp.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp.replace(p)

# 'ㅎ'씨 배우 타겟 (황정민 포함)
def is_target_name(name: str) -> bool:
    if not name: return False
    first_char = name[0]
    if '가' <= first_char <= '힣':
        chosung = (ord(first_char) - 0xAC00) // 588
        return chosung == 18 # 'ㅎ'
    return False

def fetch_movie_info(session, api_key, movieCd):
    qs = urlencode({"key": api_key, "movieCd": movieCd})
    url = f"{MOVIE_INFO_URL}?{qs}"
    r = session.get(url, timeout=10)
    r.raise_for_status()
    j = r.json()
    if j.get("faultInfo") or j.get("faultResult"):
        raise RuntimeError(f"KOBIS fault: {j.get('faultInfo') or j.get('faultResult')}")
    return j

def backfill(budget: int, rate_sleep_ms: int):
    # 최신 영화부터 역순 정렬
    files = sorted([Path(p) for p in glob.glob(str(DETAIL_DIR / "**" / "*.json"), recursive=True)], reverse=True)
    
    print(f"[paths] 데이터 폴더: {DETAIL_DIR}")
    print(f"[scan] 총 파일 수: {len(files)}개")
    print(f"[filter] 타겟: 한국 영화 + 배우 이름이 'ㅎ'으로 시작 + 코드 없음")

    updated = 0
    skipped = 0
    used = 0

    for p in files:
        raw = load_json(p)
        if not raw: continue
        
        # 데이터 구조 확인
        data = raw if raw.get("movieCd") else ((raw.get("movieInfoResult") or {}).get("movieInfo") or {})
        movieCd = data.get("movieCd")
        if not movieCd: continue

        # 한국 영화 필터
        nations = data.get("nations") or []
        is_korea = any(n.get("nationNm") == "한국" for n in nations)
        if not is_korea:
            skipped += 1
            continue

        # 타겟 배우 확인 (코드 비어있는 경우만)
        actors = data.get("actors") or []
        needs_update = False
        for a in actors:
            nm = a.get("peopleNm", "").strip()
            cd = a.get("peopleCd", "").strip()
            if nm and not cd and is_target_name(nm):
                needs_update = True
                break
        
        if not needs_update:
            skipped += 1
            continue

        if used >= budget:
            print(f"[info] 예산 소진 ({used}회). 중단.")
            break

        # API 호출
        try:
            session, api_key = get_session()
            j = fetch_movie_info(session, api_key, movieCd)
            used += 1
            
            if rate_sleep_ms > 0:
                time.sleep(rate_sleep_ms / 1000.0)

            info = (j.get("movieInfoResult") or {}).get("movieInfo") or {}
            if not info:
                skipped += 1
                continue

            # 데이터 업데이트
            new_actors = []
            for a in info.get("actors", []) or []:
                new_actors.append({
                    "peopleCd": a.get("peopleCd", "").strip(),
                    "peopleNm": a.get("peopleNm", "").strip(),
                    "repRoleNm": "배우",
                    "cast": a.get("cast", "").strip()
                })
            
            # 원본 데이터 구조 유지하며 actors 교체
            if raw.get("movieCd"):
                raw["actors"] = new_actors
            elif "movieInfoResult" in raw:
                raw["movieInfoResult"]["movieInfo"]["actors"] = new_actors

            save_json(p, raw)
            updated += 1
            print(f"[성공] {movieCd} ({data.get('movieNm')}) 업데이트 완료")

        except Exception as e:
            print(f"[실패] {movieCd}: {e}")
            time.sleep(1)
            skipped += 1

    print(f"=== 완료: {updated}개 업데이트, {skipped}개 건너뜀, API {used}회 사용 ===")

if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--budget", type=int, default=1000)
    ap.add_argument("--rate-sleep-ms", type=int, default=250)
    args, unknown = ap.parse_known_args()
    
    if not API_KEYS:
        print("[error] API 키가 없습니다.")
        exit(1)
        
    backfill(args.budget, args.rate_sleep_ms)
