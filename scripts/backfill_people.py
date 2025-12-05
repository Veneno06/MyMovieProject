# scripts/backfill_people.py
# 목적: 한국 영화 중 배우 이름이 'ㅎ'으로 시작하는 경우만 집중적으로 코드 수집
# 타겟: 황정민을 비롯한 'ㅎ'씨 배우들의 데이터를 최우선으로 확보
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

def get_shape(raw: dict) -> tuple[str, dict]:
    if isinstance(raw, dict) and raw.get("movieCd"):
        return "flat", raw
    mi = ((raw or {}).get("movieInfoResult") or {}).get("movieInfo") or {}
    if mi.get("movieCd"):
        return "raw", mi
    return "none", {}

def is_korean_movie(target: dict) -> bool:
    """한국 영화인지 확인"""
    nations = target.get("nations") or []
    if not isinstance(nations, list): return False
    for n in nations:
        if isinstance(n, dict) and n.get("nationNm") == "한국":
            return True
    return False

# [핵심 수정] 타겟 자음 범위: 오직 'ㅎ' (황정민 타겟)
def is_name_in_range(name: str) -> bool:
    """
    이름의 첫 글자가 'ㅎ'(히읗)인지 확인.
    """
    if not name: return False
    first_char = name[0]
    
    # 한글 유니코드 계산
    # 초성 인덱스 18이 'ㅎ'입니다.
    
    if '가' <= first_char <= '힣':
        chosung_idx = (ord(first_char) - 0xAC00) // 588
        return chosung_idx == 18
    return False

def need_backfill(target: dict) -> bool:
    """
    1. 코드가 없는 배우가 있어야 함
    2. 그 배우의 이름이 'ㅎ'으로 시작해야 함
    """
    actors = target.get("actors") or []
    if not isinstance(actors, list): return False

    for actor in actors:
        nm = (actor.get("peopleNm") or "").strip()
        cd = (actor.get("peopleCd") or "").strip()
        
        # 코드가 없고(빈칸) + 이름이 'ㅎ'인 경우
        if nm and not cd:
            if is_name_in_range(nm):
                return True
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
    
    print(f"[paths] DETAIL_DIR={DETAIL_DIR}")
    print(f"[scan] detail files: {len(files)} (Reverse Order)")
    print(f"[filter] Target: Korean Movies + Actor Name starts with 'ㅎ'")

    updated = skipped = used = 0

    for p in files:
        raw = load_json(p)
        if not raw: continue
        
        shape, trg = get_shape(raw)
        if shape == "none": continue

        movieCd = (trg.get("movieCd") or "").strip()
        if not movieCd: continue

        # 1. 한국 영화 필터
        if not is_korean_movie(trg):
            skipped += 1
            continue

        # 2. 타겟 배우('ㅎ', 코드 없음)가 포함된 영화인지 확인
        if not need_backfill(trg):
            skipped += 1
            continue

        if used >= budget:
            print(f"[info] Budget reached ({used}). Stop.")
            break

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
            new_directors = []
            for d in info.get("directors", []) or []:
                new_directors.append({
                    "peopleCd": d.get("peopleCd", "").strip(),
                    "peopleNm": d.get("peopleNm", "").strip(),
                    "repRoleNm": "감독"
                })

            new_actors = []
            for a in info.get("actors", []) or []:
                new_actors.append({
                    "peopleCd": a.get("peopleCd", "").strip(),
                    "peopleNm": a.get("peopleNm", "").strip(),
                    "repRoleNm": "배우",
                    "cast": a.get("cast", "").strip()
                })

            # 실제로 코드가 채워졌는지 확인 (하나라도 채워졌으면 저장)
            trg["directors"] = new_directors
            trg["actors"] = new_actors
            
            save_json(p, raw)
            updated += 1
            print(f"[ok] {movieCd} ({trg.get('movieNm')}) updated")

        except Exception as e:
            print(f"[warn] {movieCd}: {e}")
            time.sleep(1)
            skipped += 1

    print(f"[done] updated={updated}, skipped={skipped}, used_api={used}")

if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--budget", type=int, default=1000)
    ap.add_argument("--rate-sleep-ms", type=int, default=250)
    args, unknown = ap.parse_known_args()
    
    if not API_KEYS:
        print("[error] No API Keys env found.")
        exit(1)
        
    backfill(args.budget, args.rate_sleep_ms)
