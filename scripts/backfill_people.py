# scripts/backfill_people.py
from __future__ import annotations
import os
import json
import time
import glob
import argparse
import sys
import unicodedata
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

# 전역 변수
CURRENT_KEY_INDEX = 0

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

# [중요] 한글 정규화 (자소 분리 방지) 및 'ㅎ' 체크
def is_h_name(name: str) -> bool:
    if not name: return False
    # NFC 정규화: 'ㅎ'+'ㅘ'+'ㅇ' -> '황'으로 합침
    norm_name = unicodedata.normalize('NFC', name)
    first_char = norm_name[0]
    # '하'(U+D558) ~ '힣'(U+D7A3) 범위 확인
    return '\ud558' <= first_char <= '\ud7a3'

def get_next_key_session():
    global CURRENT_KEY_INDEX
    if not API_KEYS: return None, None
    
    CURRENT_KEY_INDEX = (CURRENT_KEY_INDEX + 1) % len(API_KEYS)
    api_key = API_KEYS[CURRENT_KEY_INDEX]
    session = requests.Session()
    print(f"[system] 🔄 API Key switched to index {CURRENT_KEY_INDEX} (Key: ...{api_key[-4:]})")
    return session, api_key

def fetch_movie_info_smart(movieCd):
    """키 한도 초과 시 자동으로 다음 키 사용"""
    global CURRENT_KEY_INDEX
    if not API_KEYS: raise RuntimeError("No API Keys")
    
    api_key = API_KEYS[CURRENT_KEY_INDEX]
    session = requests.Session()
    max_retries = len(API_KEYS)
    
    for attempt in range(max_retries + 1):
        try:
            qs = urlencode({"key": api_key, "movieCd": movieCd})
            url = f"{MOVIE_INFO_URL}?{qs}"
            r = session.get(url, timeout=10)
            r.raise_for_status()
            j = r.json()
            
            fault = j.get("faultInfo") or j.get("faultResult")
            if fault:
                err_code = fault.get("errorCode")
                if err_code == '320011': # 키 한도 초과
                    print(f"[warning] Key exhausted. Switching...")
                    session, api_key = get_next_key_session()
                    continue
                else:
                    raise RuntimeError(f"KOBIS fault: {fault.get('message')}")
            return j
            
        except Exception as e:
            if attempt == max_retries: raise e
            time.sleep(1)
            
    raise RuntimeError("All API keys exhausted.")

def backfill(budget: int, rate_sleep_ms: int):
    # 최신 영화부터 역순 정렬
    files = sorted([Path(p) for p in glob.glob(str(DETAIL_DIR / "**" / "*.json"), recursive=True)], reverse=True)
    
    print(f"[paths] 데이터 폴더: {DETAIL_DIR}")
    print(f"[scan] 총 파일 수: {len(files)}개")
    print(f"[filter] 타겟: 한국 영화 + 'ㅎ'씨 배우인데 코드가 없는 경우 (우선순위)")

    updated = 0
    skipped = 0
    used = 0

    for p in files:
        raw = load_json(p)
        if not raw: continue
        
        data = raw if raw.get("movieCd") else ((raw.get("movieInfoResult") or {}).get("movieInfo") or {})
        movieCd = data.get("movieCd")
        if not movieCd: continue

        # 1. 한국 영화 필터
        nations = data.get("nations") or []
        is_korea = any(n.get("nationNm") == "한국" for n in nations)
        if not is_korea:
            skipped += 1
            continue

        # 2. 'ㅎ'씨 배우 타겟팅 (API 절약의 핵심)
        actors = data.get("actors") or []
        needs_update = False
        
        for a in actors:
            cd = a.get("peopleCd", "").strip()
            nm = a.get("peopleNm", "").strip()
            
            # [핵심 조건] 코드가 비어있고 AND 이름이 'ㅎ'으로 시작하는가?
            if (not cd) and is_h_name(nm):
                needs_update = True
                break
        
        if not needs_update:
            skipped += 1
            continue

        if used >= budget:
            print(f"[info] 예산 소진 ({used}회). 안전하게 작업을 중단하고 저장 단계로 넘어갑니다.")
            break

        # 3. API 호출
        try:
            j = fetch_movie_info_smart(movieCd)
            used += 1
            
            if rate_sleep_ms > 0:
                time.sleep(rate_sleep_ms / 1000.0)

            info = (j.get("movieInfoResult") or {}).get("movieInfo") or {}
            if not info:
                skipped += 1
                continue

            # 4. 데이터 업데이트
            new_actors = []
            for a in info.get("actors", []) or []:
                new_actors.append({
                    "peopleCd": a.get("peopleCd", "").strip(),
                    "peopleNm": a.get("peopleNm", "").strip(),
                    "repRoleNm": "배우",
                    "cast": a.get("cast", "").strip()
                })
            
            new_directors = []
            for d in info.get("directors", []) or []:
                new_directors.append({
                    "peopleCd": d.get("peopleCd", "").strip(),
                    "peopleNm": d.get("peopleNm", "").strip(),
                    "repRoleNm": "감독"
                })

            if raw.get("movieCd"):
                raw["actors"] = new_actors
                raw["directors"] = new_directors
            elif "movieInfoResult" in raw:
                raw["movieInfoResult"]["movieInfo"]["actors"] = new_actors
                raw["movieInfoResult"]["movieInfo"]["directors"] = new_directors

            save_json(p, raw)
            updated += 1
            print(f"[성공] {movieCd} ({data.get('movieNm')}) - 'ㅎ'씨 배우 코드 업데이트 완료")

        except RuntimeError as re:
            if "All API keys exhausted" in str(re):
                print("[STOP] 모든 API 키가 소진되었습니다. 현재까지 작업을 저장합니다.")
                break
            print(f"[실패] {movieCd}: {re}")
            skipped += 1
        except Exception as e:
            print(f"[실패] {movieCd}: {e}")
            time.sleep(1)
            skipped += 1

    print(f"=== 완료: {updated}개 업데이트, {skipped}개 건너뜀, API {used}회 사용 ===")

if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--budget", type=int, default=3000)
    ap.add_argument("--rate-sleep-ms", type=int, default=200)
    args, unknown = ap.parse_known_args()
    
    if not API_KEYS:
        print("[error] API 키가 없습니다.")
        exit(1)
        
    backfill(args.budget, args.rate_sleep_ms)
