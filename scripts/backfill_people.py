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

def save_json(p: Path, data: dict):
    p.parent.mkdir(parents=True, exist_ok=True)
    # 안전한 JSON 쓰기 방식 사용
    with open(p, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

def is_h_name(name: str) -> bool:
    if not name: return False
    norm_name = unicodedata.normalize('NFC', name)
    first_char = norm_name[0]
    return '\ud558' <= first_char <= '\ud7a3'

def get_next_key_session():
    global CURRENT_KEY_INDEX
    if not API_KEYS: return None, None
    CURRENT_KEY_INDEX = (CURRENT_KEY_INDEX + 1) % len(API_KEYS)
    api_key = API_KEYS[CURRENT_KEY_INDEX]
    session = requests.Session()
    print(f"[system] 🔄 API Key switched to index {CURRENT_KEY_INDEX}")
    return session, api_key

def fetch_movie_info_smart(movieCd):
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
                if err_code == '320011': 
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
    # 최신 파일부터 (깨진 파일 복구가 우선이므로 전체 스캔)
    files = sorted([Path(p) for p in glob.glob(str(DETAIL_DIR / "**" / "*.json"), recursive=True)], reverse=True)
    
    print(f"[paths] 데이터 폴더: {DETAIL_DIR}")
    print(f"[scan] 총 파일 수: {len(files)}개")
    print(f"[mode] 'ㅎ'씨 배우 코드 채우기 + **손상된 파일 복구 모드**")

    updated = 0
    skipped = 0
    used = 0
    repaired = 0

    for p in files:
        if used >= budget:
            print(f"[info] 예산 소진 ({used}회). 중단합니다.")
            break

        is_corrupted = False
        raw = None
        movieCd = None

        # 1. 파일 읽기 시도 (깨졌는지 확인)
        try:
            raw = json.loads(p.read_text(encoding="utf-8"))
            
            # 읽히더라도 내용이 비정상이면 손상으로 간주
            if not raw or (not raw.get("movieCd") and not raw.get("movieInfoResult")):
                is_corrupted = True
            else:
                # 정상 파일이면 movieCd 추출
                data = raw if raw.get("movieCd") else raw["movieInfoResult"]["movieInfo"]
                movieCd = data.get("movieCd")
        except:
            is_corrupted = True

        # 2. 손상된 파일 복구 로직
        if is_corrupted:
            # 파일명에서 ID 추출 (파일명이 '20123456.json' 형태라고 가정)
            movieCd = p.stem 
            if not movieCd.isdigit(): 
                skipped += 1
                continue
            print(f"[복구] 손상된 파일 감지: {movieCd} -> 재다운로드 시도")
            
            try:
                j = fetch_movie_info_smart(movieCd)
                used += 1
                info = (j.get("movieInfoResult") or {}).get("movieInfo")
                if info:
                    save_json(p, j) # 정상 데이터로 덮어쓰기
                    repaired += 1
                    # 복구된 데이터로 업데이트 로직 계속 진행하기 위해 raw 갱신
                    raw = j
                else:
                    print(f"[실패] 복구 실패 (데이터 없음): {movieCd}")
                    continue
            except Exception as e:
                print(f"[실패] 복구 중 에러 {movieCd}: {e}")
                continue

        # --- 여기서부터는 정상(또는 복구된) raw 데이터를 가지고 진행 ---
        data = raw if raw.get("movieCd") else ((raw.get("movieInfoResult") or {}).get("movieInfo") or {})
        movieCd = data.get("movieCd")
        
        # 한국 영화 필터
        nations = data.get("nations") or []
        is_korea = any(n.get("nationNm") == "한국" for n in nations)
        if not is_korea:
            continue

        # 'ㅎ'씨 배우 타겟팅
        actors = data.get("actors") or []
        needs_update = False
        
        for a in actors:
            cd = a.get("peopleCd", "").strip()
            nm = a.get("peopleNm", "").strip()
            if (not cd) and is_h_name(nm):
                needs_update = True
                break
        
        if not needs_update:
            skipped += 1
            continue

        # 이미 복구하면서 API를 썼다면 중복 호출 방지
        if is_corrupted: 
            # 이미 위에서 저장했으므로 카운트만 하고 패스
            updated += 1
            continue

        # 업데이트 API 호출
        try:
            print(f"[API 호출] {movieCd} - {data.get('movieNm')} (코드 보강)")
            j = fetch_movie_info_smart(movieCd)
            used += 1
            
            if rate_sleep_ms > 0: time.sleep(rate_sleep_ms / 1000.0)

            info = (j.get("movieInfoResult") or {}).get("movieInfo") or {}
            if info:
                # 덮어쓰기
                save_json(p, j)
                updated += 1
        except Exception as e:
            print(f"[실패] {movieCd}: {e}")
            skipped += 1

    print(f"=== 완료: {updated}개 처리 (복구 {repaired}건), API {used}회 사용 ===")

if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--budget", type=int, default=3000)
    ap.add_argument("--rate-sleep-ms", type=int, default=200)
    args, unknown = ap.parse_known_args()
    
    if not API_KEYS:
        print("[error] API 키가 없습니다.")
        exit(1)
        
    backfill(args.budget, args.rate_sleep_ms)
