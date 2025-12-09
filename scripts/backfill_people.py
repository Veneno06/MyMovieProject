# scripts/backfill_people.py
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

# 전역 변수: 현재 사용할 API 키 인덱스
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

def get_next_key_session():
    """키 한도가 초과되면 다음 키로 세션을 변경"""
    global CURRENT_KEY_INDEX
    if not API_KEYS:
        return None, None
    
    # 다음 키로 인덱스 이동
    CURRENT_KEY_INDEX = (CURRENT_KEY_INDEX + 1) % len(API_KEYS)
    api_key = API_KEYS[CURRENT_KEY_INDEX]
    session = requests.Session()
    session.headers.update({"User-Agent": "Mozilla/5.0"})
    print(f"[system] 🔄 API Key switched to index {CURRENT_KEY_INDEX} (Key ending in ...{api_key[-4:]})")
    return session, api_key

def fetch_movie_info_smart(movieCd):
    """320011 에러 발생 시 자동으로 키를 교체하며 재시도하는 스마트 함수"""
    global CURRENT_KEY_INDEX
    
    if not API_KEYS: 
        raise RuntimeError("No API Keys available.")
    
    api_key = API_KEYS[CURRENT_KEY_INDEX]
    session = requests.Session()
    
    # 키 개수만큼 재시도 기회를 줌 (모든 키를 순회할 때까지)
    max_retries = len(API_KEYS)
    
    for attempt in range(max_retries + 1):
        try:
            qs = urlencode({"key": api_key, "movieCd": movieCd})
            url = f"{MOVIE_INFO_URL}?{qs}"
            r = session.get(url, timeout=10)
            r.raise_for_status()
            j = r.json()
            
            # API 에러 응답 체크
            fault = j.get("faultInfo") or j.get("faultResult")
            if fault:
                err_code = fault.get("errorCode")
                err_msg = fault.get("message")
                
                # 320011: 키 한도 초과 -> 키 교체 후 재시도
                if err_code == '320011':
                    print(f"[warning] Key exhausted ({api_key[-4:]}). Switching...")
                    session, api_key = get_next_key_session()
                    continue # for 루프의 다음 시도로 넘어감 (새 키로 요청)
                else:
                    # 다른 에러는 즉시 중단 (예: 서버 오류 등)
                    raise RuntimeError(f"KOBIS fault: {err_msg}")
            
            return j # 성공 시 데이터 반환
            
        except Exception as e:
            # 네트워크 에러나 알 수 없는 에러인 경우 잠시 대기 후 재시도할 수도 있으나,
            # 여기서는 키 문제인 경우 위에서 처리했으므로 로깅 후 루프 진행
            if attempt == max_retries:
                raise e # 모든 키를 다 써봤는데도 안되면 에러 발생
            time.sleep(1)
            
    raise RuntimeError("All API keys exhausted.")

def backfill(budget: int, rate_sleep_ms: int):
    # 최신 영화부터 역순 정렬
    files = sorted([Path(p) for p in glob.glob(str(DETAIL_DIR / "**" / "*.json"), recursive=True)], reverse=True)
    
    print(f"[paths] 데이터 폴더: {DETAIL_DIR}")
    print(f"[scan] 총 파일 수: {len(files)}개")
    print(f"[filter] 타겟: 한국 영화 + 코드가 없는 배우가 포함된 경우")

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

        # 1. 한국 영화 필터
        nations = data.get("nations") or []
        is_korea = any(n.get("nationNm") == "한국" for n in nations)
        if not is_korea:
            skipped += 1
            continue

        # 2. 업데이트 필요 여부 판단
        actors = data.get("actors") or []
        needs_update = False
        for a in actors:
            cd = a.get("peopleCd", "").strip()
            # 코드가 없으면 업데이트 대상
            if not cd:
                needs_update = True
                break
        
        if not needs_update:
            skipped += 1
            continue

        if used >= budget:
            print(f"[info] 예산 소진 ({used}회). 안전하게 중단합니다.")
            break

        # 3. API 호출 (스마트 로직 적용)
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

            # 원본 데이터 구조 유지하며 actors/directors 교체
            if raw.get("movieCd"):
                raw["actors"] = new_actors
                raw["directors"] = new_directors
            elif "movieInfoResult" in raw:
                raw["movieInfoResult"]["movieInfo"]["actors"] = new_actors
                raw["movieInfoResult"]["movieInfo"]["directors"] = new_directors

            save_json(p, raw)
            updated += 1
            print(f"[성공] {movieCd} ({data.get('movieNm')}) 업데이트 완료")

        except RuntimeError as re:
            if "All API keys exhausted" in str(re):
                print("[STOP] 모든 API 키가 소진되었습니다. 현재까지의 작업을 저장하고 종료합니다.")
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
    ap.add_argument("--rate-sleep-ms", type=int, default=250)
    args, unknown = ap.parse_known_args()
    
    if not API_KEYS:
        print("[error] API 키가 없습니다.")
        exit(1)
        
    backfill(args.budget, args.rate_sleep_ms)
