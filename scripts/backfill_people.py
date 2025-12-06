# scripts/backfill_people.py
# 목적: 기존 데이터에 영화인 코드가 없으면 API로 조회하여 채워 넣음
# 타겟: 'ㅎ'씨 배우(황정민) 집중 공략 + 한국 영화
# 목적: 살아있는 API 키를 찾아내어 데이터를 끝까지 수집하고, 키가 다 죽으면 안전하게 저장
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

# [핵심] 환경 변수에서 직접 키 로드 및 관리
ALL_KEYS = []
for i in range(1, 10):
    k = os.getenv(f"KOFIC_API_KEY_{i}") if i > 1 else os.getenv("KOFIC_API_KEY")
    if k: ALL_KEYS.append(k.strip())

print(f"[init] 로드된 총 API 키 개수: {len(ALL_KEYS)}개")

# 경로 설정
HERE = Path(__file__).resolve()
ROOT = HERE.parents[1] if HERE.parents[1].name == "MyMovieProject" else HERE.parents[2]
DETAIL_DIR = ROOT / "docs" / "data" / "movies"
MOVIE_INFO_URL = "https://www.kobis.or.kr/kobisopenapi/webservice/rest/movie/searchMovieInfo.json"

# 전역 세션
session = requests.Session()
current_key_idx = 0

def get_current_key():
    if not ALL_KEYS: return None
    return ALL_KEYS[current_key_idx % len(ALL_KEYS)]

def switch_next_key():
    global current_key_idx, ALL_KEYS
    if not ALL_KEYS: return False
    
    # 현재 죽은 키 제거
    dead_key = ALL_KEYS.pop(0) 
    print(f"[system] 키 사망 확인 (남은 키: {len(ALL_KEYS)}개). 다음 키로 교체합니다.")
    
    if not ALL_KEYS:
        print("[system] 🚨 모든 API 키가 소진되었습니다!")
        return False
    return True

def load_json(p: Path):
    try: return json.loads(p.read_text(encoding="utf-8"))
    except: return None

def save_json(p: Path, data: dict):
    p.parent.mkdir(parents=True, exist_ok=True)
    tmp = p.with_suffix(".json.tmp")
    tmp.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp.replace(p)

def is_korean_movie(target: dict) -> bool:
    nations = target.get("nations") or []
    if not isinstance(nations, list): return False
    return any(n.get("nationNm") == "한국" for n in nations)

# 'ㅎ'씨 배우 타겟 (황정민 포함)
def is_target_name(name: str) -> bool:
    if not name: return False
    if '가' <= name[0] <= '힣':
        return ((ord(name[0]) - 0xAC00) // 588) == 18 # ㅎ
    return False

def fetch_movie_info(movieCd):
    api_key = get_current_key()
    if not api_key: raise RuntimeError("NO_KEYS_LEFT")

    qs = urlencode({"key": api_key, "movieCd": movieCd})
    url = f"{MOVIE_INFO_URL}?{qs}"
    r = session.get(url, timeout=10)
    r.raise_for_status()
    j = r.json()
    
    # 에러 체크
    fault = j.get("faultInfo") or j.get("faultResult")
    if fault:
        msg = fault.get("message", "")
        code = fault.get("errorCode", "")
        # 키 한도 초과 에러 코드: 320011
        if code == "320011" or "초과" in msg:
            raise RuntimeError("QUOTA_EXCEEDED")
        raise RuntimeError(f"KOBIS fault: {msg}")
    return j

def backfill(budget: int, rate_sleep_ms: int):
    files = sorted([Path(p) for p in glob.glob(str(DETAIL_DIR / "**" / "*.json"), recursive=True)], reverse=True)
    print(f"[scan] 총 파일: {len(files)}개")

    updated = 0
    skipped = 0
    used = 0

    for p in files:
        # 키가 하나도 없으면 루프 즉시 종료 (저장 단계로 이동)
        if not ALL_KEYS:
            print("[stop] 사용 가능한 키가 없어 작업을 조기 종료합니다.")
            break

        if used >= budget:
            print(f"[info] 예산 소진 ({used}회). 종료.")
            break

        raw = load_json(p)
        if not raw: continue
        
        data = raw if raw.get("movieCd") else ((raw.get("movieInfoResult") or {}).get("movieInfo") or {})
        movieCd = data.get("movieCd")
        if not movieCd: continue

        # 필터링
        if not is_korean_movie(data):
            skipped += 1
            continue

        actors = data.get("actors") or []
        needs_update = False
        
        # 황정민 강제 확인 + ㅎ씨 배우 중 코드 없는 경우
        has_hwang = any(a.get("peopleNm","").strip() == "황정민" for a in actors)
        has_missing_h = any(
            (not a.get("peopleCd","").strip()) and is_target_name(a.get("peopleNm","").strip())
            for a in actors
        )

        if has_hwang or has_missing_h:
            needs_update = True
        else:
            skipped += 1
            continue

        # --- API 호출 및 재시도 로직 ---
        while True:
            try:
                j = fetch_movie_info(movieCd)
                used += 1
                if rate_sleep_ms > 0: time.sleep(rate_sleep_ms / 1000.0)

                info = (j.get("movieInfoResult") or {}).get("movieInfo") or {}
                if not info: break

                # 데이터 업데이트
                new_actors = []
                for a in info.get("actors", []) or []:
                    new_actors.append({
                        "peopleCd": a.get("peopleCd", "").strip(),
                        "peopleNm": a.get("peopleNm", "").strip(),
                        "repRoleNm": "배우",
                        "cast": a.get("cast", "").strip()
                    })
                
                if raw.get("movieCd"): raw["actors"] = new_actors
                elif "movieInfoResult" in raw: raw["movieInfoResult"]["movieInfo"]["actors"] = new_actors

                save_json(p, raw)
                updated += 1
                print(f"[성공] {movieCd} ({data.get('movieNm')}) 업데이트 완료")
                break # 성공하면 while 탈출

            except RuntimeError as e:
                if str(e) == "QUOTA_EXCEEDED":
                    # 키 교체 시도
                    if not switch_next_key():
                        # 키가 다 떨어지면 바깥 루프도 종료해야 함
                        break 
                    # 키 교체 성공했으면 while 문 처음으로 돌아가서 재시도
                    print(f"[retry] {movieCd} 새 키로 재시도...")
                    continue
                elif str(e) == "NO_KEYS_LEFT":
                    break
                else:
                    print(f"[실패] {movieCd}: {e}")
                    skipped += 1
                    break # 다른 에러면 건너뜀
            except Exception as e:
                print(f"[에러] {movieCd}: {e}")
                skipped += 1
                break

    print(f"=== 최종 완료: {updated}개 업데이트 (API {used}회 사용) ===")

if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--budget", type=int, default=1000)
    ap.add_argument("--rate-sleep-ms", type=int, default=250)
    args, unknown = ap.parse_known_args()
    
    backfill(args.budget, args.rate_sleep_ms)
