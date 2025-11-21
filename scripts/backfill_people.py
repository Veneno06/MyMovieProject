# scripts/backfill_people.py
# 목적: docs/data/movies/**.json 중 peopleCd가 빈 파일만 KOFIC에서 보충(최소 API) 후 다시 저장
from __future__ import annotations
import os, json, time, glob
import argparse
from pathlib import Path
from urllib.parse import urlencode
import requests
import sys

# [중요] 모듈 경로를 확실하게 추가 (GitHub Actions 환경 대응)
# 현재 스크립트(backfill_people.py)가 있는 디렉토리(scripts/)를 path에 추가
CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
if CURRENT_DIR not in sys.path:
    sys.path.append(CURRENT_DIR)

try:
    from kofic_api import get_session, API_KEYS
except ImportError:
    print(f"[오류] kofic_api.py 모듈을 찾을 수 없습니다. (검색 경로: {sys.path})")
    # 비상시를 대비해 한 번 더 시도
    try:
        sys.path.append(os.path.join(CURRENT_DIR, 'scripts'))
        from kofic_api import get_session, API_KEYS
    except ImportError:
        exit(1)

# 리포 루트 자동 탐지
def repo_root_from_here(here: Path) -> Path:
    cur = here.resolve()
    for _ in range(8):
        if (cur / ".git").exists() or (cur / "docs").exists():
            return cur
        cur = cur.parent
    return here.resolve().parents[2]

HERE = Path(__file__).resolve()
ROOT = repo_root_from_here(HERE)
DETAIL_DIR = ROOT / "docs" / "data" / "movies"
MOVIE_INFO_URL = "https://www.kobis.or.kr/kobisopenapi/webservice/rest/movie/searchMovieInfo.json"

def load_json(p: Path):
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except Exception:
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

def is_missing_cd(arr) -> bool:
    if not isinstance(arr, list) or not arr:
        return False
    for x in arr:
        if isinstance(x, dict) and (x.get("peopleNm") or "").strip():
            if not (x.get("peopleCd") or "").strip():
                return True
    return False

def need_backfill(target: dict) -> bool:
    return is_missing_cd(target.get("directors")) or is_missing_cd(target.get("actors"))

def fetch_movie_info(session: requests.Session, api_key: str, movieCd: str, timeout=(10, 60)) -> dict:
    qs = urlencode({"key": api_key, "movieCd": movieCd})
    url = f"{MOVIE_INFO_URL}?{qs}"
    r = session.get(url, timeout=timeout)
    r.raise_for_status()
    j = r.json()
    if j.get("faultInfo") or j.get("faultResult"):
        # 320010(일일 제한) 등의 에러는 상위에서 처리하도록 예외 발생
        raise RuntimeError(f"KOBIS fault: {j.get('faultInfo') or j.get('faultResult')}")
    return j

def backfill(budget: int, rate_sleep_ms: int) -> tuple[int,int,int]:
    # glob 결과 정렬하여 일관성 유지
    files = sorted([Path(p) for p in glob.glob(str(DETAIL_DIR / "**" / "*.json"), recursive=True)])
    print(f"[paths] DETAIL_DIR={DETAIL_DIR}")
    print(f"[scan] detail files: {len(files)}")

    updated = skipped = used = 0

    for p in files:
        raw = load_json(p)
        if not raw: continue
        
        shape, trg = get_shape(raw)
        if shape == "none":
            continue

        movieCd = (trg.get("movieCd") or "").strip()
        if not movieCd:
            continue

        if not need_backfill(trg):
            skipped += 1
            continue

        if used >= budget:
            print(f"[info] API budget reached ({used}). Stopping backfill.")
            break

        try:
            # 키 관리자로부터 세션과 키 획득
            session, api_key = get_session()
            j = fetch_movie_info(session, api_key, movieCd)
            
        except Exception as e:
            print(f"[warn] fetch fail {movieCd}: {e}")
            # API 키 제한 에러 등이면 잠시 대기
            time.sleep(1)
            skipped += 1
            continue

        used += 1
        if rate_sleep_ms > 0:
            time.sleep(rate_sleep_ms / 1000.0)

        info = (j.get("movieInfoResult") or {}).get("movieInfo") or {}
        if not info:
            skipped += 1
            continue

        # 데이터 정규화 및 덮어쓰기
        directors, actors = [], []
        for it in info.get("directors", []) or []:
            directors.append({
                "peopleCd": (it.get("peopleCd") or "").strip(),
                "peopleNm": (it.get("peopleNm") or "").strip(),
                "repRoleNm": "감독",
            })
        for it in info.get("actors", []) or []:
            actors.append({
                "peopleCd": (it.get("peopleCd") or "").strip(),
                "peopleNm": (it.get("peopleNm") or "").strip(),
                "repRoleNm": "배우",
                "cast": (it.get("cast") or "").strip(),
            })

        # 새로 받은 데이터에 코드가 있는지 확인
        has_cd = False 
        for arr in (directors, actors):
            for x in arr:
                if x.get("peopleCd"):
                    has_cd = True; break
            if has_cd: break

        if has_cd:
            trg["directors"] = directors
            trg["actors"]    = actors
            save_json(p, raw)
            updated += 1
            print(f"[ok] {movieCd} -> updated (dir:{len(directors)}, act:{len(actors)})")
        else:
            skipped += 1

    print(f"[done] updated={updated}, skipped={skipped}, used_api={used}")
    return updated, skipped, used

if __name__ == "__main__":
    # [수정] argparse를 사용하여 --budget 등의 인자를 올바르게 파싱
    ap = argparse.ArgumentParser()
    ap.add_argument("--budget", type=int, default=600, help="Max API calls")
    ap.add_argument("--rate-sleep-ms", type=int, default=250, help="Sleep ms")
    
    # 알 수 없는 인자가 있어도 무시하도록 parse_known_args 사용 (선택 사항)
    args, unknown = ap.parse_known_args()
    
    if not API_KEYS:
        print("[backfill_people] API Key not found in env.")
        exit(1)
        
    backfill(args.budget, args.rate_sleep_ms)
