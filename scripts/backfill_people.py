# scripts/backfill_people.py
# 목적: docs/data/movies/**.json 중 peopleCd가 빈 파일만 KOFIC에서 보충(최소 API) 후 다시 저장
from __future__ import annotations
import os, json, time, glob
from pathlib import Path
from urllib.parse import urlencode
import requests

# [수정] API 키 관리자 임포트
try:
    from kofic_api import get_session, API_KEYS
except ImportError:
    print("kofic_api.py가 필요합니다. 동일 디렉토리에 있는지 확인하세요.")
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

# [수정] API_KEY 및 BASE 상수를 kofic_api.py가 관리하므로 제거
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
    """('flat'/'raw'/'none', 대상 dict)"""
    if isinstance(raw, dict) and raw.get("movieCd"):
        return "flat", raw
    mi = ((raw or {}).get("movieInfoResult") or {}).get("movieInfo") or {}
    if mi.get("movieCd"):
        return "raw", mi
    return "none", {}

def is_missing_cd(arr) -> bool:
    """
    배열(actors 또는 directors)을 검사하여,
    단 한 명이라도 peopleCd가 비어있으면 True를 반환합니다.
    """
    if not isinstance(arr, list) or not arr:
        return False # 비어있는 배열은 "누락"이 아님
    
    for x in arr:
        # peopleNm은 있는데 peopleCd가 없는 경우
        if isinstance(x, dict) and (x.get("peopleNm") or "").strip():
            if not (x.get("peopleCd") or "").strip():
                return True # <-- 코드 누락 발견!
    return False # 모두 코드가 있거나, 이름이 없음

def need_backfill(target: dict) -> bool:
    """
    감독(directors) 배열이나 배우(actors) 배열 둘 중 하나라도
    peopleCd가 누락된 사람을 포함하고 있으면 True를 반환합니다.
    """
    return is_missing_cd(target.get("directors")) or is_missing_cd(target.get("actors"))

# [수정] 세션과 API 키를 인자로 받도록 변경
def fetch_movie_info(session: requests.Session, api_key: str, movieCd: str, timeout=(10, 60)) -> dict:
    qs = urlencode({"key": api_key, "movieCd": movieCd})
    url = f"{MOVIE_INFO_URL}?{qs}"
    r = session.get(url, timeout=timeout)
    r.raise_for_status()
    j = r.json()
    if j.get("faultInfo") or j.get("faultResult"):
        raise RuntimeError(f"KOBIS fault: {j.get('faultInfo') or j.get('faultResult')}")
    return j

def backfill(budget: int, rate_sleep_ms: int) -> tuple[int,int,int]:
    files = [Path(p) for p in glob.iglob(str(DETAIL_DIR / "**" / "*.json"), recursive=True)
             if not p.endswith(".gitkeep")]
    files.sort()
    print(f"[paths] ROOT={ROOT}")
    print(f"[paths] DETAIL_DIR={DETAIL_DIR}")
    print(f"[scan] detail files: {len(files)}")

    updated = skipped = used = 0

    for p in files:
        raw = load_json(p)
        shape, trg = get_shape(raw or {})
        if shape == "none":
            skipped += 1
            continue

        movieCd = (trg.get("movieCd") or "").strip()
        if not movieCd:
            skipped += 1
            continue

        if not need_backfill(trg):
            skipped += 1
            continue

        if used >= budget:
            print(f"[info] API budget reached ({used}). Stopping backfill.")
            break

        try:
            # [수정] 키 로테이터에서 세션과 다음 키를 가져옴
            session, api_key = get_session()
            j = fetch_movie_info(session, api_key, movieCd)
            
        except Exception as e:
            print(f"[warn] fetch fail {movieCd}: {e}")
            skipped += 1
            # [수정] API 키 제한 등 심각한 오류 시 5초 대기
            time.sleep(5)
            continue

        used += 1
        if rate_sleep_ms > 0:
            time.sleep(rate_sleep_ms / 1000.0)

        info = (j.get("movieInfoResult") or {}).get("movieInfo") or {}

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

        has_cd = False 
        for arr in (directors, actors):
            for x in arr:
                if x.get("peopleCd"):
                    has_cd = True
                    break
            if has_cd:
                break

        if has_cd:
            trg["directors"] = directors
            trg["actors"]    = actors
            save_json(p, raw)
            updated += 1
            rel = p.relative_to(ROOT)
            print(f"[ok] {movieCd} -> {rel} (dir:{len(directors)}, act:{len(actors)})")
        else:
            skipped += 1

    print(f"[done] updated={updated}, skipped={skipped}, used_api={used}")
    return updated, skipped, used

if __name__ == "__main__":
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("--budget", type=int, default=600, help="오늘 사용할 최대 API 호출 수(일 한도 이하로 지정)")
    ap.add_argument("--rate-sleep-ms", type=int, default=250, help="호출 간 대기(ms) - 200~400 권장")
    args = ap.parse_args()
    
    # [수정] kofic_api.py가 키를 로드할 시간을 잠시 줌
    if not API_KEYS:
        print("[backfill_people] API 키가 로드되지 않았습니다. GitHub Secrets를 확인하세요.")
        exit(1)
        
    backfill(args.budget, args.rate_sleep_ms)
