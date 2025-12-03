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

# [중요] 모듈 경로 설정
CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
if CURRENT_DIR not in sys.path:
    sys.path.append(CURRENT_DIR)

try:
    import kofic_api
except ImportError:
    print("[오류] kofic_api.py가 필요합니다.")
    exit(1)

# 경로 설정
ROOT = Path(__file__).resolve().parents[1]
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

def backfill(budget: int, rate_sleep_ms: int) -> tuple[int,int,int]:
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
            # kofic_api 모듈을 사용하여 API 호출 (키 자동 관리)
            j = kofic_api.fetch(MOVIE_INFO_URL, {"movieCd": movieCd})
            
            # 결과 검증
            new_info = (j.get("movieInfoResult") or {}).get("movieInfo")
            if not new_info:
                print(f"[warn] API data empty for {movieCd}")
                skipped += 1
                continue

            # 데이터 갱신 (기존 구조 유지)
            # API에서 받은 데이터에는 peopleCd가 포함되어 있음
            trg["directors"] = new_info.get("directors", [])
            trg["actors"]    = new_info.get("actors", [])
            
            save_json(p, raw)
            updated += 1
            print(f"[ok] {movieCd} -> updated")

        except Exception as e:
            print(f"[warn] fetch fail {movieCd}: {e}")
            time.sleep(1)
            skipped += 1
            continue

        used += 1
        if rate_sleep_ms > 0:
            time.sleep(rate_sleep_ms / 1000.0)

    print(f"[done] updated={updated}, skipped={skipped}, used_api={used}")
    return updated, skipped, used

if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--budget", type=int, default=600, help="Max API calls")
    ap.add_argument("--rate-sleep-ms", type=int, default=250, help="Sleep ms")
    
    args, unknown = ap.parse_known_args()
    
    backfill(args.budget, args.rate_sleep_ms)
