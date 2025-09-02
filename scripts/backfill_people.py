# scripts/backfill_people.py
# 목적: docs/data/movies/**.json 중 peopleCd가 빈 파일만 KOFIC에서 보충(최소 API) 후 다시 저장
from __future__ import annotations
import os, json, time, glob
from pathlib import Path
from urllib.parse import urlencode
import requests

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

API_KEY = os.environ.get("KOFIC_API_KEY", "")
BASE = "https://www.kobis.or.kr/kobisopenapi/webservice/rest/movie/searchMovieInfo.json"

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

def has_people_cd(arr) -> bool:
    if not isinstance(arr, list):
        return False
    for x in arr:
        if isinstance(x, dict) and (x.get("peopleCd") or "").strip():
            return True
    return False

def need_backfill(target: dict) -> bool:
    # 감독/배우 둘 다 peopleCd가 전혀 없을 때만 API 사용
    return not (has_people_cd(target.get("directors")) or has_people_cd(target.get("actors")))

def fetch_movie_info(movieCd: str, timeout=30) -> dict:
    qs = urlencode({"key": API_KEY, "movieCd": movieCd})
    url = f"{BASE}?{qs}"
    r = requests.get(url, timeout=timeout)
    r.raise_for_status()
    return r.json()

def backfill(budget: int, rate_sleep_ms: int) -> tuple[int,int,int]:
    if not API_KEY:
        print("[warn] KOFIC_API_KEY is empty. Backfill will be skipped.")
        return (0, 0, 0)

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
            break

        try:
            j = fetch_movie_info(movieCd)
        except Exception as e:
            print(f"[warn] fetch fail {movieCd}: {e}")
            skipped += 1
            continue

        used += 1
        if rate_sleep_ms > 0:
            time.sleep(rate_sleep_ms / 1000.0)

        info = (j.get("movieInfoResult") or {}).get("movieInfo") or {}

        # 정규화: 감독/배우 배열 만들기
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
            # 원래 구조(flat/raw)에 맞춰 대상 dict만 갱신
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
    backfill(args.budget, args.rate_sleep_ms)
