# scripts/backfill_people.py
from __future__ import annotations
import os, json, time, glob
from pathlib import Path
from urllib.parse import urlencode
import requests

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
        with p.open(encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return None

def save_json(p: Path, data: dict):
    p.parent.mkdir(parents=True, exist_ok=True)
    tmp = p.with_suffix(".json.tmp")
    tmp.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp.replace(p)

def get_shape(raw: dict) -> tuple[str, dict]:
    """('flat'/'raw', 대상 dict) 반환. raw면 movieInfo 참조 반환"""
    if raw.get("movieCd"):
        return "flat", raw
    mi = ((raw.get("movieInfoResult") or {}).get("movieInfo") or {})
    if mi.get("movieCd"):
        return "raw", mi
    return "none", {}

def has_people_cd(arr) -> bool:
    if not isinstance(arr, list):
        return False
    return any(isinstance(x, dict) and (x.get("peopleCd") or "").strip() for x in arr)

def need_backfill(target: dict) -> bool:
    if has_people_cd(target.get("directors")): return False
    if has_people_cd(target.get("actors")):    return False
    return True

def fetch_movie_info(movieCd: str, timeout=30):
    qs = urlencode({"key": API_KEY, "movieCd": movieCd})
    url = f"{BASE}?{qs}"
    r = requests.get(url, timeout=timeout)
    r.raise_for_status()
    return r.json()

def backfill(budget: int, rate_sleep_ms: int) -> tuple[int,int,int]:
    if not API_KEY:
        raise RuntimeError("KOFIC_API_KEY 비어있음")

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
        time.sleep(max(0, rate_sleep_ms) / 1000.0)

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

        if any(x.get("peopleCd") for x in directors) or any(x.get("peopleCd") for x in actors)):
            # 원래 구조 유지한 채로 덮어쓰기
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
    ap.add_argument("--budget", type=int, default=600)
    ap.add_argument("--rate-sleep-ms", type=int, default=250)
    args = ap.parse_args()
    backfill(args.budget, args.rate_sleep_ms)
