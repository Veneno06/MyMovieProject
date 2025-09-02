# scripts/backfill_people.py
# - 상세 JSON에 peopleCd가 포함된 배우/감독 배열이 없으면
#   movieInfo API로 보강(backfill)합니다.

import os, json, time, glob
from pathlib import Path
from urllib.parse import urlencode
import requests

ROOT = Path(__file__).resolve().parents[1]
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

def has_people_with_code(arr):
    """배열에 peopleCd가 채워진 항목이 하나라도 있으면 True"""
    if not isinstance(arr, list):
        return False
    for x in arr:
        if isinstance(x, dict) and (x.get("peopleCd") or "").strip():
            return True
    return False

def need_backfill(d: dict) -> bool:
    """
    - peopleCd가 채워진 directors[] 또는 actors[]가 이미 있으면 False (백필 불필요)
    - 그렇지 않으면 True (API로 보강)
    """
    if has_people_with_code(d.get("directors")):
        return False
    if has_people_with_code(d.get("actors")):
        return False
    # 이름 문자열(actorsNm/directorNm)만 있는 경우 -> 여전히 백필 필요
    return True

def fetch_movie_info(movieCd: str, timeout=30):
    qs = urlencode({"key": API_KEY, "movieCd": movieCd})
    url = f"{BASE}?{qs}"
    r = requests.get(url, timeout=timeout)
    r.raise_for_status()
    return r.json()

def backfill(budget: int, rate_sleep_ms: int) -> tuple[int, int]:
    if not API_KEY:
        raise RuntimeError("KOFIC_API_KEY가 환경변수로 설정되지 않았습니다.")

    files = [Path(p) for p in glob.iglob(str(DETAIL_DIR / "**" / "*.json"), recursive=True) if not p.endswith(".gitkeep")]
    files.sort()
    updated = skipped = 0
    used = 0

    for p in files:
        data = load_json(p)
        if not data:
            continue

        movieCd = (data.get("movieCd") or "").strip()
        if not movieCd:
            skipped += 1
            continue

        if not need_backfill(data):
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

        for d in info.get("directors", []):
            directors.append({
                "peopleCd": (d.get("peopleCd") or "").strip(),
                "peopleNm": (d.get("peopleNm") or "").strip(),
                "repRoleNm": "감독",
            })

        for a in info.get("actors", []):
            actors.append({
                "peopleCd": (a.get("peopleCd") or "").strip(),
                "peopleNm": (a.get("peopleNm") or "").strip(),
                "repRoleNm": "배우",
                "cast": (a.get("cast") or "").strip(),
            })

        # 하나라도 채워졌을 때만 저장
        if any(x.get("peopleCd") for x in directors) or any(x.get("peopleCd") for x in actors):
            data["directors"] = directors
            data["actors"] = actors
            save_json(p, data)
            updated += 1
            rel = p.relative_to(ROOT)
            print(f"[ok] {movieCd} -> {rel} (dir:{len(directors)}, act:{len(actors)})")
        else:
            skipped += 1

    print(f"[done] updated={updated}, skipped={skipped}, used_api={used}")
    return updated, used

if __name__ == "__main__":
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("--budget", type=int, default=600, help="이번 실행 최대 API 호출수")
    ap.add_argument("--rate-sleep-ms", type=int, default=250, help="호출 간 대기(ms)")
    args = ap.parse_args()
    backfill(args.budget, args.rate_sleep_ms)
