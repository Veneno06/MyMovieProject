# scripts/build_indices.py
# 목적: docs/data/movies/**.json 읽어 검색 인덱스 두 개 생성(people.json, movies.json) — API 0회
from __future__ import annotations
import json, glob
from pathlib import Path
from datetime import datetime

def repo_root_from_here(here: Path) -> Path:
    cur = here.resolve()
    for _ in range(8):
        if (cur / ".git").exists() or (cur / "docs").exists():
            return cur
        cur = cur.parent
    return here.resolve().parents[2]

HERE = Path(__file__).resolve()
ROOT = repo_root_from_here(HERE)
MOVIE_DIR  = ROOT / "docs" / "data" / "movies"
SEARCH_DIR = ROOT / "docs" / "data" / "search"
SEARCH_DIR.mkdir(parents=True, exist_ok=True)

def load_json(p: Path):
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        return None

def yyyymmdd_to_iso(s: str) -> str:
    s = (s or "").strip()
    if len(s) == 8 and s.isdigit():
        return f"{s[:4]}-{s[4:6]}-{s[6:]}"
    return s

def normalize_detail(raw: dict) -> dict | None:
    if not isinstance(raw, dict):
        return None

    # flat
    if raw.get("movieCd"):
        return {
            "movieCd": (raw.get("movieCd") or "").strip(),
            "movieNm": raw.get("movieNm", ""),
            "openDt": yyyymmdd_to_iso(str(raw.get("openDt") or "")),
            "prdtYear": str(raw.get("prdtYear") or ""),
            "repNation": raw.get("repNation", ""),
            "grade": raw.get("grade", ""),
            "genres": raw.get("genres", []) or [],
            "audiAcc": raw.get("audiAcc"),
            "directors": raw.get("directors", []) or [],
            "actors": raw.get("actors", []) or [],
        }

    # raw
    mi = ((raw.get("movieInfoResult") or {}).get("movieInfo") or {})
    if not mi.get("movieCd"):
        return None

    def first(arr, key):
        if isinstance(arr, list) and arr:
            return arr[0].get(key, "")
        return ""

    rep_nation = first(mi.get("nations"), "nationNm")
    grade      = first(mi.get("audits"),  "watchGradeNm")

    genres = []
    for g in mi.get("genres", []) or []:
        n = g.get("genreNm", "")
        if n: genres.append(n)

    directors = []
    for it in mi.get("directors", []) or []:
        directors.append({
            "peopleCd": (it.get("peopleCd") or "").strip(),
            "peopleNm": (it.get("peopleNm") or "").strip(),
            "repRoleNm": "감독",
        })

    actors = []
    for it in mi.get("actors", []) or []:
        actors.append({
            "peopleCd": (it.get("peopleCd") or "").strip(),
            "peopleNm": (it.get("peopleNm") or "").strip(),
            "repRoleNm": "배우",
            "cast": (it.get("cast") or "").strip(),
        })

    return {
        "movieCd": (mi.get("movieCd") or "").strip(),
        "movieNm": mi.get("movieNm", ""),
        "openDt": yyyymmdd_to_iso(str(mi.get("openDt") or "")),
        "prdtYear": str(mi.get("prdtYear") or ""),
        "repNation": rep_nation,
        "grade": grade,
        "genres": genres,
        "audiAcc": mi.get("audiAcc"),
        "directors": directors,
        "actors": actors,
    }

def build():
    files = [Path(p) for p in glob.iglob(str(MOVIE_DIR / "**" / "*.json"), recursive=True)
             if not p.endswith(".gitkeep")]
    files.sort()
    print(f"[paths] ROOT={ROOT}")
    print(f"[paths] MOVIE_DIR={MOVIE_DIR}")
    print(f"[scan] detail files: {len(files)}")

    movies = []
    people_map = {}  # peopleCd -> {...}
    bad = miss = 0

    for p in files:
        raw = load_json(p)
        if not raw:
            bad += 1
            continue

        d = normalize_detail(raw)
        if not d or not d.get("movieCd"):
            miss += 1
            continue

        movies.append({
            "movieCd": d["movieCd"],
            "movieNm": d["movieNm"],
            "openDt": d["openDt"],
            "prdtYear": d["prdtYear"],
            "repNation": d["repNation"],
            "grade": d["grade"],
            "genres": d["genres"],
            "audiAcc": d.get("audiAcc"),
        })

        for key, role in (("directors","감독"), ("actors","배우")):
            for it in d.get(key, []) or []:
                cd = (it.get("peopleCd") or "").strip()
                nm = (it.get("peopleNm") or "").strip()
                if not cd or not nm:
                    continue
                node = people_map.setdefault(cd, {
                    "peopleCd": cd, "peopleNm": nm, "repRoleNm": role, "films": []
                })
                node["films"].append({
                    "movieCd": d["movieCd"],
                    "movieNm": d["movieNm"],
                    "openDt": d["openDt"],
                    "part": (it.get("cast") or "") if role == "배우" else "",
                })

    for v in people_map.values():
        v["films"].sort(key=lambda x: x["openDt"], reverse=True)

    now = int(datetime.utcnow().timestamp())
    (SEARCH_DIR / "movies.json").write_text(
        json.dumps({"generatedAt": now, "count": len(movies), "movies": movies}, ensure_ascii=False, indent=2),
        encoding="utf-8"
    )
    (SEARCH_DIR / "people.json").write_text(
        json.dumps({"generatedAt": now, "count": len(people_map), "people": list(people_map.values())}, ensure_ascii=False, indent=2),
        encoding="utf-8"
    )

    print(f"[index] movies: {len(movies)} / people: {len(people_map)}")
    print(f"[note] skipped: no_json={bad}, no_movieCd={miss}")

if __name__ == "__main__":
    build()
