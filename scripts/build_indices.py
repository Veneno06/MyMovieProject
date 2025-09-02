# scripts/build_indices.py
# docs/data/movies/**.json -> docs/data/search/movies.json, people.json 생성
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
        return f"{s[0:4]}-{s[4:6]}-{s[6:8]}"
    return s

def normalize_detail(raw: dict) -> dict | None:
    """flat 또는 raw(KOFIC) 모두를 표준 형태로 정규화."""
    if not isinstance(raw, dict):
        return None

    # 1) flat
    if raw.get("movieCd"):
        d = {
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
        return d

    # 2) raw (movieInfoResult.movieInfo)
    mi = ((raw.get("movieInfoResult") or {}).get("movieInfo") or {})
    if not mi.get("movieCd"):
        return None

    # 국가/장르/심의 등 KOFIC 원형 구조 보정
    def first_or_blank(arr, key):
        if isinstance(arr, list) and arr:
            return arr[0].get(key, "")
        return ""

    rep_nation = first_or_blank(mi.get("nations"), "nationNm")
    watch_grade = first_or_blank(mi.get("audits"), "watchGradeNm")
    genres = []
    if isinstance(mi.get("genres"), list):
        for g in mi["genres"]:
            name = g.get("genreNm", "")
            if name:
                genres.append(name)

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

    d = {
        "movieCd": (mi.get("movieCd") or "").strip(),
        "movieNm": mi.get("movieNm", ""),
        "openDt": yyyymmdd_to_iso(str(mi.get("openDt") or "")),
        "prdtYear": str(mi.get("prdtYear") or ""),
        "repNation": rep_nation,
        "grade": watch_grade,
        "genres": genres,
        "audiAcc": mi.get("audiAcc"),
        "directors": directors,
        "actors": actors,
    }
    return d

def build():
    files = [Path(p) for p in glob.iglob(str(MOVIE_DIR / "**" / "*.json"), recursive=True)
             if not p.endswith(".gitkeep")]
    files.sort()
    print(f"[paths] ROOT={ROOT}")
    print(f"[paths] MOVIE_DIR={MOVIE_DIR}")
    print(f"[scan] detail files: {len(files)}")

    movies = []
    people_map = {}  # peopleCd -> {...}
    cnt_bad = cnt_no_cd = 0

    for p in files:
        raw = load_json(p)
        if not raw:
            cnt_bad += 1
            continue

        d = normalize_detail(raw)
        if not d or not d.get("movieCd"):
            cnt_no_cd += 1
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
            arr = d.get(key) or []
            if not isinstance(arr, list): continue
            for it in arr:
                peopleCd = (it.get("peopleCd") or "").strip()
                peopleNm = (it.get("peopleNm") or "").strip()
                if not peopleCd or not peopleNm:
                    continue
                node = people_map.setdefault(peopleCd, {
                    "peopleCd": peopleCd,
                    "peopleNm": peopleNm,
                    "repRoleNm": role,
                    "films": []
                })
                node["films"].append({
                    "movieCd": d["movieCd"],
                    "movieNm": d["movieNm"],
                    "openDt": d["openDt"],
                    "part": (it.get("cast") or "").strip() if role=="배우" else "",
                })

    # 배우/감독 출연작 최신순
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
    print(f"[note] skipped: no_json={cnt_bad}, no_movieCd={cnt_no_cd}")

if __name__ == "__main__":
    build()
