# scripts/build_indices.py
# docs/data/movies/**.json -> search/movies.json, search/people.json 생성

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

def build():
    files = [Path(p) for p in glob.iglob(str(MOVIE_DIR / "**" / "*.json"), recursive=True)
             if not p.endswith(".gitkeep")]
    files.sort()
    print(f"[paths] ROOT={ROOT}")
    print(f"[paths] MOVIE_DIR={MOVIE_DIR}")
    print(f"[scan] detail files: {len(files)}")

    movies = []
    people_map = {}  # peopleCd -> {...}

    for p in files:
        d = load_json(p)
        if not d: continue
        movieCd = (d.get("movieCd") or "").strip()
        if not movieCd: continue

        movies.append({
            "movieCd": movieCd,
            "movieNm": d.get("movieNm", ""),
            "openDt": d.get("openDt", ""),
            "prdtYear": d.get("prdtYear", ""),
            "repNation": d.get("repNation", ""),
            "grade": d.get("grade", ""),
            "genres": d.get("genres", []),
            "audiAcc": d.get("audiAcc"),
        })

        for key, role in (("directors","감독"), ("actors","배우")):
            arr = d.get(key) or []
            if not isinstance(arr, list): continue
            for it in arr:
                peopleCd = (it.get("peopleCd") or "").strip()
                peopleNm = (it.get("peopleNm") or "").strip()
                if not peopleCd or not peopleNm:
                    continue  # peopleCd 없는 건 버림
                node = people_map.setdefault(peopleCd, {
                    "peopleCd": peopleCd,
                    "peopleNm": peopleNm,
                    "repRoleNm": role,
                    "films": []
                })
                node["films"].append({
                    "movieCd": movieCd,
                    "movieNm": d.get("movieNm", ""),
                    "openDt": d.get("openDt", ""),
                    "part": (it.get("cast") or "").strip() if role=="배우" else "",
                })

    # 개봉일 역순
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

if __name__ == "__main__":
    build()
