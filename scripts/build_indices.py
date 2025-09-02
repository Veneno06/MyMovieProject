# scripts/build_indices.py
# - docs/data/movies/**.json을 읽어 movies.json, people.json 인덱스를 생성

import json, glob
from pathlib import Path
from datetime import datetime

ROOT = Path(__file__).resolve().parents[1]
MOVIE_DIR = ROOT / "docs" / "data" / "movies"
SEARCH_DIR = ROOT / "docs" / "data" / "search"
SEARCH_DIR.mkdir(parents=True, exist_ok=True)

def load_json(p: Path):
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        return None

def safe_date(s: str) -> str:
    s = (s or "").strip()
    if len(s) == 8 and s.isdigit():
        return f"{s[:4]}-{s[4:6]}-{s[6:]}"
    return ""

def build():
    movie_rows = []
    people_map = {}  # peopleCd -> {peopleCd, peopleNm, repRoleNm, films:[]}

    files = [Path(p) for p in glob.iglob(str(MOVIE_DIR / "**" / "*.json"), recursive=True) if not p.endswith(".gitkeep")]
    files.sort()

    for p in files:
        d = load_json(p)
        if not d:
            continue

        movieCd = (d.get("movieCd") or "").strip()
        if not movieCd:
            continue

        row = {
            "movieCd": movieCd,
            "movieNm": d.get("movieNm", ""),
            "openDt": d.get("openDt", ""),
            "prdtYear": d.get("prdtYear", ""),
            "repNation": d.get("repNation", ""),
            "grade": d.get("grade", ""),
            "genres": d.get("genres", []),
            "audiAcc": d.get("audiAcc"),
        }
        movie_rows.append(row)

        # 배우/감독 (peopleCd 없는 항목은 무시)
        for k, role in (("directors", "감독"), ("actors", "배우")):
            arr = d.get(k) or []
            if not isinstance(arr, list):
                continue
            for it in arr:
                peopleCd = (it.get("peopleCd") or "").strip()
                peopleNm = (it.get("peopleNm") or "").strip()
                if not peopleCd or not peopleNm:
                    continue
                pm = people_map.setdefault(peopleCd, {
                    "peopleCd": peopleCd,
                    "peopleNm": peopleNm,
                    "repRoleNm": role,   # 첫 역할 유지
                    "films": []
                })
                pm["films"].append({
                    "movieCd": movieCd,
                    "movieNm": row["movieNm"],
                    "openDt": row["openDt"],
                    "part": (it.get("cast") or "").strip() if role == "배우" else "",
                })

    # 개봉일 역순 정렬
    for pm in people_map.values():
        pm["films"].sort(key=lambda x: x["openDt"], reverse=True)

    # 검색 인덱스 파일 쓰기
    now = int(datetime.utcnow().timestamp())
    movies_out = {
        "generatedAt": now,
        "count": len(movie_rows),
        "movies": movie_rows,
    }
    people_out = {
        "generatedAt": now,
        "count": len(people_map),
        "people": list(people_map.values()),
    }

    (SEARCH_DIR / "movies.json").write_text(json.dumps(movies_out, ensure_ascii=False, indent=2), encoding="utf-8")
    (SEARCH_DIR / "people.json").write_text(json.dumps(people_out, ensure_ascii=False, indent=2), encoding="utf-8")

    print(f"[index] movies: {len(movie_rows)} / people: {len(people_map)}")

if __name__ == "__main__":
    build()
