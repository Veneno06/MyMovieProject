#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import json
from collections import defaultdict
from pathlib import Path
from typing import Any, Dict, List

ROOT = Path(__file__).resolve().parents[1]
DATA = ROOT / "docs" / "data"
MOVIES = DATA / "movies"
SEARCH = DATA / "search"

def ensure_dir(p: Path) -> None:
    p.mkdir(parents=True, exist_ok=True)

def read_movie_files() -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    if not MOVIES.exists():
        return out
    for year_dir in sorted(MOVIES.iterdir()):
        if not year_dir.is_dir():
            continue
        for jf in year_dir.glob("*.json"):
            try:
                out.append(json.loads(jf.read_text(encoding="utf-8")))
            except Exception:
                pass
    return out

def normalize_movie_row(m: Dict[str, Any]) -> Dict[str, Any]:
    info = m.get("movieInfo", {}) or {}
    open_dt = (m.get("openDt") or info.get("openDt") or "").replace("-", "")
    row = {
        "movieCd": m.get("movieCd", ""),
        "movieNm": m.get("movieNm") or info.get("movieNm", ""),
        "openDt": open_dt,
        "prdtYear": m.get("prdtYear") or info.get("prdtYear", ""),
        "genres": [g.get("genreNm", "") for g in (info.get("genres") or [])],
        "repNation": "",  # 간단 표기
        "grade": "",
        "audiAcc": m.get("audiAcc", None),
    }

    # 간단한 국가/등급 추출
    alt = ",".join([c.get("nationNm","") for c in (info.get("nations") or [])])
    row["repNation"] = "한국" if "한국" in alt else (alt.split(",")[0] if alt else "")
    audits = info.get("audits") or []
    if audits and audits[0].get("watchGradeNm"):
        row["grade"] = audits[0]["watchGradeNm"]
    return row

def build_movies_index(movies: List[Dict[str, Any]]) -> Dict[str, Any]:
    rows = [normalize_movie_row(m) for m in movies]
    # 날짜 내림차순
    rows.sort(key=lambda r: int((r["openDt"] or "0").replace("-", "") or "0"), reverse=True)
    return {
        "generatedAt": int(Path("/proc/uptime").read_text().split()[0].split('.')[0]) if Path("/proc/uptime").exists() else 0,
        "count": len(rows),
        "movies": rows,
    }

def build_people_index(movies: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    key는 peopleCd 있으면 그걸 쓰고, 없으면 (peopleNm, movieNm) 조합으로 안정화.
    films[]: { movieCd, movieNm, openDt, part, audiAcc }
    """
    people_map: Dict[str, Dict[str, Any]] = {}

    def key_for(person: Dict[str, Any], movie_nm: str) -> str:
        cd = person.get("peopleCd") or ""
        nm = person.get("peopleNm") or ""
        return f"id:{cd}" if cd else f"name:{nm}|work:{movie_nm}"

    for m in movies:
        info = m.get("movieInfo", {}) or {}
        open_dt = (m.get("openDt") or info.get("openDt") or "").replace("-", "")
        movie_cd = m.get("movieCd", "")
        movie_nm = m.get("movieNm") or info.get("movieNm", "")
        part_list = info.get("actors") or []  # 배우 위주. 필요 시 directors 등 추가 가능
        acc = m.get("audiAcc", None)

        for a in part_list:
            k = key_for(a, movie_nm)
            if k not in people_map:
                people_map[k] = {
                    "peopleCd": a.get("peopleCd",""),
                    "peopleNm": a.get("peopleNm",""),
                    "repRoleNm": a.get("cast","") or a.get("castEn","") or "배우",
                    "films": []
                }
            people_map[k]["films"].append({
                "movieCd": movie_cd,
                "movieNm": movie_nm,
                "openDt": open_dt,
                "part": a.get("cast","") or a.get("castEn","") or "",
                "audiAcc": acc if isinstance(acc, int) else None,
            })

    # 각 인물의 films 최신 개봉일 순
    for v in people_map.values():
        v["films"].sort(key=lambda r: int((r["openDt"] or "0") or "0"), reverse=True)

    rows = list(people_map.values())
    rows.sort(key=lambda r: (r["peopleNm"], r["peopleCd"]))
    return {
        "generatedAt": 0,
        "count": len(rows),
        "people": rows,
    }

def main():
    ensure_dir(SEARCH)
    movies = read_movie_files()
    movies_idx = build_movies_index(movies)
    people_idx = build_people_index(movies)

    (SEARCH / "movies.json").write_text(json.dumps(movies_idx, ensure_ascii=False, indent=2), encoding="utf-8")
    (SEARCH / "people.json").write_text(json.dumps(people_idx, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"[index] movies: {movies_idx['count']} / people: {people_idx['count']}")

if __name__ == "__main__":
    main()
