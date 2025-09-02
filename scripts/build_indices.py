#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Rebuild search indexes from local detail caches (no API calls).

Outputs:
  - docs/data/search/movies.json
  - docs/data/search/people.json
"""

import os, json, re, time, sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1] / "docs" / "data"
DETAIL_DIR = ROOT / "movies"
SEARCH_DIR = ROOT / "search"
SEARCH_DIR.mkdir(parents=True, exist_ok=True)

def read_json(path: Path):
    try:
        with path.open("r", encoding="utf-8") as f:
            return json.load(f)
    except Exception as e:
        # print(f"[warn] JSON read fail: {path} ({e})", file=sys.stderr)
        return None

def ymd8(s: str) -> str | None:
    if not s:
        return None
    raw = re.sub(r"[^0-9]", "", str(s))
    return raw if len(raw) == 8 else None

def to_grade(obj) -> str:
    # audits: [{watchGradeNm: "12세이상관람가"}]
    audits = obj.get("audits") or []
    if isinstance(audits, list) and audits:
        g = audits[0].get("watchGradeNm", "")
        return g or ""
    # 혹시 기존 캐시에 바로 grade 필드가 있다면
    return obj.get("grade", "") or ""

def to_rep_nation(obj) -> str:
    # nations: [{nationNm:"한국"}, ...]
    nations = obj.get("nations") or []
    label = ""
    if isinstance(nations, list) and nations:
        nm = (nations[0].get("nationNm") or "").strip()
        label = "K" if ("한국" in nm or nm == "대한민국" or nm.lower() in ("korea","south korea")) else "F"
    # 캐시에 repNation 이미 있으면 우선
    return (obj.get("repNation") or label or "").strip()

def to_genres(obj) -> list[str]:
    gs = obj.get("genres") or []
    out = []
    if isinstance(gs, list):
        for g in gs:
            if isinstance(g, dict):
                name = g.get("genreNm") or g.get("name") or ""
            else:
                name = str(g)
            name = name.strip()
            if name:
                out.append(name)
    return out

def extract_movie_row(obj: dict, fallback_code: str) -> dict | None:
    """
    필수: movieCd, openDt(YYYYMMDD).
    openDt가 없거나 포맷이 틀리면 None 반환(=스킵).
    """
    movie_cd = (obj.get("movieCd") or fallback_code or "").strip()
    open_dt = ymd8(obj.get("openDt") or obj.get("openDtStr") or "")
    if not movie_cd or not open_dt:
        return None

    row = {
        "movieCd": movie_cd,
        "movieNm": (obj.get("movieNm") or "").strip(),
        "openDt": open_dt,
        "prdtYear": str(obj.get("prdtYear") or "").strip(),
        "repNation": to_rep_nation(obj),
        "grade": to_grade(obj),
        "genres": to_genres(obj),
        # 인덱스에서는 audiAcc는 선택: 상세 페이지에서 별도로 표기 가능
        "audiAcc": obj.get("audiAcc", None)
    }
    return row

def build_movies_index() -> list[dict]:
    total_files = 0
    kept = 0
    skipped = 0
    out = []

    if not DETAIL_DIR.exists():
        print(f"[error] detail dir not found: {DETAIL_DIR}", file=sys.stderr)
        return out

    for ydir in sorted(DETAIL_DIR.iterdir()):
        if not ydir.is_dir():
            continue
        if not ydir.name.isdigit():  # 2016, 2023, 2024, 2025 ...
            continue
        for f in sorted(ydir.glob("*.json")):
            if f.name == ".gitkeep":
                continue
            total_files += 1
            data = read_json(f)
            if not data:
                skipped += 1
                continue
            row = extract_movie_row(data, fallback_code=f.stem)
            if row is None:
                skipped += 1
                continue
            out.append(row)
            kept += 1

    out.sort(key=lambda r: (r["openDt"], r.get("movieNm",""), r["movieCd"]))
    print(f"[index] scanned files: {total_files}, kept: {kept}, skipped: {skipped}")
    return out

def build_people_index(detail_files: list[Path]) -> list[dict]:
    """
    간단한 배우 인덱스: 이름 기준으로 필모 목록 구축.
    사람이 5~6천명 수준이면 충분히 가볍습니다.
    """
    people_map: dict[str, dict] = {}

    for f in detail_files:
        data = read_json(f)
        if not data: 
            continue
        movie_cd = (data.get("movieCd") or f.stem or "").strip()
        movie_nm = (data.get("movieNm") or "").strip()
        open_dt = ymd8(data.get("openDt") or "")

        # 배우 목록
        actors = data.get("actors") or []
        for a in actors:
            name = (a.get("peopleNm") or "").strip()
            if not name:
                continue
            entry = people_map.setdefault(name, {
                "peopleCd": "",          # KOFIC peopleCd가 없는 케이스도 있어 빈값 허용
                "peopleNm": name,
                "repRoleNm": "배우",
                "films": []
            })
            entry["films"].append({
                "movieCd": movie_cd,
                "movieNm": movie_nm,
                "openDt": open_dt or ""
            })

    # 보기 좋게 정렬 및 필모  최신일자 우선
    out = []
    for name, info in people_map.items():
        films = info["films"]
        films = [f for f in films if ymd8(f.get("openDt"))] + [f for f in films if not ymd8(f.get("openDt"))]
        films.sort(key=lambda x: (x.get("openDt") or "", x.get("movieNm") or ""), reverse=True)
        info["films"] = films
        info["filmNames"] = ", ".join([f.get("movieNm") or "" for f in films[:10]])
        out.append(info)

    out.sort(key=lambda x: x["peopleNm"])
    return out

def main():
    # 영화 인덱스
    movies = build_movies_index()
    movies_doc = {
        "generatedAt": int(time.time()),
        "count": len(movies),
        "movies": movies
    }
    (SEARCH_DIR / "movies.json").write_text(
        json.dumps(movies_doc, ensure_ascii=False, indent=2),
        encoding="utf-8"
    )
    print(f"[write] {SEARCH_DIR / 'movies.json'}  ({len(movies)} rows)")

    # people 인덱스도 함께(선택)
    detail_files = []
    for ydir in sorted(DETAIL_DIR.iterdir()):
        if ydir.is_dir() and ydir.name.isdigit():
            detail_files += sorted(ydir.glob("*.json"))
    people = build_people_index(detail_files)
    people_doc = {
        "generatedAt": int(time.time()),
        "count": len(people),
        "people": people
    }
    (SEARCH_DIR / "people.json").write_text(
        json.dumps(people_doc, ensure_ascii=False, indent=2),
        encoding="utf-8"
    )
    print(f"[write] {SEARCH_DIR / 'people.json'}  ({len(people)} rows)")

if __name__ == "__main__":
    main()
