#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Rebuild search indexes from local detail caches (NO API calls).

Outputs:
  - docs/data/search/movies.json
  - docs/data/search/people.json
"""

from __future__ import annotations
import os, re, sys, json, time
from pathlib import Path
from typing import Any

# ---------- Paths ----------
REPO_ROOT = Path(__file__).resolve().parents[1]
DATA_DIR  = REPO_ROOT / "docs" / "data"
DETAIL_DIR = DATA_DIR / "movies"            # detail caches live here
SEARCH_DIR = DATA_DIR / "search"
SEARCH_DIR.mkdir(parents=True, exist_ok=True)

# ---------- IO helpers ----------
def read_json(p: Path) -> Any | None:
    try:
        with p.open("r", encoding="utf-8") as f:
            return json.load(f)
    except Exception as e:
        print(f"[warn] read fail: {p} ({e})", file=sys.stderr)
        return None

def write_json(p: Path, obj: Any):
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(json.dumps(obj, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"[write] {p} ({len(obj.get('movies', obj.get('people', [])))} rows)")

# ---------- normalize ----------
def ymd8(s: str | None) -> str | None:
    if not s:
        return None
    raw = re.sub(r"[^0-9]", "", str(s))
    return raw if len(raw) == 8 else None

def primary_grade(obj: dict) -> str:
    audits = obj.get("audits") or []
    if isinstance(audits, list) and audits:
        g = (audits[0].get("watchGradeNm") or "").strip()
        if g:
            return g
    return (obj.get("grade") or "").strip()

def primary_rep_nation(obj: dict) -> str:
    nations = obj.get("nations") or []
    if isinstance(nations, list) and nations:
        nm = (nations[0].get("nationNm") or "").strip().lower()
        if nm in ("대한민국", "한국", "korea", "south korea"):
            return "K"
    return (obj.get("repNation") or "").strip()

def primary_genres(obj: dict) -> list[str]:
    gs = obj.get("genres") or []
    out: list[str] = []
    if isinstance(gs, list):
        for g in gs:
            name = (g.get("genreNm") if isinstance(g, dict) else str(g)).strip()
            if name:
                out.append(name)
    return out

# ---------- extractors ----------
def coerce_movie_obj(data: dict) -> dict:
    """Support both flattened and KOFIC-nested shapes."""
    # KOFIC original: {"movieInfoResult":{"movieInfo": {...}}}
    if "movieInfoResult" in data:
        mir = data.get("movieInfoResult") or {}
        mi  = mir.get("movieInfo") or {}
        if isinstance(mi, dict) and mi:
            return mi
    return data

def extract_movie_row(data: dict, fallback_code: str) -> dict | None:
    obj = coerce_movie_obj(data)

    movie_cd = (obj.get("movieCd") or fallback_code or "").strip()
    open_dt  = ymd8(obj.get("openDt") or obj.get("openDtStr"))
    if not movie_cd or not open_dt:
        return None

    return {
        "movieCd": movie_cd,
        "movieNm": (obj.get("movieNm") or "").strip(),
        "openDt":  open_dt,
        "prdtYear": str(obj.get("prdtYear") or "").strip(),
        "repNation": primary_rep_nation(obj),
        "grade":     primary_grade(obj),
        "genres":    primary_genres(obj),
        # keep if present; not required
        "audiAcc": obj.get("audiAcc", None),
    }

# ---------- builders ----------
def iter_detail_files() -> list[Path]:
    if not DETAIL_DIR.exists():
        print(f"[error] not found: {DETAIL_DIR}", file=sys.stderr)
        return []
    # robust: recurse all *.json under movies/
    files = [p for p in DETAIL_DIR.rglob("*.json") if p.name != ".gitkeep"]
    files.sort()
    return files

def build_movies_index() -> list[dict]:
    files = iter_detail_files()
    total = len(files)
    kept = skipped = 0
    out: list[dict] = []

    for p in files:
        data = read_json(p)
        if not data:
            skipped += 1
            continue
        row = extract_movie_row(data, fallback_code=p.stem)
        if row is None:
            skipped += 1
            continue
        out.append(row); kept += 1

    out.sort(key=lambda r: (r["openDt"], r.get("movieNm",""), r["movieCd"]))
    print(f"[index] detail files: {total}, kept: {kept}, skipped: {skipped}")
    return out

def build_people_index(detail_files: list[Path]) -> list[dict]:
    people: dict[str, dict] = {}
    for p in detail_files:
        data = read_json(p)
        if not data: 
            continue
        obj = coerce_movie_obj(data)
        movie_cd = (obj.get("movieCd") or p.stem).strip()
        movie_nm = (obj.get("movieNm") or "").strip()
        open_dt  = ymd8(obj.get("openDt"))

        for a in (obj.get("actors") or []):
            name = (a.get("peopleNm") or "").strip()
            if not name:
                continue
            rec = people.setdefault(name, {
                "peopleCd": (a.get("peopleCd") or "").strip(),
                "peopleNm": name,
                "repRoleNm": "배우",
                "films": []
            })
            rec["films"].append({
                "movieCd": movie_cd,
                "movieNm": movie_nm,
                "openDt":  open_dt or ""
            })

    out = []
    for name, info in people.items():
        films = info["films"]
        films = [f for f in films if ymd8(f.get("openDt"))] + [f for f in films if not ymd8(f.get("openDt"))]
        films.sort(key=lambda x: (x.get("openDt") or "", x.get("movieNm") or ""), reverse=True)
        info["films"] = films
        info["filmNames"] = ", ".join([f.get("movieNm") or "" for f in films[:10]])
        out.append(info)

    out.sort(key=lambda x: x["peopleNm"])
    return out

# ---------- main ----------
def main():
    detail_files = iter_detail_files()

    movies = build_movies_index()
    write_json(SEARCH_DIR / "movies.json", {
        "generatedAt": int(time.time()),
        "count": len(movies),
        "movies": movies,
    })

    people = build_people_index(detail_files)
    write_json(SEARCH_DIR / "people.json", {
        "generatedAt": int(time.time()),
        "count": len(people),
        "people": people,
    })

if __name__ == "__main__":
    main()
