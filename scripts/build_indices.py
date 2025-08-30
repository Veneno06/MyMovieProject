#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
movies/ (연도별 하위 폴더 포함)를 재귀로 스캔해서
- search/movies.json  : 간단한 영화 인덱스
- search/people.json  : 간단한 사람 인덱스
를 생성합니다.
"""

import json, os
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DOCS = ROOT / "docs" / "data"
MOVIES = DOCS / "movies"
SEARCH = DOCS / "search"
PEOPLE_DIR = DOCS / "people"

def ensure_dir(p: Path):
    p.mkdir(parents=True, exist_ok=True)

def load_json(p: Path):
    with p.open("r", encoding="utf-8") as f:
        return json.load(f)

def save_json(p: Path, data):
    ensure_dir(p.parent)
    with p.open("w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

def scan_movies():
    out_movies = []
    people_map = {}  # peopleCd -> {"peopleNm": str, "filmo": set()}
    # movies/<year>/*.json 재귀 스캔
    for p in MOVIES.rglob("*.json"):
        if p.name == ".gitkeep":
            continue
        rel = p.relative_to(DOCS)  # data/부터의 상대경로
        try:
            js = load_json(p)
        except Exception:
            continue
        info = (js.get("movieInfoResult") or {}).get("movieInfo") or {}
        cd   = str(info.get("movieCd","")).strip()
        nm   = str(info.get("movieNm","")).strip()
        open_dt = str(info.get("openDt","")).replace("-","")
        # movies.json 항목
        out_movies.append({
            "movieCd": cd,
            "movieNm": nm,
            "openDt": open_dt,
            "url": f"/{rel.as_posix()}"  # /data/movies/2024/123.json
        })
        # people.json(간단)
        for a in info.get("actors", []) or []:
            pc = str(a.get("peopleCd","")).strip()
            pn = str(a.get("peopleNm","")).strip()
            if not pc:
                continue
            if pc not in people_map:
                people_map[pc] = {"peopleNm": pn, "filmo": set()}
            if nm:
                people_map[pc]["filmo"].add(nm)

    # 정렬/정리
    out_movies.sort(key=lambda x: (x["openDt"] or "00000000", x["movieCd"]), reverse=True)
    out_people = []
    for pc, v in people_map.items():
        out_people.append({
            "peopleCd": pc,
            "peopleNm": v["peopleNm"],
            "filmo": sorted(list(v["filmo"]))[:12]
        })
    out_people.sort(key=lambda x: (x["peopleNm"], x["peopleCd"]))

    save_json(SEARCH / "movies.json", out_movies)
    save_json(SEARCH / "people.json", out_people)
    # 보조: people/unknown.json 없으면 만들어두기
    unknown = PEOPLE_DIR / "unknown.json"
    if not unknown.exists():
        save_json(unknown, {"ok": True, "note": "fallback"})
    print(f"[index] movies: {len(out_movies)} / people: {len(out_people)}")

if __name__ == "__main__":
    scan_movies()
