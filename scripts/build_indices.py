#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
build_indices.py (확장 버전)
- docs/data/movies/**.json 을 읽어서
  1) movies.json  : 영화 인덱스 (movieCd, movieNm, openDt)
  2) people.json  : 사람 인덱스 (peopleCd, peopleNm, repRoleNm, films[], filmoNames)
를 생성한다.
"""

import json
import os
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]          # repo root
DATA_DIR = ROOT / "docs" / "data"
MOVIES_DIR = DATA_DIR / "movies"
SEARCH_DIR = DATA_DIR / "search"
SEARCH_DIR.mkdir(parents=True, exist_ok=True)

def read_json(p: Path):
    try:
        with p.open("r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return None

def to_ymd(s: str) -> str:
    """YYYYMMDD 정규화 (없으면 '')"""
    if not s:
        return ""
    s = str(s).replace("-", "")
    return s if len(s) == 8 and s.isdigit() else ""

def norm(s: str) -> str:
    return (s or "").lower().replace(" ", "").strip()

def main():
    # ─────────────────────────────
    # 1) 영화 상세 캐시 훑기
    # ─────────────────────────────
    movie_index = []   # [{movieCd, movieNm, openDt}]
    people_map = {}    # key: peopleCd or "name::<name>" → {peopleCd, peopleNm, repRoleNm, films[], filmoNames}

    # movies 디렉토리 구조(연도/파일) 지원
    if not MOVIES_DIR.exists():
        print("[index] movies dir not found:", MOVIES_DIR)
        return

    movie_files = list(MOVIES_DIR.rglob("*.json"))
    # .gitkeep 등 무시
    movie_files = [p for p in movie_files if p.name != ".gitkeep"]

    for p in movie_files:
        js = read_json(p)
        if not js:
            continue

        info = js.get("movieInfoResult", {}).get("movieInfo", {})
        movieCd = info.get("movieCd") or ""
        movieNm = info.get("movieNm") or ""
        openDt  = to_ymd(info.get("openDt") or "")
        if not movieCd or not movieNm:
            continue

        # 영화 인덱스 축적
        movie_index.append({
            "movieCd": movieCd,
            "movieNm": movieNm,
            "openDt": openDt
        })

        # 배우 목록 → 사람 인덱스 확장
        for a in info.get("actors", []):
            peopleCd = a.get("peopleCd") or ""
            peopleNm = a.get("peopleNm") or ""
            part     = a.get("cast") or ""  # 역할(주연/조연/배역명 등)

            if not peopleNm:  # 이름조차 없으면 스킵
                continue

            key = peopleCd if peopleCd else f"name::{peopleNm}"

            if key not in people_map:
                people_map[key] = {
                    "peopleCd": peopleCd,                 # 없을 수도 있음(빈 문자열)
                    "peopleNm": peopleNm,
                    "repRoleNm": "배우",                  # 기본값
                    "films": [],                          # [{movieCd, movieNm, openDt, part}]
                }

            entry = people_map[key]
            # 코드가 빈 상태였는데, 이번에 코드가 있으면 승격
            if not entry.get("peopleCd") and peopleCd:
                entry["peopleCd"] = peopleCd

            entry["films"].append({
                "movieCd": movieCd,
                "movieNm": movieNm,
                "openDt": openDt,
                "part": part
            })

    # ─────────────────────────────
    # 2) 후처리: 사람 인덱스 정렬/요약필드
    # ─────────────────────────────
    people_list = list(people_map.values())
    for p in people_list:
        # 영화 최근작 우선 정렬
        p["films"].sort(key=lambda x: int(x.get("openDt") or 0), reverse=True)
        # 요약 filmoNames (최대 몇 개만)
        titles = [f["movieNm"] for f in p["films"] if f.get("movieNm")]
        if titles:
            p["filmoNames"] = " | ".join(titles[:10])
        else:
            p["filmoNames"] = ""

    # 이름 기준 정렬(가나다)
    people_list.sort(key=lambda x: norm(x.get("peopleNm")))

    # ─────────────────────────────
    # 3) 파일로 저장
    # ─────────────────────────────
    out_movies = {
        "generatedAt": int(time.time()),
        "count": len(movie_index),
        "movies": movie_index
    }
    out_people = {
        "generatedAt": int(time.time()),
        "count": len(people_list),
        "people": people_list
    }

    with (SEARCH_DIR / "movies.json").open("w", encoding="utf-8") as f:
        json.dump(out_movies, f, ensure_ascii=False, indent=2)
    with (SEARCH_DIR / "people.json").open("w", encoding="utf-8") as f:
        json.dump(out_people, f, ensure_ascii=False, indent=2)

    print(f"[index] movies: {len(movie_index)} / people: {len(people_list)}")
    print(f"[index] wrote {SEARCH_DIR/'movies.json'}")
    print(f"[index] wrote {SEARCH_DIR/'people.json'}")

if __name__ == "__main__":
    main()
