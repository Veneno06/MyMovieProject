#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
영화 상세 캐시(docs/data/movies/**.json)를 스캔해
- search/movies.json : 기간/국가/등급 필터용 가벼운 영화 인덱스
- search/people.json : 배우/감독 이름 검색용 인덱스
를 생성한다.

파일 구조 가정:
  docs/
    data/
      movies/
        2023/
          20231234.json   # KOFIC movieInfoResult 구조
        2024/
          ...
      search/
        movies.json
        people.json
"""

from pathlib import Path
import json
import re

ROOT = Path("docs/data")
MOVIES_DIR = ROOT / "movies"
SEARCH_DIR = ROOT / "search"
SEARCH_DIR.mkdir(parents=True, exist_ok=True)

def load_json(p: Path):
    try:
        with p.open("r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return None

def digits(s: str) -> str:
    return re.sub(r"\D", "", s or "")

def norm_open_dt(s: str) -> str:
    # YYYYMMDD만 남김
    return digits(s)[:8]

def is_kr(nations) -> bool:
    # KOFIC movieInfo.nations: [{"nationNm": "한국"} ...]
    if not nations:
        return False
    joined = " ".join([str(n.get("nationNm", "")) for n in nations])
    return "한국" in joined

def first_grade(movie_info) -> str:
    audits = movie_info.get("audits") or []
    if audits and isinstance(audits, list):
        nm = (audits[0] or {}).get("watchGradeNm", "")
        return str(nm or "")
    return ""

def build_indices():
    movie_index = []
    people_map = {}  # key: peopleCd or "NM:<name>" -> {"peopleCd","peopleNm","roles":set(),"films":set()}

    json_files = sorted(MOVIES_DIR.glob("**/*.json"))
    for jf in json_files:
        data = load_json(jf)
        if not data:
            continue
        info = (data.get("movieInfoResult") or {}).get("movieInfo") or {}
        if not info:
            continue

        movieCd = info.get("movieCd")
        movieNm = info.get("movieNm") or ""
        openDt  = norm_open_dt(info.get("openDt") or "")
        grade   = first_grade(info)
        kr      = is_kr(info.get("nations"))

        # 영화 인덱스(검색/필터용, 가볍게)
        if movieCd and movieNm:
            movie_index.append({
                "movieCd": movieCd,
                "movieNm": movieNm,
                "openDt": openDt,   # YYYYMMDD
                "grade": grade,     # 관람등급(없을 수 있음)
                "isKR": kr          # 국내 여부
            })

        # 인물 인덱스(배우/감독)
        def add_person(p, role):
            if not p:
                return
            cd = str(p.get("peopleCd") or "").strip()
            nm = str(p.get("peopleNm") or "").strip()
            if not (cd or nm):
                return
            key = cd if cd else f"NM:{nm}"
            entry = people_map.setdefault(key, {
                "peopleCd": cd,
                "peopleNm": nm,
                "roles": set(),
                "films": set()
            })
            if role:
                entry["roles"].add(role)
            if movieNm:
                entry["films"].add(movieNm)

        # 감독/배우 수집
        for d in (info.get("directors") or []):
            add_person(d, "감독")
        for a in (info.get("actors") or []):
            add_person(a, "배우")

    # 정렬/가공
    movie_index.sort(key=lambda m: (m["openDt"] or "00000000", m["movieNm"]))
    # people.json은 이름 기준 정렬
    people_list = []
    for k, v in people_map.items():
        people_list.append({
            "peopleCd": v["peopleCd"],
            "peopleNm": v["peopleNm"],
            "repRoleNm": " / ".join(sorted(v["roles"])) if v["roles"] else "",
            "filmoCount": len(v["films"])
        })
    people_list.sort(key=lambda p: (p["peopleNm"] or ""))

    # 저장
    with (SEARCH_DIR/"movies.json").open("w", encoding="utf-8") as f:
        json.dump(movie_index, f, ensure_ascii=False)
    with (SEARCH_DIR/"people.json").open("w", encoding="utf-8") as f:
        json.dump(people_list, f, ensure_ascii=False)

    print(f"[index] movies: {len(movie_index)} / people: {len(people_list)}")

if __name__ == "__main__":
    build_indices()
