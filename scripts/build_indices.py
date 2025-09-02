#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
docs/data/movies/<year>/*.json 을 모아
- docs/data/search/movies.json
- docs/data/search/people.json
두 개의 인덱스를 생성한다.
※ API 호출 없음 (로컬 캐시만 사용)
"""

import json, os, glob, time, re
from collections import defaultdict

ROOT = os.path.dirname(os.path.dirname(__file__))  # repo/scripts -> repo
DOCS = os.path.join(ROOT, "docs")
MOVIE_DIR = os.path.join(DOCS, "data", "movies")
SEARCH_DIR = os.path.join(DOCS, "data", "search")
PEOPLE_DIR = os.path.join(DOCS, "data", "people")

os.makedirs(SEARCH_DIR, exist_ok=True)

def _read_json(path):
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return None

def _save_json(path, obj):
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=2)
    os.replace(tmp, path)

def _to_ymd(s):
    # 'YYYYMMDD' -> 'YYYY-MM-DD'
    if not s or not isinstance(s, str) or len(s) != 8 or not s.isdigit():
        return ""
    return f"{s[0:4]}-{s[4:6]}-{s[6:8]}"

def _rep_nation(detail):
    # 국내 여부 판단: nations[], repNationNm 등에서 '한국/대한민국/Korea' 포함되면 'K'
    texts = []
    if isinstance(detail, dict):
        rn = detail.get("repNationNm") or detail.get("repNation") or ""
        if rn: texts.append(str(rn))
        nat = detail.get("nations")
        if isinstance(nat, list):
            for n in nat:
                nm = (n or {}).get("nationNm") or ""
                if nm: texts.append(str(nm))
        natAlt = detail.get("nationAlt") or ""
        if natAlt: texts.append(str(natAlt))

    joined = " ".join(texts).lower()
    if any(tok in joined for tok in ["한국", "대한민국", "korea"]):
        return "K"
    return "F"

def _grade(detail):
    # 관람등급 텍스트 보존
    for key in ("watchGradeName", "watchGradeNm", "grade", "audit"):
        g = (detail or {}).get(key)
        if g: return str(g)
    # movieInfoResult.movieInfo.audits[0].watchGradeNm 형태도 있을 수 있음
    audits = (detail or {}).get("audits")
    if isinstance(audits, list) and audits:
        g = (audits[0] or {}).get("watchGradeNm")
        if g: return str(g)
    return ""

def _genres(detail):
    # 장르 리스트 텍스트 배열
    out = []
    for key in ("genres", "genreList"):
        arr = (detail or {}).get(key)
        if isinstance(arr, list):
            for it in arr:
                name = it.get("genreNm") if isinstance(it, dict) else it
                if name:
                    out.append(str(name))
    return out

def _actors(detail):
    # KOFIC 상세엔 actors [{peopleNm, peopleCd, cast, castEn}] 등
    arr = (detail or {}).get("actors")
    if not isinstance(arr, list):
        return []
    out = []
    for a in arr:
        if not isinstance(a, dict): 
            continue
        out.append({
            "peopleCd": str(a.get("peopleCd") or ""),
            "peopleNm": str(a.get("peopleNm") or ""),
            "repRoleNm": "배우",
            "part": str(a.get("cast") or ""),
        })
    return out

def _directors(detail):
    arr = (detail or {}).get("directors")
    if not isinstance(arr, list):
        return []
    out = []
    for d in arr:
        if not isinstance(d, dict): 
            continue
        out.append({
            "peopleCd": str(d.get("peopleCd") or ""),
            "peopleNm": str(d.get("peopleNm") or ""),
            "repRoleNm": "감독",
            "part": "",
        })
    return out

def collect_details():
    files = []
    # 하위 폴더(연도) 전체 순회
    for year_dir in sorted(glob.glob(os.path.join(MOVIE_DIR, "*"))):
        if not os.path.isdir(year_dir): 
            continue
        for jf in glob.glob(os.path.join(year_dir, "*.json")):
            files.append(jf)

    details = []
    for p in files:
        j = _read_json(p)
        if not j: 
            continue
        # j 형태: build_movie_details.py가 저장한 1편 상세
        details.append(j)
    return details

def build_movies_index(details):
    rows = []
    for d in details:
        movieCd = str(d.get("movieCd") or "")
        movieNm = str(d.get("movieNm") or "")
        openDt_raw = str(d.get("openDt") or "")
        prdtYear = str(d.get("prdtYear") or "")
        repNat = _rep_nation(d)
        grade = _grade(d)
        gens = _genres(d)
        # audiAcc는 상세에 있으면 사용, 없으면 null
        audiAcc = d.get("audiAcc", None)
        rows.append({
            "movieCd": movieCd,
            "movieNm": movieNm,
            "openDt": openDt_raw,
            "prdtYear": prdtYear,
            "repNation": repNat,         # "K" or "F"
            "grade": grade,
            "genres": gens,
            "audiAcc": audiAcc if isinstance(audiAcc, int) else None,
        })

    # 개봉일이 있는 경우 우선 정렬(최신이 아래쪽에 와도 상관없음)
    def sort_key(x):
        od = x.get("openDt") or ""
        return od if (len(od) == 8 and od.isdigit()) else "99999999"
    rows.sort(key=sort_key)

    result = {
        "generatedAt": int(time.time()),
        "count": len(rows),
        "movies": rows
    }
    _save_json(os.path.join(SEARCH_DIR, "movies.json"), result)
    print(f"[write] movies.json ({len(rows)} rows)")

def build_people_index(details):
    # 이름/코드별로 필모를 모은다.
    bucket = defaultdict(lambda: {"peopleCd": "", "peopleNm": "", "repRoleNm": "", "films": []})

    for d in details:
        movieCd = str(d.get("movieCd") or "")
        movieNm = str(d.get("movieNm") or "")
        openDt = str(d.get("openDt") or "")
        # 배우 + 감독
        for person in (_actors(d) + _directors(d)):
            key = (person["peopleCd"] or f"NM::{person['peopleNm']}")
            if not bucket[key]["peopleNm"]:
                bucket[key]["peopleNm"] = person["peopleNm"]
                bucket[key]["peopleCd"] = person["peopleCd"]
                bucket[key]["repRoleNm"] = person["repRoleNm"]  # 최근 역할 하나 대표
            # 중복 작품 방지
            if movieCd and all(f.get("movieCd") != movieCd for f in bucket[key]["films"]):
                bucket[key]["films"].append({
                    "movieCd": movieCd,
                    "movieNm": movieNm,
                    "openDt": openDt,
                    "part": person.get("part","")
                })

    rows = []
    for key, v in bucket.items():
        # 최신 개봉일 순으로 보여주기 위해 정렬
        v["films"].sort(key=lambda f: f.get("openDt") or "", reverse=True)
        rows.append({
            "peopleCd": v["peopleCd"],
            "peopleNm": v["peopleNm"],
            "repRoleNm": v["repRoleNm"],
            "films": v["films"],
            "filmNames": " / ".join([f["movieNm"] for f in v["films"][:5]])
        })

    rows.sort(key=lambda x: (x["peopleNm"], x["peopleCd"] or ""))
    result = {
        "generatedAt": int(time.time()),
        "count": len(rows),
        "people": rows
    }
    _save_json(os.path.join(SEARCH_DIR, "people.json"), result)
    print(f"[write] people.json ({len(rows)} rows)")

def main():
    details = collect_details()
    print(f"[index] detail files: kept: {len(details)}")
    build_movies_index(details)
    build_people_index(details)

if __name__ == "__main__":
    main()
