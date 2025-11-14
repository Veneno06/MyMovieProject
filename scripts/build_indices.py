#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os, json, sys
from datetime import datetime, timezone

ROOT = os.path.dirname(os.path.dirname(__file__))
DOCS_DIR = os.path.join(ROOT, "docs", "data")
MOVIE_DIR = os.path.join(DOCS_DIR, "movies")
SEARCH_DIR = os.path.join(DOCS_DIR, "search")
os.makedirs(SEARCH_DIR, exist_ok=True)

def load_json(path):
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return None

def norm_open(dt):
    """openDt를 YYYY-MM-DD 형식으로 정규화"""
    if not dt:
        return ""
    s = str(dt).strip().replace(".", "").replace("-", "")
    return f"{s[0:4]}-{s[4:6]}-{s[6:8]}" if len(s) == 8 and s.isdigit() else ""

def is_korean(nm):
    nm = (nm or "").strip()
    return ("한국" in nm) or ("대한민국" in nm)

def first_or_empty(arr, key):
    if isinstance(arr, list) and arr:
        v = arr[0].get(key, "")
        return v if isinstance(v, str) else ""
    return ""

def scan_detail_files():
    files = []
    for root, _, names in os.walk(MOVIE_DIR):
        for n in names:
            if n.endswith(".json"):
                files.append(os.path.join(root, n))
    files.sort()
    return files

def get_movie_info_from_data(data):
    """KOFIC 형태/flat 형태 모두에서 movieInfo dict를 추출"""
    if not isinstance(data, dict):
        return None
    if "movieInfoResult" in data and "movieInfo" in data["movieInfoResult"]:
        return data["movieInfoResult"]["movieInfo"]
    if "movieCd" in data:
        return data
    return None

def add_appearance(buckets, name, role, movieCd, movieNm, openDt, audiAcc, actorCount, peopleCd, cast):
    """
    name+role 단위로 원본 출연 정보(영화 코드, 개봉일, peopleCd 등)를 전부 모아둠.
    나중에 여기서 동명이인을 시간/peopleCd 기준으로 분리한다.
    """
    name = (name or "").strip()
    role = (role or "").strip()
    if not name:
        return
    key = (name, role)
    arr = buckets.setdefault(key, [])
    arr.append({
        "movieCd": movieCd,
        "movieNm": movieNm,
        "openDt": openDt,           # YYYY-MM-DD 또는 ""
        "audiAcc": audiAcc,
        "actorCount": actorCount,
        "peopleCd": (peopleCd or "").strip(),
        "part": (cast or "").strip(),
    })

def year_from_open(openDt):
    """YYYY-MM-DD 또는 YYYYMMDD에서 연도(int)만 추출, 실패 시 0"""
    if not openDt:
        return 0
    try:
        return int(str(openDt)[:4])
    except Exception:
        return 0

def cluster_without_cd(apps, gap_year=10):
    """
    peopleCd가 전혀 없는 동명이인 후보들을 시간 축으로 여러 클러스터로 나눕니다.

    - openDt 기준으로 정렬 후, 연속 영화들 사이의 간격이 gap_year(기본 10년)보다 크고
      앞쪽 묶음에 최소 2편 이상 있을 때 "사람이 끊긴다"고 보고 새 클러스터 시작.
    - 전체 연도 범위가 너무 좁으면 굳이 나누지 않고 하나의 클러스터로 유지.
    """
    if not apps:
        return []

    # openDt 기준으로 정렬
    apps_sorted = sorted(apps, key=lambda a: a.get("openDt") or "9999-99-99")

    clusters = []
    current = []
    prev_year = None

    for app in apps_sorted:
        y = year_from_open(app.get("openDt"))
        if prev_year is None:
            current.append(app)
        else:
            # 이전 영화와 gap_year보다 큰 공백이 있고, 지금까지 모인 게 2편 이상이면
            # "세대가 끊겼다"고 보고 사람을 분리
            if y and prev_year and (y - prev_year) > gap_year and len(current) >= 2:
                clusters.append({"peopleCd": "", "apps": current})
                current = [app]
            else:
                current.append(app)
        if y:
            prev_year = y

    if current:
        clusters.append({"peopleCd": "", "apps": current})

    # 전체 연도 범위가 좁으면 나누지 않는다 (예: 10년 이하)
    years = [year_from_open(a.get("openDt")) for a in apps if year_from_open(a.get("openDt"))]
    if len(clusters) > 1 and years:
        if max(years) - min(years) <= gap_year:
            return [{"peopleCd": "", "apps": apps_sorted}]
    return clusters

def build_people_from_buckets(buckets):
    """
    scan 과정에서 name+role 단위로 모아둔 buckets를 기반으로
    최종 people.json에 들어갈 people 리스트를 만든다.
    """
    people_map = {}

    for (name, role), apps in buckets.items():
        coded   = [a for a in apps if a.get("peopleCd")]
        uncoded = [a for a in apps if not a.get("peopleCd")]

        clusters = []

        if coded:
            # 1) peopleCd가 있는 경우 -> 각 peopleCd별로 먼저 그룹화
            by_cd = {}
            for a in coded:
                by_cd.setdefault(a["peopleCd"], []).append(a)
            for cd, group in by_cd.items():
                clusters.append({"peopleCd": cd, "apps": group})

            # 2) peopleCd가 없는 항목을 기존 클러스터에 최대한 배정
            if uncoded:
                # 각 클러스터별 연도 범위 계산
                ranges = []
                for idx, c in enumerate(clusters):
                    years = [year_from_open(a.get("openDt")) for a in c["apps"] if year_from_open(a.get("openDt"))]
                    if years:
                        ranges.append((idx, min(years), max(years)))

                for a in uncoded:
                    y = year_from_open(a.get("openDt"))
                    if not y or not ranges:
                        # 연도 정보가 없거나 기준이 없으면 별도 클러스터로 둔다.
                        clusters.append({"peopleCd": "", "apps": [a]})
                        continue

                    best_idx = ranges[0][0]
                    best_dist = 10**9
                    for idx, ymin, ymax in ranges:
                        if ymin <= y <= ymax:
                            dist = 0
                        elif y < ymin:
                            dist = ymin - y
                        else:
                            dist = y - ymax
                        if dist < best_dist:
                            best_dist = dist
                            best_idx = idx
                    clusters[best_idx]["apps"].append(a)
        else:
            # peopleCd 정보가 전혀 없는 동명이인 후보들: 시간 기반 클러스터링
            clusters = cluster_without_cd(apps)

        # === 여기서 클러스터별로 최종 person 레코드를 생성 ===
        base_key = f"name::{name}::{role}"

        # 안정적인 순서를 위해 클러스터를 첫 개봉일 기준으로 정렬
        def earliest_open(c):
            dates = [a.get("openDt") or "" for a in c["apps"] if a.get("openDt")]
            return min(dates) if dates else "9999-99-99"

        clusters.sort(key=earliest_open)

        for idx, c in enumerate(clusters, start=1):
            cd = c.get("peopleCd") or ""
            if cd:
                # peopleCd가 있는 경우 personKey는 peopleCd 그대로 사용
                person_key = cd
            else:
                # peopleCd가 없는데 클러스터가 여러 개인 경우 suffix(#1,#2,...)로 동명이인 분리
                person_key = base_key if len(clusters) == 1 else f"{base_key}#{idx}"

            rec = people_map.get(person_key)
            if not rec:
                rec = {
                    "personKey": person_key,
                    "peopleCd": cd,
                    "peopleNm": name,
                    "repRoleNm": role,
                    "films": [],
                }
                people_map[person_key] = rec

            # 중복 영화코드 방지
            existing_cds = {f["movieCd"] for f in rec["films"]}
            for a in c["apps"]:
                if a["movieCd"] in existing_cds:
                    continue
                rec["films"].append({
                    "movieCd": a["movieCd"],
                    "movieNm": a["movieNm"],
                    "openDt": a["openDt"],
                    "part": a.get("part", ""),
                    "audiAcc": a["audiAcc"],
                    "actorCount": a["actorCount"],
                })
                existing_cds.add(a["movieCd"])

    # 각 인물 레코드 안에서 영화들을 최신 개봉일 순으로 정렬
    for rec in people_map.values():
        rec["films"].sort(key=lambda f: f.get("openDt") or "9999-99-99", reverse=True)

    return list(people_map.values())

def main():
    files = scan_detail_files()
    print(f"[scan] detail files: {len(files)}")

    movies = []
    # (peopleNm, role) -> list[appearance]
    name_role_buckets = {}

    processed_movie_cds = set()

    for fp in files:
        d = load_json(fp)
        mi = get_movie_info_from_data(d)
        if not mi:
            continue

        movieCd = (mi.get("movieCd") or "").strip()
        if not movieCd:
            continue

        # 영화 중복 방지
        if movieCd in processed_movie_cds:
            continue
        processed_movie_cds.add(movieCd)

        movieNm = (mi.get("movieNm") or "").strip()
        openDt  = norm_open(mi.get("openDt", ""))
        prdtYear = str(mi.get("prdtYear", "")).strip()
        nations = mi.get("nations") or []
        repNation = "K" if any(is_korean(x.get("nationNm")) for x in nations) else "F"
        grade = first_or_empty(mi.get("audits") or [], "watchGradeNm")
        genres = [
            (g.get("genreNm") or "").strip()
            for g in (mi.get("genres") or [])
            if (g.get("genreNm") or "").strip()
        ]
        audiAcc = mi.get("audiAcc")
        actorCount = len(mi.get("actors", []))

        if not movieCd or not movieNm:
            continue

        # movies.json에 들어갈 정보
        movies.append({
            "movieCd": movieCd,
            "movieNm": movieNm,
            "openDt": openDt,
            "prdtYear": prdtYear,
            "repNation": repNation,
            "grade": grade,
            "genres": genres,
            "audiAcc": audiAcc,
        })

        # === 동명이인 분리를 위해 감독/배우 출연 정보 원자료 수집 ===
        for x in (mi.get("directors") or []):
            add_appearance(
                name_role_buckets,
                x.get("peopleNm"),
                "감독",
                movieCd,
                movieNm,
                openDt,
                audiAcc,
                actorCount,
                x.get("peopleCd"),
                None,
            )
        for x in (mi.get("actors") or []):
            add_appearance(
                name_role_buckets,
                x.get("peopleNm"),
                "배우",
                movieCd,
                movieNm,
                openDt,
                audiAcc,
                actorCount,
                x.get("peopleCd"),
                x.get("cast"),
            )

    movies.sort(key=lambda m: m.get("openDt") or "9999-99-99")
    people = build_people_from_buckets(name_role_buckets)

    print(f"[index] movies: {len(movies)} / people: {len(people)}")

    now_ts = int(datetime.now(timezone.utc).timestamp())
    out_movies = {"generatedAt": now_ts, "count": len(movies), "movies": movies}
    out_people = {"generatedAt": now_ts, "count": len(people), "people": people}

    with open(os.path.join(SEARCH_DIR, "movies.json"), "w", encoding="utf-8") as f:
        json.dump(out_movies, f, ensure_ascii=False, indent=2)
    with open(os.path.join(SEARCH_DIR, "people.json"), "w", encoding="utf-8") as f:
        json.dump(out_people, f, ensure_ascii=False, indent=2)

    print("[write] search indexes saved.")

if __name__ == "__main__":
    main()
