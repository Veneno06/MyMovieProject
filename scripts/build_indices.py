#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os, json, time, glob, re
from datetime import datetime, timedelta
from urllib.parse import urlencode
import urllib.request

ROOT = os.path.dirname(os.path.dirname(__file__))
DOCS = os.path.join(ROOT, "docs")
DATA = os.path.join(DOCS, "data")
MOVIES_DIR = os.path.join(DATA, "movies")
SEARCH_DIR = os.path.join(DATA, "search")
YEARS_MANIFEST = os.path.join(DATA, "years", "_manifest.json")

def load_json(path):
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)

def save_json(path, obj):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=2)

def list_movie_files():
    paths = []
    # 폴더형: data/movies/YYYY/*.json
    for ydir in sorted(glob.glob(os.path.join(MOVIES_DIR, "*")), key=lambda p:p):
        if not os.path.isdir(ydir): 
            continue
        for f in glob.glob(os.path.join(ydir, "*.json")):
            paths.append(f)
    # 레거시 단일 폴더 파일(혹시 남아있으면)
    for f in glob.glob(os.path.join(MOVIES_DIR, "*.json")):
        if os.path.basename(f) != ".gitkeep":
            paths.append(f)
    return sorted(set(paths))

def pick_movie_info(js):
    # 다양성 허용
    if js.get("movieInfoResult", {}).get("movieInfo"):
        return js["movieInfoResult"]["movieInfo"]
    if js.get("movieInfo"):
        return js["movieInfo"]
    # 평면 구조 대응
    keys = ["movieCd","movieNm","openDt","prdtYear","genres","directors","actors","audits"]
    if any(k in js for k in keys):
        return js
    return None

def coalesce(*vals):
    for v in vals:
        if v not in (None, "", []):
            return v
    return None

def parse_detail(path):
    try:
        raw = load_json(path)
        info = pick_movie_info(raw)
        if not info: 
            return None
        movieCd = str(info.get("movieCd", "")).strip()
        movieNm = str(info.get("movieNm", "")).strip()
        openDt  = str(info.get("openDt", "")).strip()    # YYYYMMDD
        prdtYear= str(info.get("prdtYear", "")).strip()
        # 국가
        repNation = ""
        if info.get("nations"):
            repNation = info["nations"][0].get("nationNm", "")
        repNation = repNation or info.get("repNationNm","") or info.get("repNation","")
        # 장르
        genres = []
        if isinstance(info.get("genres"), list):
            for g in info["genres"]:
                nm = g.get("genreNm") if isinstance(g, dict) else str(g)
                if nm: genres.append(nm)
        # 등급
        grade = ""
        if isinstance(info.get("audits"), list) and info["audits"]:
            grade = info["audits"][0].get("watchGradeNm","") or info["audits"][0].get("gradeNm","")
        # 배우 (사람 인덱스용)
        actors = []
        for a in (info.get("actors") or []):
            actors.append({
                "peopleCd": a.get("peopleCd",""),
                "peopleNm": a.get("peopleNm",""),
                "cast": a.get("cast",""),
            })
        return {
            "movieCd": movieCd, "movieNm": movieNm,
            "openDt": openDt, "prdtYear": prdtYear,
            "repNation": repNation, "genres": genres,
            "grade": grade, "actors": actors
        }
    except Exception as e:
        print(f"[warn] parse fail: {path} ({e})")
        return None

# ---------- audiAcc 계산(주간 박스오피스 최대 누적) ----------
KOFIC_KEY = os.environ.get("KOFIC_API_KEY","").strip()

def ymd(d): return d.strftime("%Y%m%d")
def parse_ymd(s):
    if not s or len(s) < 8: return None
    return datetime(int(s[:4]), int(s[4:6]), int(s[6:8]))

def weekly_audiacc_max(movieCd, openDt):
    """openDt 기준 +3일부터 8~10주 훑으며 주간 리스트의 audiAcc 최대값 추출"""
    if not (KOFIC_KEY and movieCd and openDt):
        return None
    base = parse_ymd(openDt) or datetime.utcnow()
    base = base + timedelta(days=3)
    max_acc = None
    for i in range(10):
        dt = base + timedelta(days=7*i)
        q = {
            "key": KOFIC_KEY,
            "targetDt": ymd(dt),
            "weekGb": "0"
        }
        url = "https://www.kobis.or.kr/kobisopenapi/webservice/rest/boxoffice/searchWeeklyBoxOfficeList.json?" + urlencode(q)
        try:
            with urllib.request.urlopen(url, timeout=15) as r:
                js = json.loads(r.read().decode("utf-8"))
            rows = js.get("boxOfficeResult",{}).get("weeklyBoxOfficeList",[])
            hit = next((x for x in rows if str(x.get("movieCd")) == movieCd), None)
            if hit and hit.get("audiAcc"):
                acc = int(str(hit["audiAcc"]).replace(",",""))
                max_acc = acc if max_acc is None else max(max_acc, acc)
        except Exception as e:
            # 쿼터/네트워크 오류는 스킵하고 다음 주 시도
            pass
        time.sleep(0.12)  # 속도 제한
    return max_acc

def should_calc_recent(openDt, days):
    if not openDt: return False
    d = parse_ymd(openDt)
    if not d: return False
    return d >= (datetime.utcnow() - timedelta(days=days))

def build_indexes(audiacc_mode="off", audiacc_days=365):
    files = list_movie_files()
    print(f"[index] scanning details files: {len(files)}")
    movies = []
    people_map = {}  # peopleCd(or name) -> entry

    for idx, path in enumerate(files, 1):
        row = parse_detail(path)
        if not row or not row["movieCd"]:
            continue

        # 사람 인덱스(배우 중심)
        for a in row["actors"]:
            key = a.get("peopleCd") or ("nm:" + a.get("peopleNm",""))
            if not key: 
                continue
            ent = people_map.get(key)
            if not ent:
                people_map[key] = ent = {
                    "peopleCd": a.get("peopleCd",""),
                    "peopleNm": a.get("peopleNm",""),
                    "repRoleNm": "배우",
                    "films": []
                }
            ent["films"].append({
                "movieCd": row["movieCd"],
                "movieNm": row["movieNm"],
                "openDt": row["openDt"],
                "part": a.get("cast","")
            })

        item = {
            "movieCd": row["movieCd"],
            "movieNm": row["movieNm"],
            "openDt": row["openDt"],
            "prdtYear": row["prdtYear"],
            "repNation": row["repNation"],
            "genres": row["genres"],
            "grade": row["grade"],
            "audiAcc": None
        }

        # audiAcc 선계산
        if audiacc_mode == "all":
            item["audiAcc"] = weekly_audiacc_max(item["movieCd"], item["openDt"])
        elif audiacc_mode == "recent":
            if should_calc_recent(item["openDt"], audiacc_days):
                item["audiAcc"] = weekly_audiacc_max(item["movieCd"], item["openDt"])
        # off면 그냥 None 유지

        movies.append(item)

        if idx % 200 == 0:
            print(f"[index] progress {idx}/{len(files)}")

    # movies.json
    save_json(os.path.join(SEARCH_DIR, "movies.json"), {
        "generatedAt": int(time.time()),
        "count": len(movies),
        "movies": movies
    })
    print(f"[index] movies: {len(movies)}")

    # people.json (간단 생성)
    people = []
    for ent in people_map.values():
        ent["filmoNames"] = " | ".join([f["movieNm"] for f in ent["films"][:6]])
        people.append(ent)
    save_json(os.path.join(SEARCH_DIR, "people.json"), {
        "generatedAt": int(time.time()),
        "count": len(people),
        "people": people
    })
    print(f"[index] people: {len(people)}")

if __name__ == "__main__":
    import argparse
    p = argparse.ArgumentParser()
    p.add_argument("--audiacc-mode", choices=["off","recent","all"], default="off")
    p.add_argument("--audiacc-days", type=int, default=365)
    args = p.parse_args()
    print(f"[args] audiacc-mode={args.audiacc_mode} days={args.audiacc_days}")
    build_indexes(audiacc_mode=args.audiacc_mode, audiacc_days=args.audiacc_days)
