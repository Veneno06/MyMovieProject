# scripts/build_indices.py
# -*- coding: utf-8 -*-
import json
import time
from pathlib import Path

ROOT = Path("docs/data")
MOVIES_DIR = ROOT / "movies"
SEARCH_DIR = ROOT / "search"
PEOPLE_DIR = ROOT / "people"   # (지금은 사용 안 하지만 경로만 확보)

def load_json(p: Path):
    try:
        with p.open("r", encoding="utf-8") as f:
            return json.load(f)
    except Exception as e:
        print(f"[skip] invalid json: {p} ({e})")
        return None

def first(lst, key):
    if not isinstance(lst, list) or not lst:
        return ""
    v = lst[0].get(key, "")
    return v if v is not None else ""

def get_movie_info(raw):
    """
    상세 JSON 스키마를 유연하게 처리:
    - API 원형: {"movieInfoResult":{"movieInfo":{...}}}
    - 저장본(케이스1): {"movieInfo":{...}}
    - 저장본(케이스2): {...}   # 필드가 최상위에 바로 있는 경우
    """
    if not isinstance(raw, dict):
        return {}

    if "movieInfo" in raw and isinstance(raw["movieInfo"], dict):
        return raw["movieInfo"]

    # KOFIC 응답 원형을 그대로 저장한 경우 대비
    if "movieInfoResult" in raw and isinstance(raw["movieInfoResult"], dict):
        mi = raw["movieInfoResult"].get("movieInfo")
        if isinstance(mi, dict):
            return mi

    # 최상위에 바로 필드가 있을 수도 있음
    return raw

def rep_nation_from(m):
    # 상세에는 보통 nations: [{nationNm:"한국"}] 형태
    nations = m.get("nations") or []
    names = [ (x.get("nationNm") or "") for x in nations if isinstance(x, dict) ]
    joined = " ".join(names)
    if "한국" in joined:
        return "K"
    # 한국이 아니면 외국 처리
    if joined.strip():
        return "F"
    return ""  # 정보 없으면 빈 값

def grade_from(m):
    # 상세에는 audits: [{watchGradeNm:"..."}]
    audits = m.get("audits") or []
    g = first(audits, "watchGradeNm")
    return g or ""

def genres_from(m):
    gl = m.get("genres") or []
    out = []
    for g in gl:
        name = g.get("genreNm") if isinstance(g, dict) else ""
        if name:
            out.append(name)
    return out

def people_from(m):
    # 배우/참여진
    actors = m.get("actors") or []
    out = []
    for a in actors:
        if not isinstance(a, dict):
            continue
        out.append({
            "peopleCd": a.get("peopleCd") or "",
            "peopleNm": a.get("peopleNm") or "",
            "part":     a.get("cast") or a.get("castNm") or a.get("moviePartNm") or ""
        })
    return out

def iter_detail_files():
    # movies/ 하위의 연도/또는 기타 모든 폴더 재귀 탐색
    if not MOVIES_DIR.exists():
        return
    for p in MOVIES_DIR.rglob("*.json"):
        if p.name == ".gitkeep":
            continue
        yield p

def build_indexes():
    movies_index = []
    people_map = {}   # key: (peopleCd or peopleNm), value: dict

    total_files = 0
    used_files = 0

    for p in iter_detail_files():
        total_files += 1
        raw = load_json(p)
        if not raw:
            continue

        m = get_movie_info(raw)
        if not isinstance(m, dict):
            continue

        movieCd  = (m.get("movieCd") or "").strip()
        movieNm  = (m.get("movieNm") or "").strip()
        openDt   = (m.get("openDt") or "").replace("-", "").strip()
        prdtYear = (m.get("prdtYear") or "").strip()
        grade    = grade_from(m)
        repNat   = rep_nation_from(m)
        genres   = genres_from(m)

        if not movieCd and not movieNm:
            # 정보가 너무 부족한 레코드는 인덱스에서 제외
            continue

        movies_index.append({
            "movieCd": movieCd,
            "movieNm": movieNm,
            "openDt":  openDt,     # YYYYMMDD 또는 ""
            "prdtYear": prdtYear,
            "repNation": repNat,   # "K" | "F" | ""
            "grade": grade,        # 관람등급(문자열)
            "genres": genres,      # ["드라마","..."]
            "audiAcc": m.get("audiAcc") if isinstance(m.get("audiAcc"), (int, float)) else None
        })
        used_files += 1

        # 사람 인덱스 구축(배우 검색용)
        for person in people_from(m):
            key = (person["peopleCd"] or "").strip() or f"NM::{person['peopleNm']}"
            entry = people_map.get(key)
            if not entry:
                entry = {
                    "peopleCd": person["peopleCd"],
                    "peopleNm": person["peopleNm"],
                    "repRoleNm": "배우",
                    "films": [],
                    "filmoNames": set()
                }
                people_map[key] = entry

            entry["films"].append({
                "movieCd": movieCd,
                "movieNm": movieNm,
                "openDt":  openDt,
                "part":    person["part"],
                "audiAcc": None  # (선계산 버전이 저장돼 있으면 나중에 채울 수 있음)
            })
            if movieNm:
                entry["filmoNames"].add(movieNm)

    # 정렬: 최신 개봉일 우선
    def sort_key(m):
        od = m.get("openDt") or ""
        return int(od) if od.isdigit() else 0
    movies_index.sort(key=sort_key, reverse=True)

    # people.json 정리
    people_list = []
    for entry in people_map.values():
        entry["filmoNames"] = " | ".join(sorted(entry["filmoNames"]))  # 검색 힌트
        people_list.append(entry)

    # 저장
    SEARCH_DIR.mkdir(parents=True, exist_ok=True)
    with (SEARCH_DIR / "movies.json").open("w", encoding="utf-8") as f:
        json.dump({
            "generatedAt": int(time.time()),
            "count": len(movies_index),
            "movies": movies_index
        }, f, ensure_ascii=False, indent=2)

    with (SEARCH_DIR / "people.json").open("w", encoding="utf-8") as f:
        json.dump({
            "generatedAt": int(time.time()),
            "count": len(people_list),
            "people": people_list
        }, f, ensure_ascii=False, indent=2)

    print(f"[index] movies: {len(movies_index)} / people: {len(people_list)} / files: {used_files}/{total_files}")

if __name__ == "__main__":
    build_indexes()
