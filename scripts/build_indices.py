# scripts/build_indices.py
# - docs/data/movies/ 이하 연도 폴더까지 재귀적으로 스캔
# - search/movies.json, search/people.json 생성
# - 국내/국외(repNation) 보정, 등급/장르/개봉일 정규화
# - (옵션) audiAcc 선계산 recent/all 모드 지원 (KOFIC 호출은 다른 워크플로로 분리 권장)

import json, os, sys, time, glob
from pathlib import Path
from collections import defaultdict

ROOT = Path(__file__).resolve().parents[1]        # repo root
DETAIL_DIR = ROOT / "docs" / "data" / "movies"    # 영화 상세 JSON이 있는 곳 (연도 폴더 포함)
SEARCH_DIR = ROOT / "docs" / "data" / "search"
PEOPLE_DIR = ROOT / "docs" / "data" / "people"

SEARCH_DIR.mkdir(parents=True, exist_ok=True)
PEOPLE_DIR.mkdir(parents=True, exist_ok=True)

def load_json(p: Path):
    try:
        with p.open(encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return None

def parse_open_dt(s):
    # 'YYYYMMDD' 또는 'YYYY-MM-DD' 또는 '' 중 일부가 올 수 있음
    s = (s or "").strip().replace("-", "")
    if len(s) == 8 and s.isdigit():
        return s
    return ""

def norm_grade(s):
    s = (s or "").strip()
    # 표기 다양성 흡수
    mapping = {
        "전체관람가": "전체관람가",
        "전체 관람가": "전체관람가",
        "12세이상관람가": "12세이상관람가",
        "12세 이상 관람가": "12세이상관람가",
        "15세이상관람가": "15세이상관람가",
        "15세 이상 관람가": "15세이상관람가",
        "청소년관람불가": "청소년 관람불가",
        "청소년 관람불가": "청소년 관람불가",
    }
    return mapping.get(s, s)

def infer_rep_nation(detail: dict):
    """
    repNation 값이 비거나 불안정한 케이스 보정.
    - detail.get('repNation')이 'K' / 'F' 면 그대로 사용
    - nationAlt / nations / repNationNm 등에 '한국'이 보이면 'K', 아니면 'F'
    - 전혀 정보가 없으면 '' (필터에서 제외는 안 하도록 프런트는 'all'일 때만 포함)
    """
    v = (detail.get("repNation") or "").strip().upper()
    if v in ("K", "F"):
        return v

    # nationAlt 형태: "한국,미국" 같은 문자열일 수 있음
    for key in ("nationAlt", "repNationNm"):
        txt = (detail.get(key) or "").strip()
        if txt:
            return "K" if ("한국" in txt) else "F"

    # KOFIC 상세에 nations: [{nationNm: "한국"}, ...] 케이스
    nations = detail.get("nations") or []
    if isinstance(nations, list):
        names = ", ".join([(x.get("nationNm") or "") for x in nations])
        if names:
            return "K" if ("한국" in names) else "F"

    return ""  # 정보 부족

def as_list(x):
    if x is None:
        return []
    if isinstance(x, list):
        return x
    return [x]

def walk_detail_files():
    # 연도 하위 포함 모든 json (gitkeep 제외)
    pattern = str(DETAIL_DIR / "**" / "*.json")
    for p in glob.iglob(pattern, recursive=True):
        if p.endswith(".gitkeep"):
            continue
        yield Path(p)

def build_movies_index():
    rows = []
    for p in walk_detail_files():
        data = load_json(p)
        if not data:
            continue

        movieCd   = (data.get("movieCd") or "").strip()
        movieNm   = (data.get("movieNm") or "").strip()
        openDt    = parse_open_dt(data.get("openDt") or data.get("openDtStr") or "")
        prdtYear  = (data.get("prdtYear") or "").strip()
        repNation = infer_rep_nation(data)
        grade     = norm_grade(data.get("grade") or data.get("watchGradeNm") or "")
        genres    = []
        for g in as_list(data.get("genres")):
            if isinstance(g, dict):
                nm = g.get("genreNm") or g.get("name") or ""
            else:
                nm = str(g)
            nm = nm.strip()
            if nm:
                genres.append(nm)

        # audiAcc는 상세 JSON에 있으면 그대로, 없으면 null
        try:
            audiAcc = int(str(data.get("audiAcc")).replace(",", "")) if data.get("audiAcc") not in (None, "", "null") else None
        except Exception:
            audiAcc = None

        rows.append({
            "movieCd": movieCd,
            "movieNm": movieNm,
            "openDt": openDt,
            "prdtYear": (prdtYear or (openDt[:4] if openDt else "")),
            "repNation": repNation,   # 'K' / 'F' / ''
            "grade": grade,
            "genres": genres,
            "audiAcc": audiAcc,
        })

    rows.sort(key=lambda r: (r["openDt"] or "00000000", r["movieCd"]), reverse=False)
    out = {
        "generatedAt": int(time.time()),
        "count": len(rows),
        "movies": rows,
    }
    (SEARCH_DIR / "movies.json").write_text(json.dumps(out, ensure_ascii=False, indent=2), encoding="utf-8")
    return out

def build_people_index():
    """
    배우/감독 캐시 인덱스(빠른 검색 전용)
    - 결과: docs/data/search/people.json
    - 구조: { generatedAt, count, people: [ {peopleCd, peopleNm, repRoleNm, films:[{movieCd, movieNm, openDt, part}]} ] }
    """
    people = defaultdict(lambda: {"peopleCd": "", "peopleNm": "", "repRoleNm": "", "films": []})

    for p in walk_detail_files():
        data = load_json(p)
        if not data:
            continue

        movieCd = (data.get("movieCd") or "").strip()
        movieNm = (data.get("movieNm") or "").strip()
        openDt  = parse_open_dt(data.get("openDt") or "")

        # 감독
        for d in as_list(data.get("directors")):
            name = (d.get("peopleNm") or d.get("directorNm") or "").strip()
            code = (d.get("peopleCd") or "").strip()
            if not name:
                continue
            key = (code or ("NM:" + name))
            rec = people[key]
            rec["peopleNm"] = name
            rec["peopleCd"] = code
            if not rec["repRoleNm"]:
                rec["repRoleNm"] = "감독"
            rec["films"].append({"movieCd": movieCd, "movieNm": movieNm, "openDt": openDt, "part": ""})

        # 배우
        for a in as_list(data.get("actors")):
            name = (a.get("peopleNm") or a.get("cast") or a.get("castNm") or "").strip()
            code = (a.get("peopleCd") or "").strip()
            if not name:
                continue
            part = (a.get("cast") or a.get("castNm") or "").strip()
            key = (code or ("NM:" + name))
            rec = people[key]
            rec["peopleNm"] = name
            rec["peopleCd"] = code
            # 배우/감독 겸업이면 배우를 우선 표기하지 않음(이미 감독이면 그대로), 없으면 배우 세팅
            if not rec["repRoleNm"]:
                rec["repRoleNm"] = "배우"
            rec["films"].append({"movieCd": movieCd, "movieNm": movieNm, "openDt": openDt, "part": part})

    # 최신 개봉일 순으로 필모 정렬
    for rec in people.values():
        rec["films"].sort(key=lambda f: f["openDt"] or "00000000", reverse=True)

    people_rows = sorted(people.values(), key=lambda r: (r["peopleNm"], r["peopleCd"]))
    out = {
        "generatedAt": int(time.time()),
        "count": len(people_rows),
        "people": people_rows,
    }
    (SEARCH_DIR / "people.json").write_text(json.dumps(out, ensure_ascii=False, indent=2), encoding="utf-8")
    # people/unknown.json(빈 프로필 대응)도 항상 유지
    (PEOPLE_DIR / "unknown.json").write_text(json.dumps({"peopleNm":"", "films":[]}, ensure_ascii=False, indent=2), encoding="utf-8")
    return out

def main():
    mv = build_movies_index()
    pp = build_people_index()
    print(f"[index] movies: {mv['count']} / people: {pp['count']}")

if __name__ == "__main__":
    main()
