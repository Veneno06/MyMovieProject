# scripts/build_indices.py
# - docs/data/movies/ 이하 연도 폴더까지 재귀적으로 스캔
# - search/movies.json, search/people.json 생성
# - repNation/grade/openDt 정규화
# - 배우/감독 인덱스: directors/actors 외에 casts/staffs 등 다양한 키도 인식
# - peopleCd가 없을 때는 이름 키(NM:이름)로 폴백

import json, os, time, glob
from pathlib import Path
from collections import defaultdict

ROOT = Path(__file__).resolve().parents[1]
DETAIL_DIR = ROOT / "docs" / "data" / "movies"
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
    s = (s or "").strip().replace("-", "")
    if len(s) == 8 and s.isdigit():
        return s
    return ""

def norm_grade(s):
    s = (s or "").strip()
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
    v = (detail.get("repNation") or "").strip().upper()
    if v in ("K", "F"):
        return v
    # nationAlt / repNationNm / nations[*].nationNm 중 '한국' 포함 여부로 추정
    for key in ("nationAlt", "repNationNm"):
        txt = (detail.get(key) or "").strip()
        if txt:
            return "K" if ("한국" in txt) else "F"
    nations = detail.get("nations") or []
    if isinstance(nations, list):
        names = ", ".join([(x.get("nationNm") or "") for x in nations])
        if names:
            return "K" if ("한국" in names) else "F"
    return ""

def as_list(x):
    if x is None: return []
    return x if isinstance(x, list) else [x]

def walk_detail_files():
    pattern = str(DETAIL_DIR / "**" / "*.json")
    for p in glob.iglob(pattern, recursive=True):
        if p.endswith(".gitkeep"):
            continue
        yield Path(p)

# ---------- 영화 인덱스 ----------
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

        # 장르
        genres = []
        for g in as_list(data.get("genres")):
            if isinstance(g, dict):
                nm = g.get("genreNm") or g.get("name") or ""
            else:
                nm = str(g)
            nm = nm.strip()
            if nm:
                genres.append(nm)

        # 누적관객수
        try:
            audiAcc = data.get("audiAcc")
            if isinstance(audiAcc, str): audiAcc = audiAcc.replace(",", "")
            audiAcc = int(audiAcc) if audiAcc not in (None, "", "null") else None
        except Exception:
            audiAcc = None

        rows.append({
            "movieCd": movieCd,
            "movieNm": movieNm,
            "openDt": openDt,
            "prdtYear": (prdtYear or (openDt[:4] if openDt else "")),
            "repNation": repNation,  # 'K'/'F'/''
            "grade": grade,
            "genres": genres,
            "audiAcc": audiAcc,
        })

    rows.sort(key=lambda r: (r["openDt"] or "00000000", r["movieCd"]))
    out = {"generatedAt": int(time.time()), "count": len(rows), "movies": rows}
    (SEARCH_DIR / "movies.json").write_text(json.dumps(out, ensure_ascii=False, indent=2), encoding="utf-8")
    return out

# ---------- 사람 인덱스(감독/배우) ----------
def _push_person(bucket, role_hint, entry, movieCd, movieNm, openDt):
    # entry에서 이름/코드/역할 추출 (여러 스키마 대응)
    name = (entry.get("peopleNm") or entry.get("directorNm") or entry.get("name") 
            or entry.get("castNm") or entry.get("cast") or entry.get("staffNm") or "").strip()
    if not name:
        # 문자열만 있는 케이스
        if isinstance(entry, str):
            name = entry.strip()
        if not name:
            return
    code = (entry.get("peopleCd") or entry.get("staffCd") or "").strip()

    role = (entry.get("repRoleNm") or entry.get("staffRoleNm") or entry.get("role") or role_hint or "").strip()
    # role 힌트 보정
    if not role and role_hint:
        role = role_hint
    # 스태프 Role에 '감독'/'배우' 포함되면 그걸 우선
    if "감독" in role:
        role = "감독"
    elif "배우" in role or role_hint == "배우":
        role = "배우"

    key = code if code else f"NM:{name}"
    rec = bucket[key]
    rec["peopleNm"] = name
    rec["peopleCd"] = code
    if not rec["repRoleNm"]:
        rec["repRoleNm"] = role or "배우"
    rec["films"].append({"movieCd": movieCd, "movieNm": movieNm, "openDt": openDt, "part": (entry.get("castNm") or entry.get("cast") or "")})

def build_people_index():
    people = defaultdict(lambda: {"peopleCd": "", "peopleNm": "", "repRoleNm": "", "films": []})

    for p in walk_detail_files():
        data = load_json(p)
        if not data:
            continue

        movieCd = (data.get("movieCd") or "").strip()
        movieNm = (data.get("movieNm") or "").strip()
        openDt  = parse_open_dt(data.get("openDt") or "")

        # 1) directors (표준)
        for d in as_list(data.get("directors")):
            if isinstance(d, (dict, str)):
                _push_person(people, "감독", d, movieCd, movieNm, openDt)

        # 2) actors (표준)
        for a in as_list(data.get("actors")):
            if isinstance(a, (dict, str)):
                _push_person(people, "배우", a, movieCd, movieNm, openDt)

        # 3) casts (변형)
        for a in as_list(data.get("casts")):
            if isinstance(a, (dict, str)):
                _push_person(people, "배우", a, movieCd, movieNm, openDt)

        # 4) staffs 안에 staffRoleNm 으로 구분되는 구조
        for s in as_list(data.get("staffs")):
            if not isinstance(s, dict): 
                continue
            role = (s.get("staffRoleNm") or s.get("role") or "").strip()
            if "감독" in role:
                _push_person(people, "감독", s, movieCd, movieNm, openDt)
            if "배우" in role:
                _push_person(people, "배우", s, movieCd, movieNm, openDt)

        # 5) actorsNm: "이름,이름,..." 같은 문자열
        actorsNm = (data.get("actorsNm") or "").strip()
        if actorsNm:
            for nm in [x.strip() for x in actorsNm.replace("，", ",").split(",") if x.strip()]:
                _push_person(people, "배우", {"peopleNm": nm}, movieCd, movieNm, openDt)

        # 6) directorNm: 단일 문자열
        directorNm = (data.get("directorNm") or "").strip()
        if directorNm:
            _push_person(people, "감독", {"peopleNm": directorNm}, movieCd, movieNm, openDt)

    # 각 인물 필모 최신순
    for rec in people.values():
        rec["films"].sort(key=lambda f: f["openDt"] or "00000000", reverse=True)

    people_rows = sorted(people.values(), key=lambda r: (r["peopleNm"], r["peopleCd"]))
    out = {"generatedAt": int(time.time()), "count": len(people_rows), "people": people_rows}
    (SEARCH_DIR / "people.json").write_text(json.dumps(out, ensure_ascii=False, indent=2), encoding="utf-8")
    # 빈 프로필 대비
    (PEOPLE_DIR / "unknown.json").write_text(json.dumps({"peopleNm":"", "films":[]}, ensure_ascii=False, indent=2), encoding="utf-8")
    return out

def main():
    mv = build_movies_index()
    pp = build_people_index()
    print(f"[index] movies: {mv['count']} / people: {pp['count']}")

if __name__ == "__main__":
    main()
