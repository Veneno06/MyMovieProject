import os, json, pathlib

MOVIES_DIR = pathlib.Path("docs/data/movies")
SEARCH_DIR = pathlib.Path("docs/data/search")
PEOPLE_DIR = pathlib.Path("docs/data/people")
SEARCH_DIR.mkdir(parents=True, exist_ok=True)
PEOPLE_DIR.mkdir(parents=True, exist_ok=True)

def norm(s): 
    return (s or "").strip()

movies_idx = []
people_map = {}  # peopleCd -> {peopleCd, peopleNm, filmo: set([...movieCd])}

for p in MOVIES_DIR.glob("*.json"):
    j = json.loads(p.read_text(encoding="utf-8"))
    info = j.get("movieInfoResult", {}).get("movieInfo", {})
    movie_cd = info.get("movieCd")
    movie_nm = norm(info.get("movieNm"))
    open_dt  = norm(info.get("openDt"))  # yyyyMMdd
    show_tm  = norm(info.get("showTm"))
    prdt_yr  = norm(info.get("prdtYear"))
    nations  = [n.get("nationNm") for n in info.get("nations", [])]
    genres   = [g.get("genreNm")  for g in info.get("genres", [])]
    directors= [d.get("peopleNm") for d in info.get("directors", [])]
    actors   = [a.get("peopleNm") for a in info.get("actors", [])]

    movies_idx.append({
        "movieCd": movie_cd,
        "movieNm": movie_nm,
        "openDt": open_dt,
        "showTm": show_tm,
        "prdtYear": prdt_yr,
        "nations": nations,
        "genres": genres,
        "directors": directors,
        "actors": actors,
    })

    for a in info.get("actors", []):
        pid = norm(a.get("peopleCd") or "")
        pname = norm(a.get("peopleNm"))
        if not pname:
            continue
        if pid not in people_map:
            people_map[pid] = {"peopleCd": pid, "peopleNm": pname, "filmo": set()}
        people_map[pid]["filmo"].add(movie_cd)

# save movies index
(SEARCH_DIR / "movies.json").write_text(
    json.dumps({"items": movies_idx}, ensure_ascii=False, indent=2), encoding="utf-8"
)

# save people index & individual person files
people_idx = []
for pid, obj in people_map.items():
    obj["filmo"] = sorted(list(obj["filmo"]))
    people_idx.append({"peopleCd": pid, "peopleNm": obj["peopleNm"], "filmoCnt": len(obj["filmo"])})
    # 개별 person 파일(간단 버전)
    (PEOPLE_DIR / f"{pid or 'unknown'}.json").write_text(
        json.dumps(obj, ensure_ascii=False, indent=2), encoding="utf-8"
    )

(SEARCH_DIR / "people.json").write_text(
    json.dumps({"items": people_idx}, ensure_ascii=False, indent=2), encoding="utf-8"
)

print("[DONE] movies:", len(movies_idx), "people:", len(people_idx))
