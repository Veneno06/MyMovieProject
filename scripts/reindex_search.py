# scripts/reindex_search.py
import os
import json
import glob
from pathlib import Path

HERE = Path(__file__).resolve()
ROOT = HERE.parents[1] if HERE.parents[1].name == "MyMovieProject" else HERE.parents[2]
DATA_DIR = ROOT / "docs" / "data" / "movies"
PEOPLE_DIR = ROOT / "docs" / "data" / "people"
OUTPUT_FILE = ROOT / "docs" / "data" / "search_index.json"

def get_person_gender(people_cd):
    if not people_cd: return ""
    person_file = PEOPLE_DIR / f"{people_cd}.json"
    if not person_file.exists(): return ""
    
    try:
        data = json.loads(person_file.read_text(encoding="utf-8"))
        info = data.get("peopleInfoResult", {}).get("peopleInfo", {})
        return info.get("sex", "").strip()
    except Exception:
        return ""

def reindex():
    print(f"[reindex] Scanning {DATA_DIR}...")
    files = sorted([Path(p) for p in glob.glob(str(DATA_DIR / "**" / "*.json"), recursive=True)])
    
    index_list = []
    seen_movies = set()
    
    for p in files:
        try:
            txt = p.read_text(encoding="utf-8")
            if not txt.strip(): continue
            
            data = json.loads(txt)
            info = data if data.get("movieCd") else ((data.get("movieInfoResult") or {}).get("movieInfo") or {})
            
            movie_cd = info.get("movieCd")
            if not movie_cd: continue

            if movie_cd in seen_movies: continue
            seen_movies.add(movie_cd)

            actors = []
            for a in (info.get("actors") or []):
                nm = a.get("peopleNm", "").strip()
                pid = a.get("peopleCd", "").strip()
                
                if nm:
                    # 'ㅎ' 배우는 이제 성별 데이터가 있을 확률이 높음
                    gender = get_person_gender(pid) if pid else ""
                    actors.append({
                        "name": nm,
                        "id": pid,
                        "gender": gender,
                        "cast": a.get("cast", "").strip()
                    })

            directors = []
            for d in (info.get("directors") or []):
                nm = d.get("peopleNm", "").strip()
                if nm:
                    directors.append({ 
                        "name": nm, 
                        "id": d.get("peopleCd", "").strip() 
                    })

            audi_acc_raw = data.get("audiAcc") or info.get("audiAcc") or 0
            try:
                audi_acc = int(str(audi_acc_raw).replace(",", "").strip())
            except:
                audi_acc = 0

            index_list.append({
                "movieCd": movie_cd,
                "movieNm": info.get("movieNm"),
                "movieNmEn": info.get("movieNmEn"),
                "prdtYear": info.get("prdtYear"),
                "openDt": info.get("openDt") or "",
                "audiAcc": audi_acc,
                "nation": (info.get("nations") or [{}])[0].get("nationNm") if info.get("nations") else "",
                "directors": directors,
                "actors": actors
            })

        except Exception as e:
            print(f"[warn] Failed {p.name}: {e}")
            continue

    OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT_FILE.write_text(json.dumps(index_list, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"[done] Indexed {len(index_list)} movies (Unique).")

if __name__ == "__main__":
    reindex()
