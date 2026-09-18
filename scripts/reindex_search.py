# scripts/reindex_search.py
import os
import json
try:
    from .movie_records import load_movie_records, movie_info, audience, preserve_enhanced_fields, update_audience_files
except ImportError:
    from movie_records import load_movie_records, movie_info, audience, preserve_enhanced_fields, update_audience_files
import glob
from pathlib import Path

HERE = Path(__file__).resolve()
ROOT = HERE.parents[1]
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
        sex = info.get("sex", "").strip()
        # API 데이터는 '남자', '여자'로 오므로 UI에 맞춰 '남', '여'로 변환 가능하나
        # 여기선 원본 데이터를 그대로 넘기고 UI(JS)에서 처리하도록 함.
        return sex
    except Exception:
        return ""

def reindex():
    print(f"[reindex] Scanning {DATA_DIR}...")
    records, audit = load_movie_records(DATA_DIR)
    audit_path = ROOT / "docs" / "data" / "research" / "coverage_audit.json"
    audit_path.parent.mkdir(parents=True, exist_ok=True)
    audit_path.write_text(json.dumps(audit, ensure_ascii=False, indent=2), encoding="utf-8")
    index_list = []
    for info in records:
        movie_cd = info["movieCd"]
        try:
            actors = []
            for a in (info.get("actors") or []):
                nm = a.get("peopleNm", "").strip()
                pid = a.get("peopleCd", "").strip()
                
                # 성별 데이터 조회
                gender = ""
                if pid:
                    gender = get_person_gender(pid)
                
                if nm:
                    actors.append({
                        "name": nm,
                        "id": pid,         # 코드가 있으면 ID로 들어감 (분리의 핵심)
                        "gender": gender,  # 성별 정보 추가
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

            audi_acc = info.get("audiAcc")

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
            print(f"[warn] Failed {movie_cd}: {e}")
            continue

    OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT_FILE.write_text(json.dumps(index_list, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"[done] Indexed {len(index_list)} movies.")

if __name__ == "__main__":
    reindex()
