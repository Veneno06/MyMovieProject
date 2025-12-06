import os
import json
import glob
from pathlib import Path

# 경로 설정
HERE = Path(__file__).resolve()
ROOT = HERE.parents[1] if HERE.parents[1].name == "MyMovieProject" else HERE.parents[2]
DATA_DIR = ROOT / "docs" / "data" / "movies"
OUTPUT_FILE = ROOT / "docs" / "data" / "search_index.json"

def reindex():
    print(f"[reindex] Scanning {DATA_DIR}...")
    files = sorted([Path(p) for p in glob.glob(str(DATA_DIR / "**" / "*.json"), recursive=True)])
    index_list = []
    
    for p in files:
        try:
            txt = p.read_text(encoding="utf-8")
            if not txt.strip(): continue
            data = json.loads(txt)
            info = data if data.get("movieCd") else ((data.get("movieInfoResult") or {}).get("movieInfo") or {})
            if not info.get("movieCd"): continue

            # 배우 정보 (이름 + ID)
            actors = []
            for a in (info.get("actors") or []):
                nm = a.get("peopleNm", "").strip()
                if nm:
                    actors.append({
                        "name": nm,
                        "id": a.get("peopleCd", "").strip(),
                        "cast": a.get("cast", "").strip()
                    })

            # 감독 정보
            directors = []
            for d in (info.get("directors") or []):
                nm = d.get("peopleNm", "").strip()
                if nm:
                    directors.append({ "name": nm, "id": d.get("peopleCd", "").strip() })

            # 관객수 처리
            audi_acc = data.get("audiAcc") or info.get("audiAcc") or 0
            try: audi_acc = int(str(audi_acc).replace(",", ""))
            except: audi_acc = 0

            index_list.append({
                "movieCd": info.get("movieCd"),
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
    print(f"[done] Indexed {len(index_list)} movies.")

if __name__ == "__main__":
    reindex()
