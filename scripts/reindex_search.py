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
            
            # 데이터 구조 평탄화
            info = data if data.get("movieCd") else ((data.get("movieInfoResult") or {}).get("movieInfo") or {})
            
            if not info.get("movieCd"):
                continue

            # 감독 정보 (이름+ID)
            directors = []
            for d in (info.get("directors") or []):
                nm = d.get("peopleNm", "").strip()
                if nm:
                    directors.append({
                        "name": nm,
                        "id": d.get("peopleCd", "").strip()
                    })

            # 배우 정보 (이름+ID)
            actors = []
            for a in (info.get("actors") or []):
                nm = a.get("peopleNm", "").strip()
                if nm:
                    actors.append({
                        "name": nm,
                        "id": a.get("peopleCd", "").strip(),
                        "cast": a.get("cast", "").strip()
                    })

            # [중요] 기존 UI(차트, 정렬)를 위해 openDt, audiAcc 추가
            index_list.append({
                "movieCd": info.get("movieCd"),
                "movieNm": info.get("movieNm"),
                "movieNmEn": info.get("movieNmEn"),
                "prdtYear": info.get("prdtYear"),
                "openDt": info.get("openDt") or "",
                "audiAcc": data.get("audiAcc") or info.get("audiAcc"), # 관객수는 root나 info 어디든 있을 수 있음
                "nation": (info.get("nations") or [{}])[0].get("nationNm") if info.get("nations") else "",
                "directors": directors,
                "actors": actors
            })
            
        except Exception as e:
            print(f"[warn] Failed to parse {p.name}: {e}")
            continue

    OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT_FILE.write_text(json.dumps(index_list, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"[done] Indexed {len(index_list)} movies to {OUTPUT_FILE}")

if __name__ == "__main__":
    reindex()
