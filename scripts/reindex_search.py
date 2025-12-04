import os
import json
import glob
from pathlib import Path

# 경로 설정
HERE = Path(__file__).resolve()
ROOT = HERE.parents[1]  # 프로젝트 루트
DATA_DIR = ROOT / "docs" / "data" / "movies"
OUTPUT_FILE = ROOT / "docs" / "data" / "search_index.json"

def reindex():
    print(f"[reindex] Scanning {DATA_DIR}...")
    files = sorted(glob.glob(str(DATA_DIR / "**" / "*.json"), recursive=True))
    
    index_list = []
    
    for p in files:
        try:
            data = json.loads(Path(p).read_text(encoding="utf-8"))
            # 데이터 구조가 평탄화(flat)된 경우와 원본(raw)인 경우 모두 대응
            info = data if data.get("movieCd") else ((data.get("movieInfoResult") or {}).get("movieInfo") or {})
            
            if not info.get("movieCd"):
                continue

            # 검색에 필요한 최소한의 정보만 추출
            index_list.append({
                "id": info.get("movieCd"),
                "title": info.get("movieNm"),
                "titleEn": info.get("movieNmEn"),
                "year": info.get("prdtYear"),
                "nation": (info.get("nations") or [{}])[0].get("nationNm") if info.get("nations") else "",
                "director": (info.get("directors") or [{}])[0].get("peopleNm") if info.get("directors") else ""
            })
        except Exception as e:
            print(f"[warn] Failed to parse {p}: {e}")
            continue

    # 디렉토리가 없으면 생성
    OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)
    
    # JSON 파일 저장
    OUTPUT_FILE.write_text(json.dumps(index_list, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"[done] Indexed {len(index_list)} movies to {OUTPUT_FILE}")

if __name__ == "__main__":
    reindex()
