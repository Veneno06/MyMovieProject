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
    # [수정] glob 결과를 Path 객체 리스트로 변환 (p.name 사용을 위해)
    files = sorted([Path(p) for p in glob.glob(str(DATA_DIR / "**" / "*.json"), recursive=True)])
    
    index_list = []
    
    for p in files:
        try:
            # 파일 읽기 (p는 이제 Path 객체이므로 바로 read_text 사용 가능)
            txt = p.read_text(encoding="utf-8")
            if not txt.strip(): continue # 빈 파일 건너뜀
            
            data = json.loads(txt)
            
            # 데이터 구조 평탄화 (flat vs raw 대응)
            info = data if data.get("movieCd") else ((data.get("movieInfoResult") or {}).get("movieInfo") or {})
            
            if not info.get("movieCd"):
                continue

            # 감독 및 배우 정보를 이름과 코드(ID)가 포함된 리스트로 변환
            directors = []
            for d in (info.get("directors") or []):
                nm = d.get("peopleNm", "").strip()
                if nm:
                    directors.append({
                        "name": nm,
                        "id": d.get("peopleCd", "").strip()
                    })

            actors = []
            for a in (info.get("actors") or []):
                nm = a.get("peopleNm", "").strip()
                if nm:
                    actors.append({
                        "name": nm,
                        "id": a.get("peopleCd", "").strip(),
                        "cast": a.get("cast", "").strip()
                    })

            # 검색 인덱스 데이터 구성
            index_list.append({
                "id": info.get("movieCd"),
                "title": info.get("movieNm"),
                "titleEn": info.get("movieNmEn"),
                "year": info.get("prdtYear"),
                "nation": (info.get("nations") or [{}])[0].get("nationNm") if info.get("nations") else "",
                "directors": directors,
                "actors": actors
            })
            
        except Exception as e:
            # [수정] p가 Path 객체이므로 p.name 접근이 가능해짐 -> 에러 없이 경고 출력
            print(f"[warn] Failed to parse {p.name}: {e}")
            continue

    # 디렉토리 생성 및 저장
    OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)
    
    # JSON 파일 저장 (UTF-8)
    OUTPUT_FILE.write_text(json.dumps(index_list, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"[done] Indexed {len(index_list)} movies to {OUTPUT_FILE}")

if __name__ == "__main__":
    reindex()
