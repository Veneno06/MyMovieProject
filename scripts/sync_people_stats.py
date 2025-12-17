# scripts/sync_people_stats.py
import os
import json
import glob
from pathlib import Path
import sys

# 인코딩 설정
sys.stdout.reconfigure(encoding='utf-8')

HERE = Path(__file__).resolve()
ROOT = HERE.parents[1] if HERE.parents[1].name == "MyMovieProject" else HERE.parents[2]
MOVIE_DIR = ROOT / "docs" / "data" / "movies"
PEOPLE_DIR = ROOT / "docs" / "data" / "people"

def load_json(p: Path):
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except: return None

def save_json(p: Path, data: dict):
    with open(p, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

def sync_stats():
    # 1. 영화 데이터 메모리에 로드
    print("[Step 1] 영화 관객수 데이터 로딩 중...")
    movie_audi_map = {} 
    
    movie_files = glob.glob(str(MOVIE_DIR / "**" / "*.json"), recursive=True)
    for p in movie_files:
        data = load_json(Path(p))
        if not data: continue
        
        info = data if data.get("movieCd") else ((data.get("movieInfoResult") or {}).get("movieInfo") or {})
        movie_cd = info.get("movieCd")
        audi_acc = info.get("audiAcc")
        
        if movie_cd and audi_acc:
            try:
                val = int(str(audi_acc).replace(",", ""))
                if val > 0:
                    movie_audi_map[movie_cd] = val
            except: pass
                
    print(f" -> 총 {len(movie_audi_map)}개의 유효한 관객수 정보 확보.")

    # 2. 배우 파일 업데이트
    print("[Step 2] 배우 파일 동기화 시작...")
    people_files = glob.glob(str(PEOPLE_DIR / "*.json"))
    updated_count = 0
    
    for p in people_files:
        p_path = Path(p)
        data = load_json(p_path)
        if not data: continue
        
        people_info = (data.get("peopleInfoResult") or {}).get("peopleInfo")
        if not people_info: continue
        
        filmos = people_info.get("filmos") or []
        if not filmos: continue
        
        is_changed = False
        for filmo in filmos:
            m_cd = filmo.get("movieCd")
            # 영화 파일에 더 최신/정확한 관객수가 있다면 업데이트
            if m_cd in movie_audi_map:
                new_acc = movie_audi_map[m_cd]
                old_acc = filmo.get("audiAcc")
                
                # 기존 값이 없거나(None), 0이거나, 새 값이 더 크면 업데이트
                try:
                    old_val = int(str(old_acc).replace(",", "")) if old_acc else 0
                except: old_val = 0
                
                if new_acc > old_val:
                    filmo["audiAcc"] = new_acc
                    is_changed = True
        
        if is_changed:
            save_json(p_path, data)
            updated_count += 1
            
    print(f"=== 최종 완료: 총 {updated_count}명의 배우 프로필 업데이트 ===")

if __name__ == "__main__":
    sync_stats()
