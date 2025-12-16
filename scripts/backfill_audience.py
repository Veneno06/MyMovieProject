# scripts/backfill_audience.py
import os
import json
import time
import glob
import argparse
import sys
import requests
from pathlib import Path
from bs4 import BeautifulSoup

# 인코딩 설정
sys.stdout.reconfigure(encoding='utf-8')

HERE = Path(__file__).resolve()
ROOT = HERE.parents[1] if HERE.parents[1].name == "MyMovieProject" else HERE.parents[2]
DETAIL_DIR = ROOT / "docs" / "data" / "movies"

# KOBIS 영화 상세 페이지 URL
KOBIS_DETAIL_URL = "https://www.kobis.or.kr/kobis/business/mast/mvie/searchMovieDtl.do"

def load_json(p: Path):
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except: return None

def save_json(p: Path, data: dict):
    with open(p, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

def fetch_audience_count(movie_cd):
    """
    KOBIS 웹사이트에서 movieCd로 상세 페이지를 조회하여 누적관객수를 크롤링합니다.
    """
    try:
        # 1. 페이지 요청
        params = {"code": movie_cd}
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
        }
        r = requests.get(KOBIS_DETAIL_URL, params=params, headers=headers, timeout=10)
        r.raise_for_status()

        # 2. HTML 파싱
        soup = BeautifulSoup(r.text, "html.parser")
        
        # 3. "누적관객수" 찾기
        stats = soup.find_all("dl", class_="ov_stats")
        
        for dl in stats:
            dt_list = dl.find_all("dt")
            dd_list = dl.find_all("dd")
            for dt, dd in zip(dt_list, dd_list):
                if "누적관객수" in dt.text:
                    # "12,345 명" 또는 "12,345" 등 처리
                    raw_str = dd.text.strip().replace(",", "").replace("명", "")
                    if raw_str.isdigit():
                        return int(raw_str)
        return 0 
    except Exception as e:
        print(f"[Error] {movie_cd} 크롤링 실패: {e}")
        return -1 

def backfill_audience(budget: int, rate_sleep_ms: int):
    # 1. 영화 파일 스캔 (관객수 없는 영화 찾기)
    # 최신 영화부터 찾기 위해 reverse=True
    files = sorted([Path(p) for p in glob.glob(str(DETAIL_DIR / "**" / "*.json"), recursive=True)], reverse=True)
    
    print(f"[Step 1] 전체 영화({len(files)}개) 스캔 중... 관객수 미보유작 탐색")
    
    target_list = []
    
    for p in files:
        data = load_json(p)
        if not data: continue
        
        info = data if data.get("movieCd") else ((data.get("movieInfoResult") or {}).get("movieInfo") or {})
        movieCd = info.get("movieCd")
        movieNm = info.get("movieNm")
        
        if not movieCd: continue

        # 이미 관객수가 있는지 확인
        audi_acc = info.get("audiAcc")
        
        # 관객수가 없거나(None), 0이거나, 문자열 '0'인 경우 타겟
        if audi_acc is None or audi_acc == 0 or str(audi_acc) == "0":
            target_list.append({
                "path": p,
                "movieCd": movieCd,
                "movieNm": movieNm,
                "data": data
            })

    print(f"[Step 1 완료] 총 {len(target_list)}편의 관객수 미보유 영화 발견.")
    
    # 2. 크롤링 시작
    used = 0
    updated_count = 0
    
    for item in target_list:
        if budget > 0 and used >= budget:
            print(f"[Stop] 예산 소진 ({used}회).")
            break
            
        m_cd = item["movieCd"]
        m_nm = item["movieNm"]
        p_path = item["path"]
        f_data = item["data"]
        
        # 크롤링
        count = fetch_audience_count(m_cd)
        used += 1
        
        if count > 0:
            # 데이터 구조에 맞게 업데이트
            if f_data.get("movieCd"): 
                f_data["audiAcc"] = count
            elif "movieInfoResult" in f_data:
                f_data["movieInfoResult"]["movieInfo"]["audiAcc"] = count
            
            save_json(p_path, f_data)
            print(f" -> ✅ {m_nm}({m_cd}): {count:,}명 업데이트 성공")
            updated_count += 1
        else:
            print(f" -> ⚠️ {m_nm}({m_cd}): 데이터 없음 (0명)")
            
        # 차단 방지를 위한 딜레이
        time.sleep(max(0.5, rate_sleep_ms / 1000.0))

    print(f"=== 최종 완료: 총 {updated_count}편 관객수 업데이트, {used}회 시도 ===")

if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--budget", type=int, default=1000)
    ap.add_argument("--rate-sleep-ms", type=int, default=500)
    args, unknown = ap.parse_known_args()
    
    backfill_audience(args.budget, args.rate_sleep_ms)
