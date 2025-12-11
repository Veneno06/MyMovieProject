# scripts/build_movie_details.py
import os, sys, json, time, argparse, re
from datetime import datetime, timedelta
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from urllib.parse import urlencode

ROOT = os.path.dirname(os.path.dirname(__file__))
# kofic_api 모듈 경로 추가
if ROOT not in sys.path:
    sys.path.append(os.path.join(ROOT, "scripts"))

# [중요] 키 교체 기능을 위해 모듈 임포트
try:
    from kofic_api import API_KEYS
except ImportError:
    API_KEYS = []
    print("[warn] kofic_api.py not found or API_KEYS empty.")

DATA = os.path.join(ROOT, "docs", "data")
YEARS_DIR = os.path.join(DATA, "years")
MOVIES_DIR = os.path.join(DATA, "movies")

# 전역 변수: 현재 키 인덱스
CURRENT_KEY_INDEX = 0

# 웹 스크래핑용 헤더
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
}

def make_session() -> requests.Session:
    s = requests.Session()
    retries = Retry(total=8, connect=5, read=5, backoff_factor=1.5, status_forcelist=[429, 500, 502, 503, 504])
    adapter = HTTPAdapter(max_retries=retries)
    s.mount("https://", adapter)
    s.mount("http://", adapter)
    s.headers.update(HEADERS)
    return s

def ensure_dir(p): os.makedirs(p, exist_ok=True)

def load_json(path, default=None):
    try:
        with open(path, "r", encoding="utf-8") as f: return json.load(f)
    except Exception: return default

def save_json(path, obj):
    ensure_dir(os.path.dirname(path))
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f: json.dump(obj, f, ensure_ascii=False, indent=2)
    os.replace(tmp, path)

def get(session, url, timeout=30, sleep=0.2):
    time.sleep(sleep)
    r = session.get(url, timeout=timeout)
    r.raise_for_status()
    return r

# [핵심] 다음 키로 교체하는 함수
def get_next_key():
    global CURRENT_KEY_INDEX
    if not API_KEYS: return None
    CURRENT_KEY_INDEX = (CURRENT_KEY_INDEX + 1) % len(API_KEYS)
    print(f"[system] 🔄 API Key switched to index {CURRENT_KEY_INDEX}")
    return API_KEYS[CURRENT_KEY_INDEX]

# [핵심] 스마트 API 호출 (자동 교체 적용)
def fetch_movie_info_smart(session, movieCd):
    global CURRENT_KEY_INDEX
    if not API_KEYS: return None, "NO_KEYS"
    
    # 현재 키
    current_key = API_KEYS[CURRENT_KEY_INDEX]
    max_retries = len(API_KEYS) + 1
    
    for _ in range(max_retries):
        url = f"https://www.kobis.or.kr/kobisopenapi/webservice/rest/movie/searchMovieInfo.json?key={current_key}&movieCd={movieCd}"
        try:
            r = session.get(url, timeout=10)
            j = r.json()
            
            fault = j.get("faultInfo") or j.get("faultResult")
            if fault:
                code = fault.get("errorCode")
                if code == "320011": # 키 소진
                    print(f"[warn] Key exhausted. Switching...")
                    current_key = get_next_key()
                    continue # 다음 키로 재시도
                else:
                    return None, f"fault={fault.get('message')}"
            
            info = (j.get("movieInfoResult") or {}).get("movieInfo")
            return info, None
            
        except Exception as e:
            time.sleep(1)
            
    return None, "ALL_KEYS_EXHAUSTED"

def fetch_final_audi_from_kobis(session, movieCd):
    # 이 함수는 API 키를 안 쓰므로 그대로 유지
    url = f"https://www.kobis.or.kr/kobis/business/mast/mvie/searchMovieDtl.do?code={movieCd}"
    try:
        r = get(session, url)
        html = r.text
        match = re.search(r"누적관객수\s*</dt>\s*<dd>\s*([0-9,]+)\s*명", html, re.DOTALL)
        if match:
            raw_num = match.group(1).replace(",", "")
            return int(raw_num)
        return None
    except Exception as e:
        return None

def collect_candidates(year_start, year_end):
    all_cds = set()
    for y in range(year_start, year_end + 1):
        p = os.path.join(YEARS_DIR, f"year-{y}.json")
        j = load_json(p, {"movieList": []})
        for movie in j.get("movieList", []):
            cd = str(movie.get("movieCd") or "").strip()
            if cd: all_cds.add(cd)
    return sorted(list(all_cds))

def get_movie_info_from_data(data):
    if not isinstance(data, dict): return None
    if "movieInfoResult" in data and "movieInfo" in data["movieInfoResult"]: return data["movieInfoResult"]["movieInfo"]
    if "movieCd" in data: return data
    return None

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--year-start", type=int, required=True)
    ap.add_argument("--year-end", type=int, required=True)
    ap.add_argument("--max", type=str, default="999999")
    ap.add_argument("--audiacc", choices=["off","all","missing"], default="missing")
    ap.add_argument("--audiacc-weeks", default="0")
    args, unknown = ap.parse_known_args()

    y1, y2 = args.year_start, args.year_end
    mode = args.audiacc
    session = make_session()
    
    total_newly_saved = 0
    total_updated_audi = 0
    
    try:
        cds = collect_candidates(y1, y2)
        print(f"[{y1}-{y2}] 총 {len(cds)}개 영화 검사 시작 (모드: {mode})")

        for i, cd in enumerate(cds):
            year_guess = cd[:4]
            out_dir = os.path.join(MOVIES_DIR, year_guess)
            ensure_dir(out_dir)
            out = os.path.join(out_dir, f"{cd}.json")

            if os.path.exists(out):
                data = load_json(out)
                info = get_movie_info_from_data(data)
                if not info: continue
                
                if mode == "off": continue
                
                current_audi = info.get("audiAcc")
                has_valid_audi = (current_audi is not None and int(str(current_audi).replace(",","")) > 0)
                
                if mode == "missing" and has_valid_audi:
                    continue 

                print(f" -> [복구/갱신] {cd} ({info.get('movieNm')}) 관객수 찾는 중...", end='\r')
                
                final_acc = fetch_final_audi_from_kobis(session, cd)
                
                if final_acc is not None:
                    info["audiAcc"] = final_acc
                    save_json(out, data)
                    total_updated_audi += 1
            else:
                # 파일 없으면 생성 (API 사용)
                # [수정] 스마트 키 교체 적용된 함수 호출
                info, err = fetch_movie_info_smart(session, cd)
                
                if err == "ALL_KEYS_EXHAUSTED":
                     print("\n[STOP] 모든 API 키가 소진되었습니다.")
                     sys.exit(0)
                if not info: continue
                
                if mode in ["all", "missing"]:
                    final_acc = fetch_final_audi_from_kobis(session, cd)
                    if final_acc is not None:
                        info["audiAcc"] = final_acc
                
                save_json(out, {"movieInfoResult": {"movieInfo": info}})
                total_newly_saved += 1
        
        print(f"\n[완료] 총 {total_updated_audi}개의 영화 관객수 업데이트 완료.")

    except Exception as e:
        print(f"\n[Error] {e}")
        raise

if __name__ == "__main__":
    main()
