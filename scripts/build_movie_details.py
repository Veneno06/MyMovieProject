# scripts/build_movie_details.py
import os, sys, json, time, argparse, re
from datetime import datetime, timedelta
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

ROOT = os.path.dirname(os.path.dirname(__file__))
DATA = os.path.join(ROOT, "docs", "data")
YEARS_DIR = os.path.join(DATA, "years")
MOVIES_DIR = os.path.join(DATA, "movies")

KOFIC_KEY = os.environ.get("KOFIC_API_KEY", "").strip()
# [중요] 웹사이트 접근을 위한 헤더 설정
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

def get(session, url, timeout=30, sleep=0.2): # 웹 스크래핑이므로 0.2초 대기
    time.sleep(sleep)
    r = session.get(url, timeout=timeout)
    r.raise_for_status()
    return r

def fetch_movie_info(session, movieCd):
    # 기본 메타데이터는 여전히 API가 가장 정확함
    url = f"https://www.kobis.or.kr/kobisopenapi/webservice/rest/movie/searchMovieInfo.json?key={KOFIC_KEY}&movieCd={movieCd}"
    try:
        r = get(session, url, sleep=0.1)
        j = r.json()
        if j.get("faultInfo") or j.get("faultResult"): return None, f"fault={j.get('faultInfo') or j.get('faultResult')}"
        info = (j.get("movieInfoResult") or {}).get("movieInfo")
        return info, None
    except requests.exceptions.RequestException as e: return None, f"http_error={e}"

# [핵심 기능] KOBIS 웹사이트에서 최종 관객수 '직접' 가져오기 (1회 호출)
def fetch_final_audi_from_kobis(session, movieCd):
    url = f"https://www.kobis.or.kr/kobis/business/mast/mvie/searchMovieDtl.do?code={movieCd}"
    try:
        # 웹페이지 HTML 요청
        r = get(session, url)
        html = r.text
        
        # 정규표현식으로 "누적관객수" 숫자 추출
        # 패턴: <dt>누적관객수</dt> ... <dd> 12,345 명</dd> 형태를 찾음
        # (공백이나 줄바꿈에 유연하게 대응하도록 작성)
        match = re.search(r"누적관객수\s*</dt>\s*<dd>\s*([0-9,]+)\s*명", html, re.DOTALL)
        
        if match:
            raw_num = match.group(1).replace(",", "")
            return int(raw_num)
        
        return None # 못 찾았으면 None
        
    except Exception as e:
        # print(f"[warn] Scraping failed for {movieCd}: {e}")
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
    # off: 안함, missing: 없는것만 채움(복구용), all: 전부 다시 채움(업그레이드용)
    ap.add_argument("--audiacc", choices=["off","all","missing"], default="missing")
    ap.add_argument("--audiacc-weeks", default="0") # 더 이상 사용 안 함
    args, unknown = ap.parse_known_args()

    y1, y2 = args.year_start, args.year_end
    mode = args.audiacc
    session = make_session()
    
    total_newly_saved = 0
    total_updated_audi = 0
    
    try:
        cds = collect_candidates(y1, y2)
        print(f"[{y1}-{y2}] 총 {len(cds)}개 영화 검사 시작 (모드: {mode} - KOBIS 직접 조회)")

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
                
                # 현재 관객수 확인
                current_audi = info.get("audiAcc")
                has_valid_audi = (current_audi is not None and int(str(current_audi).replace(",","")) > 0)
                
                # missing 모드인데 이미 있으면 패스
                if mode == "missing" and has_valid_audi:
                    continue 

                # [업그레이드] all 모드이거나, missing인데 데이터가 없으면 실행
                print(f" -> [KOBIS 조회] {cd} ({info.get('movieNm')}) 최종 관객수 확인 중...", end='\r')
                
                final_acc = fetch_final_audi_from_kobis(session, cd)
                
                # 성공 시 업데이트 (0명이어도 확정 데이터면 저장)
                if final_acc is not None:
                    info["audiAcc"] = final_acc
                    save_json(out, data)
                    total_updated_audi += 1
                    # print(f" -> 업데이트 완료: {final_acc}명")
            else:
                # 아예 파일이 없는 경우 -> API로 기본정보 + 웹으로 관객수
                info, err = fetch_movie_info(session, cd)
                if err:
                    if "320011" in err: raise RuntimeError("RATE_LIMIT")
                    continue
                if not info: continue
                
                if mode in ["all", "missing"]:
                    final_acc = fetch_final_audi_from_kobis(session, cd)
                    if final_acc is not None:
                        info["audiAcc"] = final_acc
                
                save_json(out, {"movieInfoResult": {"movieInfo": info}})
                total_newly_saved += 1
        
        print(f"\n[완료] 총 {total_updated_audi}개의 영화 관객수 데이터를 '최종 확정값'으로 업데이트했습니다.")

    except RuntimeError as e:
        if str(e) == "RATE_LIMIT":
            print("\n[STOP] API rate limit reached.")
            sys.exit(0)
        raise

if __name__ == "__main__":
    main()
