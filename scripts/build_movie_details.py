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
HEADERS = {"User-Agent": "cache-builder/1.0"}

# ... (make_session, ensure_dir, load_json, save_json, get, norm_ymd, parse_date_ymd, fetch_movie_info, fetch_weekly_audi_acc 함수는 이전과 동일) ...
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
def get(session, url, timeout=30, sleep=0.13):
    time.sleep(sleep)
    r = session.get(url, timeout=timeout)
    r.raise_for_status()
    return r
def norm_ymd(s):
    if not s: return ""
    return re.sub(r"\D", "", str(s))[:8]
def parse_date_ymd(s):
    s = norm_ymd(s)
    if len(s) != 8: return None
    return datetime(int(s[:4]), int(s[4:6]), int(s[6:]))
def fetch_movie_info(session, movieCd):
    url = f"https://www.kobis.or.kr/kobisopenapi/webservice/rest/movie/searchMovieInfo.json?key={KOFIC_KEY}&movieCd={movieCd}"
    try:
        r = get(session, url)
        j = r.json()
        if j.get("faultInfo") or j.get("faultResult"): return None, f"fault={j.get('faultInfo') or j.get('faultResult')}"
        info = (j.get("movieInfoResult") or {}).get("movieInfo")
        return info, None
    except requests.exceptions.RequestException as e: return None, f"http_error={e}"
def fetch_weekly_audi_acc(session, movieCd, openDtYMD, weeks=12):
    if not KOFIC_KEY: return None
    openDtYMD = norm_ymd(openDtYMD)
    base = parse_date_ymd(openDtYMD) or datetime.now()
    base = base + timedelta(days=3)
    max_acc = None
    for i in range(weeks):
        d = (base + timedelta(weeks=i)); td = d.strftime("%Y%m%d")
        url = f"https://www.kobis.or.kr/kobisopenapi/webservice/rest/boxoffice/searchWeeklyBoxOfficeList.json?key={KOFIC_KEY}&targetDt={td}&weekGb=0"
        try:
            r = get(session, url, sleep=0.1)
            js = r.json()
            if js.get("faultInfo") or js.get("faultResult"):
                err = js.get("faultInfo") or js.get("faultResult") or {}; code = str(err.get("errorCode") or err.get("errorcode") or "")
                if code == "320011": raise RuntimeError("RATE_LIMIT")
                continue
            items = (js.get("boxOfficeResult") or {}).get("weeklyBoxOfficeList") or []
            for it in items:
                if it.get("movieCd") == movieCd:
                    a = it.get("audiAcc")
                    if a is None: continue
                    a = int(str(a).replace(",", "")); max_acc = a if max_acc is None else max(max_acc, a)
        except requests.exceptions.RequestException: continue
    return max_acc

# [수정] 날짜 범위 필터링을 위해 함수 로직 변경
def collect_candidates(year_start, year_end, date_start=None, date_end=None):
    all_cds = set()
    for y in range(year_start, year_end + 1):
        p = os.path.join(YEARS_DIR, f"year-{y}.json")
        j = load_json(p, {"movieList": []})
        
        # 날짜 필터링 로직 추가
        if date_start and date_end:
            start_ymd = norm_ymd(date_start)
            end_ymd = norm_ymd(date_end)
            for movie in j.get("movieList", []):
                open_dt = norm_ymd(movie.get("openDt", ""))
                if open_dt and start_ymd <= open_dt <= end_ymd:
                    cd = str(movie.get("movieCd") or "").strip()
                    if cd: all_cds.add(cd)
        else: # 날짜 범위가 없으면 기존처럼 연도 전체를 대상으로 함
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
    # [수정] 날짜 범위 인자 추가
    ap.add_argument("--year-start", type=int, required=True)
    ap.add_argument("--year-end", type=int, required=True)
    ap.add_argument("--date-start", default=None, help="Start date in YYYY-MM-DD format")
    ap.add_argument("--date-end", default=None, help="End date in YYYY-MM-DD format")
    ap.add_argument("--audiacc", choices=["off","all"], default="off")
    ap.add_argument("--audiacc-weeks", default="5")
    args = ap.parse_args()

    y1, y2 = args.year_start, args.year_end
    mode = args.audiacc
    weeks = int(args.audiacc_weeks)
    session = make_session()
    total_newly_saved = 0
    total_updated_audi = 0
    
    try:
        # [수정] 날짜 범위 인자를 collect_candidates에 전달
        cds = collect_candidates(y1, y2, args.date_start, args.date_end)
        print(f"[{y1}-{y2} / {args.date_start or ''}-{args.date_end or ''}] total candidates: {len(cds)}")

        for i, cd in enumerate(cds):
            print(f"  -> Processing ({i+1}/{len(cds)}): {cd}", end='\r')
            
            # 영화코드로 연도를 추정하여 올바른 폴더 경로 생성
            year_guess = cd[:4]
            out_dir = os.path.join(MOVIES_DIR, year_guess)
            ensure_dir(out_dir)
            out = os.path.join(out_dir, f"{cd}.json")

            if os.path.exists(out):
                if mode == "off": continue
                data = load_json(out)
                info = get_movie_info_from_data(data)
                if info is None or info.get("audiAcc") is not None: continue
                
                acc = fetch_weekly_audi_acc(session, cd, info.get("openDt"), weeks=weeks)
                if isinstance(acc, int):
                    info["audiAcc"] = acc
                    save_json(out, data)
                    total_updated_audi += 1
            else:
                info, err = fetch_movie_info(session, cd)
                if err:
                    if "320011" in err: raise RuntimeError("RATE_LIMIT")
                    print(f"\n[warn] {cd}: {err}")
                    continue
                if not info: continue
                
                if mode == "all":
                    acc = fetch_weekly_audi_acc(session, cd, info.get("openDt"), weeks=weeks)
                    if isinstance(acc, int): info["audiAcc"] = acc
                
                save_json(out, {"movieInfoResult": {"movieInfo": info}})
                total_newly_saved += 1
        
        print(f"\n[DONE] Total newly saved: {total_newly_saved}, Total audiAcc updated: {total_updated_audi}")

    except RuntimeError as e:
        if str(e) == "RATE_LIMIT":
            print("\n[STOP] API rate limit reached.")
            sys.exit(0)
        raise

if __name__ == "__main__":
    main()
