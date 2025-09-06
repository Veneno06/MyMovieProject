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

def make_session() -> requests.Session:
    s = requests.Session()
    retries = Retry(
        total=8,
        connect=5,
        read=5,
        backoff_factor=1.5,
        status_forcelist=[429, 500, 502, 503, 504],
    )
    adapter = HTTPAdapter(max_retries=retries)
    s.mount("https://", adapter)
    s.mount("http://", adapter)
    s.headers.update(HEADERS)
    return s

def ensure_dir(p): os.makedirs(p, exist_ok=True)

def load_json(path, default=None):
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return default

def save_json(path, obj):
    ensure_dir(os.path.dirname(path))
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=2)
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
        if j.get("faultInfo") or j.get("faultResult"):
            return None, f"fault={j.get('faultInfo') or j.get('faultResult')}"
        info = (j.get("movieInfoResult") or {}).get("movieInfo")
        return info, None
    except requests.exceptions.RequestException as e:
        return None, f"http_error={e}"

def fetch_weekly_audi_acc(session, movieCd, openDtYMD, weeks=12):
    if not KOFIC_KEY: return None
    # openDtYMD가 YYYY-MM-DD 형식일 수 있으므로 정규화
    openDtYMD = norm_ymd(openDtYMD)
    base = parse_date_ymd(openDtYMD) or datetime.now()
    base = base + timedelta(days=3)
    max_acc = None
    for i in range(weeks):
        d = (base + timedelta(weeks=i))
        td = d.strftime("%Y%m%d")
        url = f"https://www.kobis.or.kr/kobisopenapi/webservice/rest/boxoffice/searchWeeklyBoxOfficeList.json?key={KOFIC_KEY}&targetDt={td}&weekGb=0"
        try:
            r = get(session, url, sleep=0.1) # 관객수 조회는 더 짧은 딜레이
            js = r.json()
            if js.get("faultInfo") or js.get("faultResult"):
                err = js.get("faultInfo") or js.get("faultResult") or {}
                code = str(err.get("errorCode") or err.get("errorcode") or "")
                if code == "320011": raise RuntimeError("RATE_LIMIT")
                continue
            items = (js.get("boxOfficeResult") or {}).get("weeklyBoxOfficeList") or []
            for it in items:
                if it.get("movieCd") == movieCd:
                    a = it.get("audiAcc")
                    if a is None: continue
                    a = int(str(a).replace(",", ""))
                    max_acc = a if max_acc is None else max(max_acc, a)
        except requests.exceptions.RequestException:
            continue
    return max_acc

def collect_candidates(year):
    p = os.path.join(YEARS_DIR, f"year-{year}.json")
    j = load_json(p, {"movieList": [], "movieCds": []})
    return sorted(list(set(j.get("movieCds") or [m.get("movieCd") for m in j.get("movieList", []) if m.get("movieCd")])))

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--year-start", required=True)
    ap.add_argument("--year-end", required=True)
    ap.add_argument("--audiacc", choices=["off","all"], default="off")
    args = ap.parse_args()

    y1, y2 = int(args.year_start), int(args.year_end)
    mode = args.audiacc
    session = make_session()
    total_newly_saved = 0
    total_updated_audi = 0
    
    try:
        for y in range(y1, y2+1):
            cds = collect_candidates(y)
            print(f"[{y}] total candidates: {len(cds)}")
            out_dir = os.path.join(MOVIES_DIR, f"{y}")
            ensure_dir(out_dir)

            for i, cd in enumerate(cds):
                print(f"  -> Processing {y} ({i+1}/{len(cds)}): {cd}", end='\r')
                out = os.path.join(out_dir, f"{cd}.json")

                if os.path.exists(out):
                    if mode == "off": continue
                    
                    data = load_json(out)
                    info = (data.get("movieInfoResult") or {}).get("movieInfo") or {}
                    
                    if info.get("audiAcc") is not None: continue

                    # [업그레이드] 파일은 있지만 관객수 정보가 없는 경우, 관객수만 추가 조회
                    acc = fetch_weekly_audi_acc(session, cd, info.get("openDt"))
                    if isinstance(acc, int):
                        info["audiAcc"] = acc
                        save_json(out, data)
                        total_updated_audi += 1
                else:
                    # 파일이 없는 경우, 영화 정보와 관객수 모두 조회
                    info, err = fetch_movie_info(session, cd)
                    if err:
                        if "320011" in err: raise RuntimeError("RATE_LIMIT")
                        print(f"\n[warn] {y} {cd}: {err}")
                        continue
                    if not info: continue
                    
                    if mode == "all":
                        acc = fetch_weekly_audi_acc(session, cd, info.get("openDt"))
                        if isinstance(acc, int): info["audiAcc"] = acc
                    
                    save_json(out, {"movieInfoResult": {"movieInfo": info}})
                    total_newly_saved += 1
            
            print(f"\n[{y}] year done.")

        print(f"\n[DONE] Total newly saved: {total_newly_saved}, Total audiAcc updated: {total_updated_audi}")

    except RuntimeError as e:
        if str(e) == "RATE_LIMIT":
            print("\n[STOP] API rate limit reached.")
            sys.exit(0)
        raise

if __name__ == "__main__":
    main()
