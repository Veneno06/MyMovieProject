# scripts/build_movie_details.py
import os, sys, json, time, argparse, re
from datetime import datetime, timedelta
import requests
# [수정] 자동 재시도를 위해 필요한 라이브러리 추가
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

ROOT = os.path.dirname(os.path.dirname(__file__))
DATA = os.path.join(ROOT, "docs", "data")
YEARS_DIR = os.path.join(DATA, "years")
MOVIES_DIR = os.path.join(DATA, "movies")

KOFIC_KEY = os.environ.get("KOFIC_API_KEY", "").strip()
HEADERS = {"User-Agent": "cache-builder/1.0"}

# [추가] build_year_cache.py에 있던 강력한 자동 재시도 세션 생성 함수
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

# [수정] requests.get 대신 session.get을 사용하도록 변경
def get(session, url, timeout=30, sleep=0.13):
    time.sleep(sleep)
    r = session.get(url, timeout=timeout)
    r.raise_for_status() # 오류 발생 시 여기서 예외를 던짐
    return r

def norm_ymd(s):
    if not s: return ""
    return re.sub(r"\D", "", str(s))[:8]

def parse_date_ymd(s):
    s = norm_ymd(s)
    if len(s) != 8: return None
    return datetime(int(s[:4]), int(s[4:6]), int(s[6:]))

# [수정] session을 인자로 받도록 변경
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
        # 재시도 후에도 실패하면 여기서 잡힘
        return None, f"http_error={e}"


def fetch_weekly_audi_acc(session, movieCd, openDtYMD, weeks=8):
    # 이 함수는 현재 로직에서 호출되진 않지만, 호환성을 위해 session을 받도록 수정
    if not KOFIC_KEY: return None
    base = parse_date_ymd(openDtYMD) or datetime.now()
    base = base + timedelta(days=3)
    max_acc = None
    for i in range(weeks):
        d = (base + timedelta(weeks=i))
        td = d.strftime("%Y%m%d")
        url = f"https://www.kobis.or.kr/kobisopenapi/webservice/rest/boxoffice/searchWeeklyBoxOfficeList.json?key={KOFIC_KEY}&targetDt={td}&weekGb=0"
        try:
            r = get(session, url)
            js = r.json()
            if js.get("faultInfo") or js.get("faultResult"):
                err = js.get("faultInfo") or js.get("faultResult") or {}
                code = str(err.get("errorCode") or err.get("errorcode") or "")
                if code == "320011":
                    raise RuntimeError("RATE_LIMIT")
                continue
            items = (js.get("boxOfficeResult") or {}).get("weeklyBoxOfficeList") or []
            for it in items:
                if it.get("movieCd") == movieCd:
                    a = it.get("audiAcc")
                    if a is None: continue
                    a = int(str(a).replace(",", ""))
                    max_acc = a if max_acc is None else max(max_acc, a)
        except requests.exceptions.RequestException:
            # 재시도 후에도 실패하면 그냥 다음 주로 넘어감
            continue
    return max_acc

def collect_candidates(year):
    p = os.path.join(YEARS_DIR, f"year-{year}.json")
    j = load_json(p, {"movieList": [], "movieCds": []})
    cds = set(j.get("movieCds") or [])
    for row in j.get("movieList") or []:
        cd = str(row.get("movieCd") or "").strip()
        if cd: cds.add(cd)
    return sorted(cds)

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--year-start", required=True)
    ap.add_argument("--year-end", required=True)
    ap.add_argument("--max", default="999999")
    ap.add_argument("--audiacc", choices=["off","recent","all"], default="off")
    ap.add_argument("--audiacc-days", default="90")
    args = ap.parse_args()

    y1, y2 = int(args.year_start), int(args.year_end)
    max_count = int(str(args.max))
    # mode = args.audiacc # 현재 사용되지 않음
    # days = int(str(args.audiacc_days)) # 현재 사용되지 않음

    # [수정] main 함수 시작 시 session 생성
    session = make_session()
    total_saved = 0
    
    try:
        for y in range(y1, y2+1):
            cds = collect_candidates(y)
            print(f"[{y}] total candidates: {len(cds)}")
            out_dir = os.path.join(MOVIES_DIR, f"{y}")
            ensure_dir(out_dir)

            for i, cd in enumerate(cds):
                if total_saved >= max_count:
                    break
                out = os.path.join(out_dir, f"{cd}.json")
                if os.path.exists(out):
                    continue

                print(f"  -> Fetching {y} ({i+1}/{len(cds)}): {cd}", end='\r')
                
                # [수정] fetch_movie_info에 session 전달
                info, err = fetch_movie_info(session, cd)
                
                if err:
                    if "320011" in err:
                        print(f"\n[warn] {y} {cd}: {err} (stop early)")
                        raise RuntimeError("RATE_LIMIT")
                    print(f"\n[warn] {y} {cd}: {err}")
                    continue
                if not info:
                    continue

                # 상세 정보가 아닌 원본 API 응답 전체를 저장 (기존 로직 유지)
                # movieInfoResult.movieInfo 형태가 되도록 재구성
                save_json(out, {"movieInfoResult": {"movieInfo": info}})
                total_saved += 1
            
            print(f"\n[{y}] year done. Total files saved so far: {total_saved}")

        print(f"\n[DONE] Total newly saved details: {total_saved}")

    except RuntimeError as e:
        if str(e) == "RATE_LIMIT":
            sys.exit(0)
        raise

if __name__ == "__main__":
    main()
