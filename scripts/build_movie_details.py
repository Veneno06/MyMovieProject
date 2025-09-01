# scripts/build_movie_details.py
# 목적:
#  - docs/data/years/year-YYYY.json의 영화 후보를 읽어
#    docs/data/movies/{YYYY}/{movieCd}.json 상세 캐시를 구축/보수
#  - 옵션으로 audiAcc(누적 관객수) 선계산: off / recent / all
# 사용 예:
#  python scripts/build_movie_details.py --year-start 2023 --year-end 2024 --max 999999 --audiacc recent --audiacc-days 90

import os, sys, json, time, argparse, re
from datetime import datetime, timedelta
import requests

ROOT = os.path.dirname(os.path.dirname(__file__))
DATA = os.path.join(ROOT, "docs", "data")
YEARS_DIR = os.path.join(DATA, "years")
MOVIES_DIR = os.path.join(DATA, "movies")

KOFIC_KEY = os.environ.get("KOFIC_API_KEY", "").strip()
HEADERS = {"User-Agent": "cache-builder/1.0"}

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

def get(url, timeout=30, sleep=0.13):
    # 작은 딜레이로 API 폭주 방지
    time.sleep(sleep)
    r = requests.get(url, timeout=timeout, headers=HEADERS)
    return r

def norm_ymd(s):
    if not s: return ""
    return re.sub(r"\D", "", str(s))[:8]

def parse_date_ymd(s):
    s = norm_ymd(s)
    if len(s) != 8: return None
    return datetime(int(s[:4]), int(s[4:6]), int(s[6:]))

def fetch_movie_info(movieCd):
    url = f"https://www.kobis.or.kr/kobisopenapi/webservice/rest/movie/searchMovieInfo.json?key={KOFIC_KEY}&movieCd={movieCd}"
    r = get(url)
    if r.status_code != 200:
        return None, f"http={r.status_code}"
    j = r.json()
    if j.get("faultInfo") or j.get("faultResult"):
        return None, f"fault={j.get('faultInfo') or j.get('faultResult')}"
    info = (j.get("movieInfoResult") or {}).get("movieInfo")
    return info, None

def fetch_weekly_audi_acc(movieCd, openDtYMD, weeks=8):
    """
    주간 박스오피스에서 해당 작품의 audiAcc 최대값을 찾는다.
    API 호출량이 크므로 옵션으로만 사용.
    """
    if not KOFIC_KEY: return None
    base = parse_date_ymd(openDtYMD) or datetime.now()
    base = base + timedelta(days=3)  # 개봉 주 주간 집계 시점 근사
    max_acc = None
    for i in range(weeks):
        d = (base + timedelta(weeks=i))
        td = d.strftime("%Y%m%d")
        url = f"https://www.kobis.or.kr/kobisopenapi/webservice/rest/boxoffice/searchWeeklyBoxOfficeList.json?key={KOFIC_KEY}&targetDt={td}&weekGb=0"
        r = get(url)
        if r.status_code != 200:
            continue
        try:
            js = r.json()
        except:
            continue
        if js.get("faultInfo") or js.get("faultResult"):
            # 쿼터 초과(320011) 등: 바로 중단 -> 상위 로직에서 안전 종료
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
    return max_acc

def collect_candidates(year):
    # year-YYYY.json에서 후보 수집(없으면 빈 리스트)
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
    mode = args.audiacc
    days = int(str(args.audiacc_days))

    total_saved = 0
    try:
        for y in range(y1, y2+1):
            cds = collect_candidates(y)
            print(f"[{y}] total candidates: {len(cds)}")
            out_dir = os.path.join(MOVIES_DIR, f"{y}")
            ensure_dir(out_dir)

            for cd in cds:
                if total_saved >= max_count:
                    break
                out = os.path.join(out_dir, f"{cd}.json")
                if os.path.exists(out):
                    # 이미 있으면 스킵(필요 시 보수/재계산만 수행)
                    continue

                info, err = fetch_movie_info(cd)
                if err:
                    # 쿼터 초과로 의심되면 바로 정지
                    if "320011" in err:
                        print(f"[warn] {y} {cd}: {err} (stop early)")
                        raise RuntimeError("RATE_LIMIT")
                    print(f"[warn] {y} {cd}: {err}")
                    continue
                if not info:
                    continue

                # 기본 필드 가공
                openDt = norm_ymd(info.get("openDt"))
                rec = {
                    "movieCd": info.get("movieCd"),
                    "movieNm": info.get("movieNm"),
                    "openDt": openDt,
                    "showTm": info.get("showTm"),
                    "audits": info.get("audits") or [],
                    "genres": info.get("genres") or [],
                    "directors": info.get("directors") or [],
                    "actors": info.get("actors") or [],
                    "companys": info.get("companys") or [],
                }

                # audiAcc 선계산 (옵션)
                if mode != "off" and KOFIC_KEY:
                    allow = (mode == "all")
                    if mode == "recent" and openDt:
                        try:
                            d = parse_date_ymd(openDt)
                            if d and (datetime.now() - d).days <= days:
                                allow = True
                        except:
                            pass
                    if allow:
                        try:
                            acc = fetch_weekly_audi_acc(rec["movieCd"], openDt, weeks=8)
                            if isinstance(acc, int): rec["audiAcc"] = acc
                        except RuntimeError as e:
                            if str(e) == "RATE_LIMIT":
                                print("[warn] RATE_LIMIT during audiAcc. Stop now.")
                                raise

                save_json(out, rec)
                total_saved += 1

        print(f"[DONE] saved/migrated details: {total_saved}")

    except RuntimeError as e:
        # 쿼터 초과시라도 인덱싱 단계는 계속 돌 수 있도록 0으로 종료
        if str(e) == "RATE_LIMIT":
            # 에러 대신 정상 종료로 간주
            sys.exit(0)
        raise

if __name__ == "__main__":
    main()
