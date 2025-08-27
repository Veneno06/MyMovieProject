import os, json, requests, pathlib, time
from urllib.parse import urljoin

KOBIS_BASES = [
    "https://www.kobis.or.kr",
    "http://kobis.or.kr",
    "http://www.kobis.or.kr",
]
INFO_PATH = "/kobisopenapi/webservice/rest/movie/searchMovieInfo.json"

KEY = os.getenv("KOFIC_API_KEY")
if not KEY:
    raise SystemExit("ERROR: KOFIC_API_KEY missing")

YEAR_START = int(os.getenv("YEAR_START", "2024"))
YEAR_END   = int(os.getenv("YEAR_END",   "2024"))
ONLY_MISSING = os.getenv("ONLY_MISSING", "true").lower() != "false"
MAX_COUNT = int(os.getenv("MAX_COUNT", "999999"))

ROOT = pathlib.Path("docs/data")
YEARS_DIR = ROOT / "years"
MOVIES_DIR = ROOT / "movies"
MOVIES_DIR.mkdir(parents=True, exist_ok=True)

def request_json(url, params, tries=6):
    last = None
    for i in range(tries):
        try:
            r = requests.get(url, params=params, timeout=30, headers={"User-Agent":"MyMovieProject/1.0"})
            if r.status_code in (429, 500, 502, 503, 504):
                raise requests.HTTPError(f"retryable {r.status_code}", response=r)
            return r.json()
        except Exception as e:
            last = e
            time.sleep(min(2**i, 15))
    raise last

def fetch_detail(movie_cd: str):
    for base in KOBIS_BASES:
        url = urljoin(base, INFO_PATH)
        try:
            return request_json(url, {"key": KEY, "movieCd": movie_cd})
        except Exception:
            continue
    raise SystemExit(f"Failed all bases for movieCd={movie_cd}")

def load_year(year: int):
    p = YEARS_DIR / f"year-{year}.json"
    if not p.exists():
        print(f"[WARN] missing {p}, skip")
        return []
    j = json.loads(p.read_text(encoding="utf-8"))
    return j.get("movieList", [])

count = 0
for y in range(YEAR_START, YEAR_END+1):
    items = load_year(y)
    for m in items:
        movie_cd = str(m.get("movieCd"))
        if not movie_cd: 
            continue
        out = MOVIES_DIR / f"{movie_cd}.json"
        if ONLY_MISSING and out.exists():
            continue
        data = fetch_detail(movie_cd)
        out.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
        count += 1
        print("[OK]", y, movie_cd, "->", out)
        time.sleep(0.5)  # 과호출 방지
        if count >= MAX_COUNT:
            break
    if count >= MAX_COUNT:
        break
print(f"[DONE] saved details: {count}")
