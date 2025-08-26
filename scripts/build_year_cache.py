# scripts/build_year_cache.py
import os, json, requests, pathlib, time
from datetime import datetime

# --- 기본 설정 ----------------------------------------------------
BASES = [
    "https://www.kobis.or.kr",
    "http://kobis.or.kr",
    "http://www.kobis.or.kr",
]
API_PATH = "/kobisopenapi/webservice/rest/movie/searchMovieList.json"
HEADERS = {"User-Agent": "MyMovieProject/1.0 (+github actions)"}

KEY = os.getenv("KOFIC_API_KEY")
if not KEY:
    raise SystemExit("ERROR: KOFIC_API_KEY is missing")

def parse_year(v, default):
    s = str(v or "").strip()
    return int(s) if len(s) == 4 and s.isdigit() else int(default)

CUR = datetime.utcnow().year
START = parse_year(os.getenv("YEAR_START"), CUR - 1)  # 입력 없으면 작년~올해
END   = parse_year(os.getenv("YEAR_END"),   CUR)
ITEMS = 100

out_dir = pathlib.Path("docs/data/years")
out_dir.mkdir(parents=True, exist_ok=True)

# --- 재시도 + 프로토콜 폴백 ---------------------------------------
def request_json(params, tries=6):
    """
    HTTPS가 타임아웃/연결 실패하면 HTTP로 자동 폴백.
    429/5xx는 지수 백오프로 재시도.
    """
    last = None
    for attempt in range(tries):
        base = BASES[attempt % len(BASES)]
        url = f"{base}{API_PATH}"
        try:
            r = requests.get(url, params=params, timeout=30, headers=HEADERS)
            # 재시도 가치가 있는 상태코드
            if r.status_code in (429, 500, 502, 503, 504):
                raise requests.HTTPError(f"retryable status={r.status_code}", response=r)
            return r.json()
        except Exception as e:
            last = e
            sleep = min(2 ** attempt, 15)  # 1,2,4,8,15,15...
            time.sleep(sleep)
    # 마지막 예외 재발생
    raise last

def fetch_year(year: int):
    page, acc, tot = 1, [], None
    while True:
        params = {
            "key": KEY,
            "openStartDt": str(year),
            "openEndDt":   str(year),
            "itemPerPage": str(ITEMS),
            "curPage":     str(page),
        }
        j = request_json(params)
        res = j.get("movieListResult", {})
        lst = res.get("movieList", []) or []
        tot = res.get("totCnt", tot if tot is not None else 0)

        for m in lst:
            if m.get("openDt"):
                m["openDt"] = m["openDt"].replace("-", "")
        acc.extend(lst)

        if len(acc) >= int(tot or 0) or not lst:
            break
        page += 1
        if page > 2000:   # 안전장치
            break
        time.sleep(0.6)   # 매너 지연(과도한 호출 방지)
    return {"year": year, "movieList": acc, "totCnt": len(acc)}

# --- 실행 ---------------------------------------------------------
manifest = {"years": [], "generatedAt": datetime.utcnow().isoformat()}

for y in range(START, END + 1):
    data = fetch_year(y)
    (out_dir / f"year-{y}.json").write_text(
        json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    manifest["years"].append(y)

(out_dir / "_manifest.json").write_text(
    json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8"
)
print("[OK] years:", manifest["years"])
