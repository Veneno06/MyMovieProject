# scripts/build_year_cache.py
import os, json, time, argparse, pathlib, sys
import requests

BASE = "https://www.kobis.or.kr/kobisopenapi/webservice/rest/movie/searchMovieList.json"

def fetch_year(year: int, key: str, per_page: int = 100, max_pages: int = 200, delay: float = 0.15):
    acc = []
    page = 1
    while True:
        params = {
            "key": key,
            "openStartDt": str(year),
            "openEndDt": str(year),
            "itemPerPage": str(per_page),
            "curPage": str(page),
        }
        r = requests.get(BASE, params=params, timeout=30)
        r.raise_for_status()
        data = r.json()

        lst = (data.get("movieListResult") or {}).get("movieList") or []
        # openDt 정규화(YYYYMMDD)
        for m in lst:
            m["openDt"] = (m.get("openDt") or "").replace("-", "")
        acc.extend(lst)

        tot = (data.get("movieListResult") or {}).get("totCnt")
        if tot is None:
            tot = len(acc)

        if len(acc) >= int(tot):
            break
        page += 1
        if page > max_pages:
            break
        time.sleep(delay)

    # 고유 movieCd 목록
    codes = sorted({m.get("movieCd") for m in acc if m.get("movieCd")})
    out = {
        "year": year,
        "totCnt": len(codes),
        "movieCds": codes,     # 상세 스크립트에서 쓰기 쉬움
        "movieList": acc,      # 참고용(필요시)
    }
    return out

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--year-start", required=True, type=int)
    ap.add_argument("--year-end", required=True, type=int)
    args = ap.parse_args()

    key = os.getenv("KOFIC_API_KEY", "").strip()
    if not key:
        print("ERROR: KOFIC_API_KEY is missing (GitHub Actions secret)", file=sys.stderr)
        sys.exit(1)

    root = pathlib.Path("docs/data/years")
    root.mkdir(parents=True, exist_ok=True)

    for y in range(args.year_start, args.year_end + 1):
        print(f"[year-cache] fetching {y} ...", flush=True)
        out = fetch_year(y, key)
        p = root / f"year-{y}.json"
        with p.open("w", encoding="utf-8") as f:
            json.dump(out, f, ensure_ascii=False, indent=2)
        print(f"[year-cache] saved {p} (totCnt={out['totCnt']})")

if __name__ == "__main__":
    main()
