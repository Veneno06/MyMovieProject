# scripts/build_year_cache.py
import os, sys, json, time, argparse, pathlib, calendar
import requests

BASE = "https://www.kobis.or.kr/kobisopenapi/webservice/rest/movie/searchMovieList.json"

def call_kobis(params: dict) -> dict:
    r = requests.get(BASE, params=params, timeout=30)
    r.raise_for_status()
    data = r.json()
    # KOFIC가 에러를 JSON으로 돌려주는 경우
    if "faultInfo" in data or "faultResult" in data:
        # 키는 로그에 노출하지 않음
        msg = json.dumps(data, ensure_ascii=False)
        raise RuntimeError(f"KOBIS fault: {msg}")
    return data

def fetch_range(key: str, start: str, end: str, per_page=100, max_pages=300, delay=0.15):
    """openStartDt=start(YYYYMMDD/ YYYY), openEndDt=end 로 전 기간 페이지네이션 수집"""
    acc = []
    page = 1
    while True:
        params = {
            "key": key,
            "openStartDt": start,
            "openEndDt": end,
            "itemPerPage": str(per_page),
            "curPage": str(page),
        }
        data = call_kobis(params)
        lst = (data.get("movieListResult") or {}).get("movieList") or []
        # openDt 정규화(YYYYMMDD)
        for m in lst:
            m["openDt"] = (m.get("openDt") or "").replace("-", "")
        acc.extend(lst)

        tot = (data.get("movieListResult") or {}).get("totCnt")
        if tot is None:  # 방어
            tot = len(acc)

        if len(acc) >= int(tot):
            break
        page += 1
        if page > max_pages:
            break
        time.sleep(delay)
    return acc

def fetch_year_all(key: str, year: int) -> list:
    """1) 연-단위 조회 시도 → 0건이면 2) 월별 쪼개기 폴백"""
    # 1) 연-단위
    acc = fetch_range(key, str(year), str(year))
    if len(acc) > 0:
        return acc

    # 2) 월별 폴백
    print(f"[year-cache] year {year} returned 0; fallback to monthly windows...", flush=True)
    seen = {}
    for m in range(1, 13):
        last = calendar.monthrange(year, m)[1]
        start = f"{year}{m:02}01"
        end   = f"{year}{m:02}{last:02}"
        chunk = fetch_range(key, start, end)
        for it in chunk:
            cd = it.get("movieCd")
            if cd and cd not in seen:
                seen[cd] = it
        time.sleep(0.1)
    return list(seen.values())

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--year-start", required=True, type=int)
    ap.add_argument("--year-end",   required=True, type=int)
    args = ap.parse_args()

    key = os.getenv("KOFIC_API_KEY", "").strip()
    if not key:
        print("ERROR: KOFIC_API_KEY is missing (GitHub Actions secret).", file=sys.stderr)
        sys.exit(1)

    out_dir = pathlib.Path("docs/data/years")
    out_dir.mkdir(parents=True, exist_ok=True)

    for y in range(args.year_start, args.year_end + 1):
        print(f"[year-cache] fetching {y} ...", flush=True)
        try:
            items = fetch_year_all(key, y)
        except Exception as e:
            print(f"[year-cache] ERROR for {y}: {e}", file=sys.stderr)
            sys.exit(1)

        codes = sorted({m.get("movieCd") for m in items if m.get("movieCd")})
        payload = {
            "year": y,
            "totCnt": len(codes),
            "movieCds": codes,
            "movieList": items,   # 참고용
        }
        path = out_dir / f"year-{y}.json"
        with path.open("w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)
        print(f"[year-cache] saved {path} (totCnt={payload['totCnt']})", flush=True)

if __name__ == "__main__":
    main()
