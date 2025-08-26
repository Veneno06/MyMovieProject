# scripts/build_year_cache.py
import os, json, requests, pathlib

API = "https://www.kobis.or.kr/kobisopenapi/webservice/rest/movie/searchMovieList.json"
KEY = os.getenv("KOFIC_API_KEY")
START = int(os.getenv("YEAR_START", "2024"))
END   = int(os.getenv("YEAR_END", "2025"))  # 포함 범위
ITEMS = 100

out_dir = pathlib.Path("docs/data/years")
out_dir.mkdir(parents=True, exist_ok=True)

def fetch_year(year: int):
  page = 1
  acc = []
  tot = None
  while True:
    params = {
      "key": KEY,
      "openStartDt": str(year),
      "openEndDt":   str(year),
      "itemPerPage": str(ITEMS),
      "curPage":     str(page),
    }
    r = requests.get(API, params=params, timeout=30)
    r.raise_for_status()
    j = r.json()
    res = j.get("movieListResult", {})
    lst = res.get("movieList", []) or []
    tot = res.get("totCnt", tot if tot is not None else 0)
    for m in lst:
      if "openDt" in m and m["openDt"]:
        m["openDt"] = m["openDt"].replace("-", "")
    acc.extend(lst)
    if len(acc) >= int(tot or 0) or not lst:
      break
    page += 1
    if page > 2000:  # 안전장치
      break
  return {"year": year, "movieList": acc, "totCnt": len(acc)}

manifest = {"years": [], "generatedAt": None}

for y in range(START, END + 1):
  data = fetch_year(y)
  (out_dir / f"year-{y}.json").write_text(
    json.dumps(data, ensure_ascii=False, indent=2),
    encoding="utf-8"
  )
  manifest["years"].append(y)

(out_dir / "_manifest.json").write_text(
  json.dumps(manifest, ensure_ascii=False, indent=2),
  encoding="utf-8"
)
print("[OK] years:", manifest["years"])
