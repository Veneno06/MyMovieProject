# scripts/update_data.py
import os, json, requests, datetime, sys, pathlib

# ---- 설정 ---------------------------------------------------------
# GitHub Secrets에 등록한 이름과 일치해야 합니다.
API_KEY = os.getenv("KOFIC_API_KEY")

# target 날짜 강제 지정이 필요하면 환경변수로 YYYYMMDD 넣을 수 있습니다.
# 없으면 KST 기준 '어제'를 사용합니다(일일 박스오피스는 보통 어제 데이터가 안전).
target_env = os.getenv("TARGET_DT")
KST = datetime.timezone(datetime.timedelta(hours=9))
now_kst = datetime.datetime.now(tz=KST)

if target_env and len(target_env) == 8 and target_env.isdigit():
    target = target_env
else:
    target = (now_kst - datetime.timedelta(days=1)).strftime("%Y%m%d")

# 저장 경로 (GitHub Pages가 배포하는 /docs 아래)
OUT_DIR = pathlib.Path("docs/data")
OUT_DIR.mkdir(parents=True, exist_ok=True)

# ---- 수집 ---------------------------------------------------------
if not API_KEY:
    print("ERROR: 환경변수 KOFIC_API_KEY가 없습니다.", file=sys.stderr)
    sys.exit(1)

url = (
    "http://kobis.or.kr/kobisopenapi/webservice/rest/boxoffice/"
    f"searchDailyBoxOfficeList.json?key={API_KEY}&targetDt={target}"
)

try:
    resp = requests.get(url, timeout=30)
    resp.raise_for_status()
    data = resp.json()
except Exception as e:
    print(f"ERROR: KOFIC 요청 실패 -> {e}", file=sys.stderr)
    sys.exit(2)

# ---- 저장 ---------------------------------------------------------
out_file = OUT_DIR / f"{target}.json"
with out_file.open("w", encoding="utf-8") as f:
    json.dump(data, f, ensure_ascii=False, indent=2)

# latest.json 메타 파일도 갱신 (프런트가 '가장 최신'을 쉽게 불러오도록)
latest = {
    "date": target,
    "file": f"{target}.json",
    "url": f"./data/{target}.json"
}
with (OUT_DIR / "latest.json").open("w", encoding="utf-8") as f:
    json.dump(latest, f, ensure_ascii=False, indent=2)

print(f"[OK] Saved: {out_file}")
print(f"[OK] Updated: {OUT_DIR / 'latest.json'}")
