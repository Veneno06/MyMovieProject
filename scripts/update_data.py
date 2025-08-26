import requests
import datetime
import os
import json

api_key = os.getenv("KOFIC_API_KEY")  # GitHub Actions에서 시크릿으로 불러옴
today = datetime.datetime.now().strftime("%Y%m%d")

url = f"http://kobis.or.kr/kobisopenapi/webservice/rest/boxoffice/searchDailyBoxOfficeList.json?key={api_key}&targetDt={today}"

res = requests.get(url)
data = res.json()

os.makedirs("data", exist_ok=True)
with open(f"data/{today}.json", "w", encoding="utf-8") as f:
    json.dump(data, f, ensure_ascii=False, indent=2)