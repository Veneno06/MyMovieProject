#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
import json
import os
from pathlib import Path
from time import sleep
from typing import Any, Dict, List, Optional, Tuple
import requests

ROOT = Path(__file__).resolve().parents[1]  # repo root
DATA = ROOT / "docs" / "data"
MOVIES_DIR = DATA / "movies"
YEARS_DIR = DATA / "years"

API_BASE = "https://www.kobis.or.kr/kobisopenapi/webservice/rest"
API_KEY = os.environ.get("KOFIC_API_KEY", "").strip()

SESSION = requests.Session()
SESSION.headers.update({"User-Agent": "MyMovieProject/1.0"})
TIMEOUT = 30


# ---------- HTTP & 오류 처리 ----------
class QuotaError(Exception):
    """KOFIC 일일 쿼터 초과 (errorCode 320011)"""


def _get(url: str, params: Dict[str, str]) -> Dict[str, Any]:
    """HTTP GET + fault 검사 + 소프트 재시도."""
    last_err = None
    for attempt in range(3):
        try:
            r = SESSION.get(url, params=params, timeout=TIMEOUT)
            txt = r.text
            if r.status_code != 200:
                raise RuntimeError(f"HTTP {r.status_code}: {txt[:300]}")
            js = json.loads(txt)
            # KOFIC fault 응답
            fault = js.get("faultInfo") or js.get("faultResult")
            if fault:
                # errorCode: '320011' → 하루 이용량 초과
                code = str(fault.get("errorCode") or "")
                if code == "320011":
                    raise QuotaError(f"quota: {fault}")
                # 기타 fault는 일반 오류로 올림
                raise RuntimeError(f"fault: {fault}")
            return js
        except QuotaError:
            # 쿼터 초과는 즉시 상위로
            raise
        except Exception as e:
            last_err = e
            # 네트워크/일시 오류는 짧게 재시도
            sleep(1 + attempt)
    # 3회 실패
    if last_err:
        raise last_err
    return {}


# ---------- 보조 유틸 ----------
def norm_ymd(s: Optional[str]) -> str:
    if not s:
        return ""
    return "".join(ch for ch in str(s) if ch.isdigit())[:8]


def y_from_open_dt(open_dt_ymd: str) -> str:
    return open_dt_ymd[:4] if len(open_dt_ymd) >= 4 else "unknown"


def ensure_dir(p: Path) -> None:
    p.mkdir(parents=True, exist_ok=True)


def movie_detail_path(movie_cd: str, open_dt_ymd: str) -> Path:
    y = y_from_open_dt(open_dt_ymd)
    return MOVIES_DIR / y / f"{movie_cd}.json"


# ---------- KOFIC 호출 ----------
def fetch_movie_info(movie_cd: str) -> Dict[str, Any]:
    js = _get(
        f"{API_BASE}/movie/searchMovieInfo.json",
        {"key": API_KEY, "movieCd": movie_cd},
    )
    return js.get("movieInfoResult", {}).get("movieInfo", {}) or {}


def compute_audi_acc(movie_cd: str, open_dt_ymd: str, weeks: int = 12) -> Optional[int]:
    """
    주간 박스오피스(weekGb=0)로 최대 누적 관객수(audiAcc)를 12주 스캔.
    데이터가 없으면 개봉일 일간 박스오피스로 보정.
    """
    # 개봉일 +3일을 기준으로 주간 스캔
    if open_dt_ymd and len(open_dt_ymd) == 8:
        y, m, d = int(open_dt_ymd[:4]), int(open_dt_ymd[4:6]), int(open_dt_ymd[6:8])
        from datetime import date, timedelta
        base = date(y, m, d) + timedelta(days=3)
    else:
        from datetime import date
        base = date.today()

    best = None
    from datetime import timedelta
    for i in range(weeks):
        target = base + timedelta(days=i * 7)
        target_ymd = f"{target.year:04d}{target.month:02d}{target.day:02d}"
        js = _get(
            f"{API_BASE}/boxoffice/searchWeeklyBoxOfficeList.json",
            {"key": API_KEY, "targetDt": target_ymd, "weekGb": "0"},
        )
        items = js.get("boxOfficeResult", {}).get("weeklyBoxOfficeList", [])
        hit = next((x for x in items if x.get("movieCd") == movie_cd), None)
        if hit and hit.get("audiAcc"):
            val = int(str(hit["audiAcc"]).replace(",", ""))
            best = max(best or val, val)
        sleep(0.15)  # 과호출 방지

    if best is not None:
        return best

    # 보정: 개봉일의 일간 박스오피스
    if open_dt_ymd:
        js = _get(
            f"{API_BASE}/boxoffice/searchDailyBoxOfficeList.json",
            {"key": API_KEY, "targetDt": open_dt_ymd},
        )
        items = js.get("boxOfficeResult", {}).get("dailyBoxOfficeList", [])
        hit = next((x for x in items if x.get("movieCd") == movie_cd), None)
        if hit and hit.get("audiAcc"):
            return int(str(hit["audiAcc"]).replace(",", ""))
    return None


# ---------- 연도 후보 / 저장 ----------
def read_year_candidates(year: int) -> List[str]:
    """year-YYYY.json 에 movieCds 있으면 그걸 쓰고, 없으면 movies/<year> 스캔."""
    path = YEARS_DIR / f"year-{year}.json"
    if path.exists():
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
            cds = data.get("movieCds") or []
            if cds:
                return [str(c) for c in cds]
        except Exception:
            pass

    # fallback: 이미 저장된 캐시에서 스캔
    folder = MOVIES_DIR / f"{year}"
    if folder.exists():
        return [p.stem for p in folder.glob("*.json")]
    return []


def save_json(path: Path, data: Dict[str, Any]) -> None:
    ensure_dir(path.parent)
    tmp = path.with_suffix(".json.tmp")
    tmp.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp.replace(path)


# ---------- 메인 빌드 ----------
def build_for_year(year: int, max_count: int) -> Tuple[int, int, bool]:
    """
    return (total_candidates, saved_or_updated, stopped_by_quota)
    """
    cand = read_year_candidates(year)
    total = len(cand)
    saved = 0
    print(f"[{year}] total candidates: {total}")

    if total == 0:
        return 0, 0, False

    for i, movie_cd in enumerate(cand, 1):
        if i > max_count:
            break

        # 이미 audiAcc가 들어간 캐시가 있으면 건너뛴다(재개/절약)
        existed = False
        for p in (MOVIES_DIR / str(year)).glob(f"{movie_cd}.json"):
            try:
                cur = json.loads(p.read_text(encoding="utf-8"))
                if isinstance(cur.get("audiAcc"), int):
                    existed = True
                    break
            except Exception:
                pass
        if existed:
            continue

        try:
            info = fetch_movie_info(movie_cd)
            open_dt = norm_ymd(info.get("openDt"))
            path = movie_detail_path(movie_cd, open_dt or str(year))
            payload = {
                "movieCd": movie_cd,
                "movieNm": info.get("movieNm", ""),
                "openDt": open_dt,
                "prdtYear": info.get("prdtYear", ""),
                "movieInfo": info,
            }

            # audiAcc 선계산
            acc = compute_audi_acc(movie_cd, open_dt) if API_KEY else None
            if acc is not None:
                payload["audiAcc"] = acc

            save_json(path, payload)
            saved += 1

        except QuotaError as qe:
            print(f"[quota] exhausted: {qe}")
            # 쿼터 초과 → 즉시 연도 처리 중단
            return total, saved, True
        except Exception as e:
            print(f"[warn] {year} {movie_cd}: {e}")
            sleep(0.5)

        sleep(0.15)

    return total, saved, False


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--year-start", required=True, type=int)
    ap.add_argument("--year-end", required=True, type=int)
    ap.add_argument("--max", default="999999", type=str)
    args = ap.parse_args()

    max_count = int(args.max) if str(args.max).isdigit() else 999_999

    ensure_dir(MOVIES_DIR)
    ensure_dir(YEARS_DIR)

    total_saved = 0
    for y in range(args.year_start, args.year_end + 1):
        tot, saved, stopped = build_for_year(y, max_count)
        total_saved += saved
        if stopped:
            print(f"[STOP] quota exhausted while processing {y}.")
            break

    print(f"[DONE] saved/migrated details: {total_saved}")


if __name__ == "__main__":
    if not API_KEY:
        print("WARNING: KOFIC_API_KEY is empty. 'audiAcc' computation will be skipped.")
    main()
