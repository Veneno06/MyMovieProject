import os
import json
import glob
import math
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SEARCH_INDEX_PATH = ROOT / "docs" / "data" / "search_index.json"
SENTIMENT_DIR = ROOT / "docs" / "data" / "sentiment"
OUTPUT_PATH = ROOT / "docs" / "data" / "db_summary.json"

def main():
    if not SEARCH_INDEX_PATH.exists():
        return

    with open(SEARCH_INDEX_PATH, 'r', encoding='utf-8') as f:
        movies = json.load(f)

    yearly_stats = {}
    total_movies, total_dom_movies, total_for_movies = 0, 0, 0
    cumulative_audience = 0
    dom_actors, for_actors, dom_male, dom_female = set(), set(), set(), set()
    dom_directors, for_directors = set(), set()
    YEAR_TOTALS = {}

    # 1. 영화 순회 및 2003년 필터링 (강제 적용)
    for m in movies:
        y_str = m.get('openDt', '')[:4] or str(m.get('prdtYear', ''))
        
        # 핵심: 2003년 이전 데이터이거나 연도가 없는 데이터는 철저히 배제
        if not y_str or not y_str.isdigit() or int(y_str) < 2003:
            continue
            
        y = y_str
        audi = int(str(m.get('audiAcc', 0)).replace(',', ''))
        is_k = (m.get('nation') == '한국' or m.get('repNation') == 'K')

        if audi > 0:
            YEAR_TOTALS[y] = YEAR_TOTALS.get(y, 0) + audi

        if y not in yearly_stats:
            yearly_stats[y] = {"dom_movies": 0, "for_movies": 0, "dom_audi": 0, "for_audi": 0}
            
        if is_k:
            yearly_stats[y]["dom_movies"] += 1
            yearly_stats[y]["dom_audi"] += audi
            total_dom_movies += 1
        else:
            yearly_stats[y]["for_movies"] += 1
            yearly_stats[y]["for_audi"] += audi
            total_for_movies += 1

        total_movies += 1
        cumulative_audience += audi

        for a in m.get('actors', []):
            aid = a.get('id') or a.get('name')
            gender = a.get('gender', '').strip()
            if is_k:
                dom_actors.add(aid)
                if gender in ["남", "남자"]: dom_male.add(aid)
                elif gender in ["여", "여자"]: dom_female.add(aid)
            else:
                for_actors.add(aid)
                
        for d in m.get('directors', []):
            did = d.get('id') or d.get('name')
            if is_k: dom_directors.add(did)
            else: for_directors.add(did)

    for_actors = for_actors - dom_actors
    for_directors = for_directors - dom_directors

    # 2. 스타 파워 랭킹(SP_Final) 계산 (2003년 이후 데이터만)
    ACTOR_SCORES = {}
    for m in movies:
        y_str = m.get('openDt', '')[:4] or str(m.get('prdtYear', ''))
        if not y_str or not y_str.isdigit() or int(y_str) < 2003:
            continue
            
        y = y_str
        audi = int(str(m.get('audiAcc', 0)).replace(',', ''))
        if audi <= 0: continue
        
        total_y = YEAR_TOTALS[y]
        w_time = max(0.1, (int(y) - 2000) / 10)
        actors = m.get('actors', [])
        c_i = max(1, len(actors))

        for idx, a in enumerate(actors):
            aid = a.get('id')
            aname = a.get('name')
            if not aid: continue
            
            w_role = 1.0 if idx < math.ceil(c_i / 3) else 0.5
            score_i = (audi / total_y) * (w_role / math.sqrt(c_i)) * w_time * 10000

            if aid not in ACTOR_SCORES:
                ACTOR_SCORES[aid] = {'id': aid, 'name': aname, 'score': 0, 'sex': a.get('gender'), 'total_audi': 0}
            ACTOR_SCORES[aid]['score'] += score_i
            ACTOR_SCORES[aid]['total_audi'] += audi

    ranked_all = sorted(ACTOR_SCORES.values(), key=lambda x: x['score'], reverse=True)

    # 3. 여론 데이터 스캔 및 글로벌 평균 통계 생성
    sentiment_actors = []
    global_max_slope = 0
    
    total_pos_all = 0
    total_neg_all = 0
    valid_sentiment_count = 0
    
    sentiment_files = glob.glob(str(SENTIMENT_DIR / "*.json"))
    for pf in sentiment_files:
        with open(pf, 'r', encoding='utf-8') as sf:
            s_data = json.load(sf)
            name = s_data.get('actor_name')
            timeline = s_data.get('timeline', {})
            
            actor_rank_info = next((item for item in ranked_all if item["name"] == name), None)
            if not actor_rank_info or not timeline: continue

            total_pos, total_neg, temp_max_slope = 0, 0, 0
            prev_neg = None
            peak_date = ""

            for w in sorted(timeline.keys()):
                p = timeline[w].get('positive', 0)
                n = abs(timeline[w].get('negative', 0))
                total_pos += p
                total_neg += n
                
                if prev_neg is not None:
                    slope = abs(n - prev_neg)
                    if slope > temp_max_slope:
                        temp_max_slope = slope
                        peak_date = w.split('-W')[0] + "0601"
                prev_neg = n
                
            if temp_max_slope > global_max_slope:
                global_max_slope = temp_max_slope
                
            total_comments = total_pos + total_neg
            neg_ratio = (total_neg / total_comments * 100) if total_comments > 0 else 0
            
            total_pos_all += total_pos
            total_neg_all += total_neg
            valid_sentiment_count += 1
            
            sentiment_actors.append({
                "id": actor_rank_info['id'],
                "name": name,
                "score": actor_rank_info['score'],
                "negTotal": total_neg,
                "negRatio": neg_ratio,
                "maxSlope": temp_max_slope,
                "peakDate": peak_date,
                "rawTimeline": timeline
            })
            
    sentiment_actors.sort(key=lambda x: x['score'], reverse=True)

    avg_pos = total_pos_all / valid_sentiment_count if valid_sentiment_count > 0 else 0
    avg_neg = total_neg_all / valid_sentiment_count if valid_sentiment_count > 0 else 0
    avg_total_comments = avg_pos + avg_neg
    avg_neg_ratio = (avg_neg / avg_total_comments * 100) if avg_total_comments > 0 else 0
    
    ratio_pos_str = f"{(avg_pos / avg_total_comments * 100):.1f}" if avg_total_comments > 0 else "0"
    ratio_neg_str = f"{avg_neg_ratio:.1f}"

    sentiment_global_stats = {
        "avg_pos": round(avg_pos),
        "avg_neg": round(avg_neg),
        "avg_neg_ratio": round(avg_neg_ratio, 1),
        "ratio_str": f"{ratio_pos_str} : {ratio_neg_str}"
    }

    # 4. 최종 JSON 출력
    summary_data = {
        "generatedAt": os.popen("date -u +'%Y-%m-%dT%H:%M:%SZ'").read().strip() if os.name != 'nt' else "",
        "db_stats": {
            "total_movies": total_movies,
            "total_dom_movies": total_dom_movies,
            "total_for_movies": total_for_movies,
            "cumulative_audience": cumulative_audience,
            "actors": {
                "total": len(dom_actors) + len(for_actors),
                "domestic": len(dom_actors),
                "foreign": len(for_actors),
                "dom_male": len(dom_male),
                "dom_female": len(dom_female)
            },
            "directors": {
                "domestic": len(dom_directors),
                "foreign": len(for_directors)
            }
        },
        "yearly_stats": yearly_stats,
        "year_totals": YEAR_TOTALS,
        "rankings_all": ranked_all,
        "sentiment_actors": sentiment_actors,
        "global_max_slope": global_max_slope,
        "sentiment_global_stats": sentiment_global_stats
    }

    with open(OUTPUT_PATH, 'w', encoding='utf-8') as f:
        json.dump(summary_data, f, ensure_ascii=False, indent=2)

if __name__ == "__main__":
    main()
