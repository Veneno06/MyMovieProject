import os
import json
import glob
import math
import re
import hashlib
from datetime import date, datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SEARCH_INDEX_PATH = ROOT / "docs" / "data" / "search_index.json"
SENTIMENT_DIR = ROOT / "docs" / "data" / "sentiment"
OUTPUT_PATH = ROOT / "docs" / "data" / "db_summary.json"

STARPOWER_LAMBDA = 12
STARPOWER_CUTOFF = "20260903"
STARPOWER_DOMESTIC_NATIONS = {"한국", "대한민국"}
STARPOWER_SCALE = 10000.0

def _safe_audience(value):
    try:
        return int(str(value or 0).replace(',', '').strip())
    except (TypeError, ValueError):
        return 0

def _normalize_open_date(value):
    digits = ''.join(ch for ch in str(value or '') if ch.isdigit())
    return digits[:8] if len(digits) >= 8 else ''

def is_eligible_starpower_movie(movie):
    open_dt = _normalize_open_date(movie.get('openDt'))
    nation = str(movie.get('nation') or '').strip()
    return bool(
        nation in STARPOWER_DOMESTIC_NATIONS
        and open_dt
        and open_dt <= STARPOWER_CUTOFF
        and _safe_audience(movie.get('audiAcc')) > 0
    )

def _dedupe_eligible_movies(movies):
    unique = {}
    for index, movie in enumerate(movies or []):
        if not is_eligible_starpower_movie(movie):
            continue
        open_dt = _normalize_open_date(movie.get('openDt'))
        movie_cd = str(movie.get('movieCd') or '').strip()
        movie_nm = str(movie.get('movieNm') or '').strip()
        key = f"cd:{movie_cd}" if movie_cd else f"fallback:{movie_nm}|{open_dt}"
        normalized = dict(movie)
        normalized['openDt'] = open_dt
        normalized['audiAcc'] = _safe_audience(movie.get('audiAcc'))
        normalized['actors'] = movie.get('actors') if isinstance(movie.get('actors'), list) else []
        old = unique.get(key)
        if old is None or normalized['audiAcc'] > old['audiAcc'] or (not old['actors'] and normalized['actors']):
            unique[key] = normalized
    return list(unique.values())

def build_star_power_model(movies):
    eligible_movies = _dedupe_eligible_movies(movies)
    year_totals = {}
    for movie in eligible_movies:
        year = movie['openDt'][:4]
        year_totals[year] = year_totals.get(year, 0) + movie['audiAcc']

    base_observations = []
    for movie in eligible_movies:
        year = movie['openDt'][:4]
        total_y = year_totals.get(year, 0)
        if total_y <= 0:
            continue
        market_weight = movie['audiAcc'] / total_y
        seen = set()
        for idx, actor in enumerate(movie.get('actors') or []):
            actor_id = str(actor.get('id') or '').strip()
            actor_name = str(actor.get('name') or '').strip()
            actor_key = actor_id or (f"name:{actor_name}" if actor_name else '')
            if not actor_key or actor_key in seen:
                continue
            seen.add(actor_key)
            role_rank = idx + 1
            role_weight = 1.0 / math.log2(role_rank + 1)
            base_sp = market_weight * role_weight
            if not math.isfinite(base_sp) or base_sp <= 0:
                continue
            base_observations.append({
                'actor_key': actor_key,
                'actor_id': actor_id,
                'actor_name': actor_name,
                'sex': str(actor.get('gender') or '').strip(),
                'movieCd': str(movie.get('movieCd') or '').strip(),
                'movieNm': str(movie.get('movieNm') or '').strip(),
                'openDt': movie['openDt'],
                'year': year,
                'audience': movie['audiAcc'],
                'market_weight': market_weight,
                'role_rank': role_rank,
                'role_weight': role_weight,
                'base_sp': base_sp,
            })

    base_observations.sort(key=lambda r: (r['openDt'], r['movieCd'], r['actor_key']))
    actor_history = {}
    global_past_sum = 0.0
    global_past_count = 0
    observations = []
    observation_by_actor_movie = {}

    pos = 0
    while pos < len(base_observations):
        current_date = base_observations[pos]['openDt']
        end = pos + 1
        while end < len(base_observations) and base_observations[end]['openDt'] == current_date:
            end += 1
        prior_mean = (global_past_sum / global_past_count) if global_past_count > 0 else None

        for row in base_observations[pos:end]:
            hist = actor_history.get(row['actor_key'], {'sum': 0.0, 'count': 0})
            if prior_mean is not None and prior_mean > 0:
                history_estimate = (hist['sum'] + STARPOWER_LAMBDA * prior_mean) / (hist['count'] + STARPOWER_LAMBDA)
                ratio = history_estimate / prior_mean
                history_weight = math.sqrt(ratio) if math.isfinite(ratio) and ratio >= 0 else 1.0
            else:
                history_estimate = row['base_sp']
                history_weight = 1.0
            star_power = STARPOWER_SCALE * row['base_sp'] * history_weight
            obs = dict(row)
            obs.update({
                'history_count': hist['count'],
                'history_sum': hist['sum'],
                'prior_mean': prior_mean,
                'history_estimate': history_estimate,
                'history_weight': history_weight,
                'star_power': star_power,
            })
            observations.append(obs)
            observation_by_actor_movie[(row['actor_key'], row['movieCd'] or f"{row['movieNm']}|{row['openDt']}")] = obs

        for row in base_observations[pos:end]:
            hist = actor_history.setdefault(row['actor_key'], {'sum': 0.0, 'count': 0})
            hist['sum'] += row['base_sp']
            hist['count'] += 1
            global_past_sum += row['base_sp']
            global_past_count += 1
        pos = end

    actor_stats = {}
    for obs in observations:
        stat = actor_stats.setdefault(obs['actor_key'], {
            'id': obs['actor_id'],
            'name': obs['actor_name'],
            'sex': obs['sex'],
            'movie_count': 0,
            'total_star_power': 0.0,
            'total_audi': 0,
        })
        if not stat.get('sex') and obs.get('sex'):
            stat['sex'] = obs['sex']
        stat['movie_count'] += 1
        stat['total_star_power'] += obs['star_power']
        stat['total_audi'] += obs['audience']

    rankings_all = []
    for stat in actor_stats.values():
        if not stat['id']:
            continue
        row = dict(stat)
        career_score = stat['total_star_power']
        average_score = career_score / stat['movie_count'] if stat['movie_count'] else 0.0
        row['score'] = career_score  # backward-compatible actor-wide rank score
        row['career_score'] = career_score
        row['average_score'] = average_score
        row.pop('total_star_power', None)
        rankings_all.append(row)
    rankings_all.sort(key=lambda r: (-r['career_score'], r['name']))

    return {
        'eligible_movies': eligible_movies,
        'year_totals': year_totals,
        'observations': observations,
        'observation_by_actor_movie': observation_by_actor_movie,
        'rankings_all': rankings_all,
    }

def period_start(period):
    """ISO week Monday or calendar-quarter start; malformed labels are invalid."""
    try:
        match = re.fullmatch(r'(\d{4})-W(\d{2})', period)
        if match: return date.fromisocalendar(int(match[1]), int(match[2]), 1)
        match = re.fullmatch(r'(\d{4})-Q([1-4])', period)
        if match: return date(int(match[1]), 3*int(match[2])-2, 1)
    except (ValueError, TypeError): pass
    return None


def select_sentiment_datasets(directory, rankings, movies=None):
    by_id = {str(row['id']):row for row in rankings}
    by_name = {}
    for row in rankings: by_name.setdefault(row['name'], set()).add(str(row['id']))
    if movies is not None:
        for movie in movies:
            for actor in movie.get("actors", []):
                if actor.get("id") and actor.get("name"):
                    by_name.setdefault(actor["name"], set()).add(str(actor["id"]))
    report, selected = {}, {}
    def reject(reason): report[reason] = report.get(reason, 0) + 1
    for path in sorted(Path(directory).glob('*.json')):
        try: data = json.loads(path.read_text(encoding='utf-8'))
        except (ValueError, OSError): reject('invalid_json'); continue
        timeline = data.get('timeline')
        if not isinstance(timeline, dict) or not timeline: reject('empty_timeline'); continue
        if any('-Q' in label for label in timeline) and data.get('date_basis') != 'comment_published_at':
            reject('legacy_video_quarter'); continue
        if not all(period_start(label) is not None for label in timeline): reject('invalid_period'); continue
        if data.get('date_basis') not in (None, 'comment_published_at'):
            reject('invalid_date_basis'); continue
        if path.name.startswith('hawk_analysis_') and data.get('date_basis') != 'comment_published_at':
            reject('unverified_hawk_date_basis'); continue
        if path.name.startswith('hawk_analysis_'):
            reject('hawk_event_sample_separate'); continue
        aid = str(data.get('actor_id') or '')
        if not aid:
            matches = by_name.get(data.get('actor_name'), set())
            if len(matches) != 1: reject('ambiguous_or_unknown_name'); continue
            aid = next(iter(matches))
        if aid not in by_id: reject('unranked_actor'); continue
        if data.get('actor_name') != by_id[aid]['name']: reject('identity_mismatch'); continue
        # General collection is preferred; ID files supersede name mirrors. Never concatenate samples.
        priority = (not path.name.startswith('hawk_analysis_'), path.stem == aid,
                    data.get('schema_version') == 2, str(data.get('last_updated', '')))
        if aid in selected:
            reject('duplicate_actor_dataset')
            if priority <= selected[aid][0]: continue
        selected[aid] = (priority, {**data, 'source_file':path.name,
                                   'date_basis_status':'declared' if data.get('date_basis') else 'legacy_general_comment_weeks'}, by_id[aid])
    return [(rank, data) for _, data, rank in selected.values()], report


def summarize_timeline(timeline):
    total_pos = total_neg = 0
    peak_abs = 0.0; peak_signed = None; peak_date = ''; previous = None
    for label in sorted(timeline, key=lambda key: period_start(key) or date.max):
        dt = period_start(label)
        if dt is None: continue
        counts = timeline[label]
        p, n = max(0, int(counts.get('positive',0))), abs(int(counts.get('negative',0)))
        total_pos += p; total_neg += n
        if previous:
            elapsed_weeks = (dt-previous[0]).days/7
            slope = (n-previous[1])/elapsed_weeks if elapsed_weeks > 0 else 0
            if abs(slope) > peak_abs:
                peak_abs, peak_signed, peak_date = abs(slope), slope, dt.strftime('%Y%m%d')
        previous = (dt,n)
    return {'negTotal':total_neg, 'posTotal':total_pos,
            'negRatio':100*total_neg/(total_pos+total_neg) if total_pos+total_neg else 0,
            'maxSlope':peak_abs, 'peakSignedSlope':peak_signed, 'peakDate':peak_date,
            'slopeUnit':'negative_count_change_per_elapsed_week',
            'peakDateBasis':'bucket_start_not_exact_comment_date'}


def input_fingerprints():
    files = [SEARCH_INDEX_PATH] + sorted(SENTIMENT_DIR.glob('*.json'))
    fingerprints = {str(path.relative_to(ROOT)):hashlib.sha256(path.read_bytes()).hexdigest() for path in files}
    digest = hashlib.sha256(json.dumps(fingerprints, sort_keys=True).encode()).hexdigest()
    return {'search_index_sha256':fingerprints[str(SEARCH_INDEX_PATH.relative_to(ROOT))],
            'inputs_sha256':digest, 'files':fingerprints}


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

    for m in movies:
        y_str = m.get('openDt', '')[:4] or str(m.get('prdtYear', ''))
        
        if not y_str or not y_str.isdigit() or int(y_str) < 2003:
            continue
            
        y = y_str
        audi = _safe_audience(m.get('audiAcc'))
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

    star_power_model = build_star_power_model(movies)
    ranked_all = star_power_model["rankings_all"]
    STARPOWER_YEAR_TOTALS = star_power_model["year_totals"]

    sentiment_actors = []
    global_max_slope = 0
    
    total_pos_all = 0
    total_neg_all = 0
    valid_sentiment_count = 0
    
    datasets, sentiment_exclusions = select_sentiment_datasets(SENTIMENT_DIR, ranked_all, movies)
    for actor_rank_info, s_data in datasets:
        stats = summarize_timeline(s_data['timeline'])
        total_pos_all += stats['posTotal']; total_neg_all += stats['negTotal']
        valid_sentiment_count += 1
        global_max_slope = max(global_max_slope, stats['maxSlope'])
        sentiment_actors.append({
            'id':actor_rank_info['id'], 'name':actor_rank_info['name'],
            'score':actor_rank_info['score'], **stats,
            'rawTimeline':s_data['timeline'], 'source_file':s_data['source_file'],
            'date_basis_status':s_data['date_basis_status']})

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

    summary_data = {
        "generatedAt": datetime.now(timezone.utc).isoformat(),
        "input_fingerprints": input_fingerprints(),
        "sentiment_exclusions": sentiment_exclusions,
        "sentiment_method": {"schema_version": 2, "maxSlope": "absolute magnitude of peakSignedSlope; negative-count change per elapsed week", "peakDate": "actual bucket start YYYYMMDD, not an exact comment date", "missing_periods": "unobserved; never zero-filled", "legacy_general": "unique-name weekly files accepted with explicit legacy provenance", "legacy_hawk": "video-quarter files excluded; recollection required"},
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
        "year_totals": STARPOWER_YEAR_TOTALS,
        "rankings_all": ranked_all,
        "sentiment_actors": sentiment_actors,
        "global_max_slope": global_max_slope,
        "sentiment_global_stats": sentiment_global_stats
    }

    with open(OUTPUT_PATH, 'w', encoding='utf-8') as f:
        json.dump(summary_data, f, ensure_ascii=False, indent=2)

if __name__ == "__main__":
    main()
