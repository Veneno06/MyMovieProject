"""Experiment 1: a fixed second distinct release-day endpoint, without selection on SP.

Usage: python scripts/analyze_recovery_followup.py INPUT_JSON OUTPUT_JSON
Consumes the frozen paper snapshot. No downloads, source changes, or third-party
dependencies. Scores retain the snapshot's retrospective construction; only the
collaboration histories are restricted to releases before the disclosure month.
"""
from collections import Counter, defaultdict
from datetime import datetime, timezone
import hashlib
import json
import math
from pathlib import Path
import sys


VERSION = '2026-10-23-experiment-1'


def mean(values):
    return math.fsum(values) / len(values) if values else None


def ratio(numerator, denominator):
    return numerator / denominator if numerator is not None and denominator is not None and denominator > 0 else None


def metrics(pool, target, percent=25):
    """Rank all computable partners; retain positive cutoff ties in the high set."""
    ranked = sorted(pool, key=lambda item: (-item['value'], item['id']))
    edge = ranked[math.ceil(len(ranked) * percent / 100) - 1]['value'] if ranked else None
    high = {item['id'] for item in ranked if item['value'] > 0 and item['value'] >= edge - 1e-10}
    ids = {item['id'] for item in ranked}
    matches = [item for item in ranked if item['id'] in high & target]
    return {'M': len(ranked), 'K': len(high), 'n': len(ids & target), 'x': len(matches),
            'matches': matches, 'any': bool(matches), 'cutoff': edge,
            'expected': len(ids & target) * len(high) / len(ranked) if ranked else None}


def frozen_pools(actor, start, observations, actor_pre, films, names):
    """Construct A→B and B→A separately; neither pool gates the other.

    A→B compares B's mean SP with A against B's mean without A.
    B→A compares A's mean SP with B against A's mean without B.
    Joint and without counts count eligible films, not release days.
    """
    history = defaultdict(list)
    for observation in observations:
        if observation['date'] < start:
            history[observation['id']].append(observation['score'])
    joint = defaultdict(lambda: {'a': [], 'b': [], 'dates': []})
    for observation in actor_pre:
        for coactor in films[observation['movie']]:
            if coactor['id'] != actor:
                record = joint[coactor['id']]
                record['a'].append(observation['score'])
                record['b'].append(coactor['score'])
                record['dates'].append(observation['date'])
    answer = {'forward': [], 'reverse': []}
    for partner, record in sorted(joint.items()):
        count = len(record['a'])
        for direction, scored_actor, values in (
                ('forward', partner, record['b']), ('reverse', actor, record['a'])):
            without = len(history[scored_actor]) - count
            without_sum = math.fsum(history[scored_actor]) - math.fsum(values)
            if without <= 0 or without_sum <= 1e-10:
                continue
            with_mean = mean(values)
            without_mean = without_sum / without
            answer[direction].append({
                'id': partner, 'name': names.get(partner, partner),
                'value': 100 * (with_mean / without_mean - 1),
                'joint': count, 'without': without,
                'withMean': with_mean, 'withoutMean': without_mean,
                'lastJoint': max(record['dates']),
            })
    return answer


def analyze(snapshot):
    config = snapshot['config']
    cutoff = str(config['cutoffDate'])
    datetime.strptime(cutoff, '%Y%m%d')
    raw = snapshot['observations']
    checks = []

    def check(name, passed, detail):
        checks.append({'name': name, 'passed': bool(passed), 'detail': detail})

    observations = []
    invalid = 0
    after_cutoff = 0
    missing_identity = 0
    for original in raw:
        observation = dict(original)
        try:
            day = str(observation['date'])
            if len(day) != 8 or datetime.strptime(day, '%Y%m%d').strftime('%Y%m%d') != day:
                raise ValueError('invalid release date')
            score = float(observation['score'])
            if not math.isfinite(score) or score < 0:
                raise ValueError('invalid score')
        except (ValueError, TypeError, KeyError):
            invalid += 1
            continue
        if day > cutoff:
            after_cutoff += 1
            continue
        if not observation.get('id') or not observation.get('movie'):
            missing_identity += 1
            continue
        observation.update(id=str(observation['id']), movie=str(observation['movie']), date=day, score=score)
        observations.append(observation)
    observations.sort(key=lambda item: (item['date'], item['movie'], item['id']))
    duplicate_count = sum(n - 1 for n in Counter((o['id'], o['movie']) for o in observations).values() if n > 1)
    check('valid_dates_and_nonnegative_finite_scores', invalid == 0, {'invalidRecords': invalid})
    check('unique_actor_film_observations', duplicate_count == 0, {'duplicateRecords': duplicate_count})
    films, by_actor = defaultdict(list), defaultdict(list)
    names = {}
    for observation in observations:
        films[observation['movie']].append(observation)
        by_actor[observation['id']].append(observation)
        names[observation['id']] = observation.get('name', observation['id'])
    inconsistent = [movie for movie, rows in films.items() if len({row['date'] for row in rows}) != 1]
    check('one_release_date_per_film', not inconsistent, {'inconsistentFilms': inconsistent})
    events = snapshot['events']
    event_keys = [(str(event['id']), event['eventMonth']) for event in events]
    check('unique_registry_events', len(set(event_keys)) == len(event_keys), {'events': len(events)})
    annual = defaultdict(list)
    for row in snapshot['annual']:
        annual[str(row['id'])].append(row)
    cases, registry = [], []
    for event in events:
        actor = str(event['id'])
        event_month = event['eventMonth']
        datetime.strptime(event_month, '%Y-%m')
        start = event_month.replace('-', '') + '01'
        event_year = int(start[:4])
        pre = [o for o in by_actor[actor] if o['date'] < start]
        pre_high = sorted({int(row['year']) for row in annual[actor]
                           if row.get('usable') and row.get('high') and int(row['year']) < event_year})
        reasons = []
        if event.get('pauseStatus') != 'documented':
            reasons.append('pause_not_documented')
        if len(pre_high) < 2:
            reasons.append('fewer_than_two_usable_pre_event_high_years')
        eligible = not reasons
        base = mean([o['score'] for o in pre])
        case = {**event, 'id': actor, 'eligible': eligible, 'eligibleHistory': eligible,
                'reasons': reasons, 'preMean': base, 'preN': len(pre), 'preHighYears': pre_high,
                'eventYearAnnual': [row for row in annual[actor] if int(row['year']) == event_year],
                'first': None, 'second': None, 'delta': None, 'normalizedDelta': None,
                'ratioToPre': None, 'ratioToFirst': None, 'improved': None,
                'aboveBaseline': None, 'partialRise': None, 'trajectory': [],
                'sources': event.get('sources', []), 'notes': []}
        if eligible:
            pools = frozen_pools(actor, start, observations, pre, films, names)
            case['frozenPoolSummary'] = {direction: metrics(pool, set()) for direction, pool in pools.items()}
            post_days = defaultdict(list)
            for observation in by_actor[actor]:
                if observation['date'][:6] > start[:6]:
                    post_days[observation['date']].append(observation)
            for day, rows in sorted(post_days.items()):
                target = {coactor['id'] for row in rows for coactor in films[row['movie']] if coactor['id'] != actor}
                score = mean([row['score'] for row in rows])
                point = {'date': day,
                         'movies': [{'id': row['movie'], 'title': row.get('title', row['movie'])} for row in rows],
                         'score': score, 'ratioToPre': ratio(score, base), 'coactorCount': len(target),
                         'synergy': {direction: metrics(pool, target) for direction, pool in pools.items()}}
                case['trajectory'].append(point)
            if case['trajectory']:
                case['first'] = case['trajectory'][0]
            if len(case['trajectory']) >= 2:
                case['second'] = case['trajectory'][1]
                first_score, second_score = case['first']['score'], case['second']['score']
                delta = second_score - first_score
                case.update(delta=delta, normalizedDelta=ratio(delta, base),
                            ratioToPre=ratio(second_score, base), ratioToFirst=ratio(second_score, first_score),
                            improved=delta > 0, aboveBaseline=second_score > base if base is not None else None,
                            partialRise=delta > 0 and second_score <= base if base is not None else None)
            if case['first'] is None:
                case['notes'].append('No eligible post-month release observed by cutoff; censored before first and second endpoints.')
            elif case['second'] is None:
                case['notes'].append('No second distinct eligible release day observed by cutoff; second endpoint censored.')
            case['notes'].append('Release timing does not establish post-event casting, a new collaboration decision, or causation.')
            if base is None or base <= 0:
                case['notes'].append('Pre-event mean is unavailable or nonpositive; normalized ratios are null.')
            cases.append(case)
        registry.append({key: value for key, value in case.items()
                         if key not in ('trajectory', 'first', 'second', 'frozenPoolSummary')} |
                        {'firstObserved': case['first'] is not None, 'secondObserved': case['second'] is not None})
    first_observed = [case for case in cases if case['first'] is not None]
    second_observed = [case for case in cases if case['second'] is not None]
    check('post_disclosure_month_only', all(point['date'][:6] > case['eventMonth'].replace('-', '')
          for case in cases for point in case['trajectory']), 'Disclosure-month releases are excluded.')
    check('second_distinct_day_fixed_without_score_gate', all(case['second'] == case['trajectory'][1]
          and case['first']['date'] < case['second']['date'] for case in second_observed),
          'P2 is trajectory position two even when P2 remains below baseline.')
    check('pre_event_high_years_strict', all(len(case['preHighYears']) >= 2 and
          all(year < int(case['eventMonth'][:4]) for year in case['preHighYears']) for case in cases),
          'Usable, high years are deduplicated and strictly precede the event year.')
    check('censored_outcomes_are_null', all(all(case[key] is None for key in
          ('delta', 'normalizedDelta', 'ratioToPre', 'ratioToFirst', 'improved', 'aboveBaseline', 'partialRise'))
          for case in cases if case['second'] is None), 'Missing endpoints never become zero outcomes.')
    check('frozen_directional_pool_sizes', all(
          all(point['synergy'][direction][key] == case['frozenPoolSummary'][direction][key]
              for point in case['trajectory'] for key in ('M', 'K'))
          for case in cases for direction in ('forward', 'reverse')),
          'Both independent directional pools and high sets are fixed before the disclosure month.')
    return {
        'version': VERSION, 'generatedAt': datetime.now(timezone.utc).isoformat(),
        'config': {**config, 'primaryEndpoint': 'second distinct post-month release date',
                   'selection': 'score independent: documented pause and at least two usable high years strictly before event year',
                   'primaryDirection': 'A→B', 'directions': {'forward': 'A→B', 'reverse': 'B→A'},
                   'scoreBasis': 'retrospective snapshot SP; not release-time information',
                   'synergyFreeze': 'strictly before first day of disclosure month for every post-event point',
                   'highSynergyPercent': 25, 'highSynergyRule': 'ceil(25% of independently computable pool); cutoff ties included; value > 0',
                   'sameDayRule': 'union of coactors and mean actor SP across films on each release day',
                   'preMeanRule': 'mean SP across eligible pre-event-month films, not annual means',
                   'eventYearAnnualRole': 'context only; never an eligibility gate',
                   'primaryFdrGate': False, 'currentHawkGate': False, 'experiment2Implemented': False,
                   'newDataCollected': False, 'causalInterpretation': False},
        'counts': {'eligibleFilms': snapshot.get('counts', {}).get('eligibleFilms', len(films)),
                   'filmsWithIdentifiedObservations': len(films),
                   'observations': len(raw), 'idObservations': len(observations),
                   'registryActors': len({str(event['id']) for event in events}), 'historyEligible': len(cases),
                   'firstObserved': len(first_observed), 'secondObserved': len(second_observed),
                   'censoredBeforeFirst': len(cases) - len(first_observed),
                   'censoredBeforeSecond': len(cases) - len(second_observed),
                   'censoredAfterFirstBeforeSecond': len(first_observed) - len(second_observed),
                   'missingIdentityObservations': missing_identity, 'afterCutoffObservationsExcluded': after_cutoff},
        'summary': {'pairedN': len(second_observed),
                    'improved': sum(case['improved'] is True for case in second_observed),
                    'aboveBaseline': sum(case['aboveBaseline'] is True for case in second_observed),
                    'partialRise': sum(case['partialRise'] is True for case in second_observed),
                    'meanDelta': mean([case['delta'] for case in second_observed]),
                    'meanNormalizedDelta': mean([case['normalizedDelta'] for case in second_observed
                                                  if case['normalizedDelta'] is not None])},
        'audit': {'checks': checks, 'passed': all(item['passed'] for item in checks)},
        'cases': cases, 'registry': registry,
    }


def main(argv=None):
    args = sys.argv[1:] if argv is None else argv
    if len(args) != 2:
        raise SystemExit('Usage: python scripts/analyze_recovery_followup.py INPUT_JSON OUTPUT_JSON')
    source, target = map(Path, args)
    if source.resolve() == target.resolve():
        raise SystemExit('Input and output paths must differ.')
    content = source.read_bytes()
    result = analyze(json.loads(content))
    result['provenance'] = {'snapshotSha256': hashlib.sha256(content).hexdigest(),
                            'analysisSha256': hashlib.sha256(Path(__file__).read_bytes()).hexdigest()}
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(json.dumps(result, ensure_ascii=False, indent=2, allow_nan=False) + '\n', encoding='utf-8')
    print(json.dumps({'counts': result['counts'], 'summary': result['summary'], 'auditPassed': result['audit']['passed']}, ensure_ascii=False))
    return 0 if result['audit']['passed'] else 1


if __name__ == '__main__':
    raise SystemExit(main())
