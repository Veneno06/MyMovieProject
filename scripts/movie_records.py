"""Deterministic local snapshot merger shared by every offline indexer.

Positive cumulative audience uses the maximum observed snapshot, NOT audience as
of release/event date. Unknown/zero/invalid audience becomes None. Cast is chosen
whole from one richest source; arrays are never unioned to invent billing ranks.
Lexical source paths break ties; conflicts and field provenance remain auditable.
"""
import copy
import json
from pathlib import Path


def movie_info(data):
    if not isinstance(data, dict):
        return {}
    if data.get('movieCd'):
        return data
    return (data.get('movieInfoResult') or {}).get('movieInfo') or {}


def audience(value):
    try:
        n = int(str(value).replace(',', '').strip())
        return n if n > 0 else None
    except (TypeError, ValueError):
        return None


def _populated(value):
    return value is not None and value != '' and value != [] and value != {}


def _richness(value):
    if isinstance(value, list):
        return (len(value), sum(sum(_populated(v) for v in x.values()) for x in value if isinstance(x, dict)))
    if isinstance(value, dict):
        return (len(value), 0)
    return (1, 0)


def merge_sources(sources):
    sources = sorted(sources, key=lambda x: str(x[0]))
    nodes = []
    for path, raw in sources:
        node = copy.deepcopy(movie_info(raw))
        if not node.get('movieCd'):
            continue
        # Some enhanced raw wrappers carry audience outside movieInfo.
        vals = [audience(node.get('audiAcc')), audience(raw.get('audiAcc'))]
        node['audiAcc'] = max((v for v in vals if v is not None), default=None)
        nodes.append((str(path), node))
    if not nodes:
        return {}, {'sources': [], 'fields': {}, 'conflicts': {}}
    ids = {str(n['movieCd']) for _, n in nodes}
    if len(ids) != 1:
        raise ValueError('merge_sources requires one movieCd')
    out, fields, conflicts = {}, {}, {}
    for key in sorted({k for _, n in nodes for k in n}):
        candidates = [(p, n[key]) for p, n in nodes if key in n and _populated(n[key])]
        if not candidates:
            continue
        if key == 'audiAcc':
            selected = max(candidates, key=lambda x: x[1])
        elif isinstance(candidates[0][1], (list, dict)):
            selected = max(candidates, key=lambda x: _richness(x[1]))
        else:
            selected = candidates[0]
        fields[key], out[key] = selected[0], copy.deepcopy(selected[1])
        if len({json.dumps(v, sort_keys=True, ensure_ascii=False) for _, v in candidates}) > 1:
            conflicts[key] = [{'source': p, 'value': v} for p, v in candidates]
    out['movieCd'] = next(iter(ids))
    out.setdefault('audiAcc', None)
    return out, {'sources': [p for p, _ in nodes], 'fields': fields, 'conflicts': conflicts}


def load_movie_records(directory):
    directory = Path(directory)
    groups, invalid = {}, []
    file_count = 0
    for path in sorted(directory.rglob('*.json')):
        file_count += 1
        rel = path.relative_to(directory).as_posix()
        try:
            raw = json.loads(path.read_text(encoding='utf-8'))
            code = str(movie_info(raw).get('movieCd') or '').strip()
            if not code:
                raise ValueError('missing movieCd')
            groups.setdefault(code, []).append((rel, raw))
        except (ValueError, TypeError, AttributeError) as exc:
            invalid.append({'source': rel, 'error': str(exc)})
    records, entries = [], {}
    years = {}
    for code, sources in sorted(groups.items()):
        merged, audit = merge_sources(sources)
        records.append(merged)
        entries[code] = audit
        before = movie_info(sorted(sources)[0][1])
        # Reproduce legacy reindex root-or-nested selection for coverage comparison.
        raw = sorted(sources)[0][1]
        before_audi = audience(raw.get('audiAcc') or before.get('audiAcc') or 0)
        year = str(merged.get('openDt') or '')[:4] or 'unknown'
        stats = years.setdefault(year, {'movies': 0, 'before_positive': 0, 'after_positive': 0, 'recovered_movie_ids': [], 'korean_movies': 0, 'korean_before_positive': 0, 'korean_after_positive': 0, 'korean_recovered_movie_ids': []})
        stats['movies'] += 1
        stats['before_positive'] += int(before_audi is not None)
        stats['after_positive'] += int(merged['audiAcc'] is not None)
        korean = any(x.get('nationNm') in ('한국', '대한민국') for x in merged.get('nations', []))
        stats['korean_movies'] += int(korean)
        stats['korean_before_positive'] += int(korean and before_audi is not None)
        stats['korean_after_positive'] += int(korean and merged['audiAcc'] is not None)
        if before_audi is None and merged['audiAcc'] is not None:
            stats['recovered_movie_ids'].append(code)
            if korean:
                stats['korean_recovered_movie_ids'].append(code)
    audit = {'audience_semantics': 'maximum positive cumulative snapshot; not historical as-of audience',
             'cast_policy': 'single richest ordered source; lexical path tie-break; no union',
             'file_count': file_count, 'movie_count': len(records),
             'duplicate_movie_count': sum(len(v)>1 for v in groups.values()),
             'invalid_sources': invalid, 'years': dict(sorted(years.items())), 'records': entries}
    return records, audit


def preserve_enhanced_fields(fresh, existing):
    result = copy.deepcopy(fresh)
    new = movie_info(result)
    old = movie_info(existing)
    for key, value in old.items():
        if not _populated(new.get(key)):
            new[key] = copy.deepcopy(value)
    vals = [audience(n.get('audiAcc')) for n in (new, old, existing or {}, fresh or {})]
    best = max((v for v in vals if v is not None), default=None)
    if best is not None:
        new['audiAcc'] = best
    return result


def update_audience_files(paths, new_value):
    loaded = [(Path(p), json.loads(Path(p).read_text(encoding='utf-8'))) for p in paths]
    vals = [audience(new_value)]
    for _, raw in loaded:
        vals.extend([audience(raw.get('audiAcc')), audience(movie_info(raw).get('audiAcc'))])
    best = max((v for v in vals if v is not None), default=None)
    changed = 0
    if best is None:
        return changed
    for path, raw in loaded:
        node = movie_info(raw)
        if node.get('audiAcc') != best or ('audiAcc' in raw and raw['audiAcc'] != best):
            node['audiAcc'] = best
            if 'audiAcc' in raw:
                raw['audiAcc'] = best
            path.write_text(json.dumps(raw, ensure_ascii=False, indent=2), encoding='utf-8')
            changed += 1
    return changed
