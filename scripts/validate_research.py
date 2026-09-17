#!/usr/bin/env python3
"""Reproduce the retrospective SP/Hawk audit from local snapshots only.

python scripts/validate_research.py --baseline /path/to/original/project
Requires Node.js and pip install -r requirements-research.txt. No data API calls.
"""
import argparse
import csv
import hashlib
import inspect
import json
import math
import platform
import statistics
import subprocess
import warnings
from collections import Counter
from pathlib import Path

import kneed
import numpy as np
import scipy
from kneed import KneeLocator
from scipy.stats import binom

ROOT = Path(__file__).resolve().parents[1]
METHODS = ('maxDeviation', 'kneedS1', 'kneedS2', 'kneedS5')
# Execute the page's unmodified analytical functions, without its DOM/render layer.
NODE_BRIDGE = r"""
const fs=require('fs'), vm=require('vm'), path=require('path');
const root=process.argv[1], dataPath=process.argv[2];
const raw=JSON.parse(fs.readFileSync(dataPath,'utf8'));
const sp=require(path.join(root,'docs/assets/starpower-core.js'));
const hk=require(path.join(root,'docs/assets/hawk-criterion-core.js'));
const html=fs.readFileSync(path.join(root,'docs/experiment.html'),'utf8');
const cutoff=html.match(/const ANALYSIS_CUTOFF\s*=\s*['"]([^'"]+)/)[1];
const start=html.indexOf('function safeNumber('), end=html.indexOf('function getResult(');
if(start<0 || end<start) throw Error('Cannot locate experiment analytical functions');
const ctx={raw, console};vm.createContext(ctx);
vm.runInContext(`const ANALYSIS_CUTOFF='${cutoff}'; const LAMBDAS=Array.from({length:50},(_,i)=>i+1); const DISPLAY_SCALE=10000;\n`+html.slice(start,end)+`\nconst ds=buildDataset(raw); const results=evaluateAllLambdas(ds.validationRows,ds.historyRows); globalThis.audit={counters:ds.counters,movieCount:ds.movies.length,observationCount:ds.observations.length,sampleCount:ds.validationRows.length,historySampleCount:ds.historyRows.length,results,recommendedLambda:chooseRecommended(results)?.lambda,firstDate:ds.firstDate,lastDate:ds.lastDate};globalThis.validationRows=ds.validationRows;`,ctx);
const model=sp.buildModel(raw), yearly=hk.buildAnnualScores(model);
const coverage=hk.coverageDiagnostics(yearly,cutoff);
const years={};for(const [year,map] of Object.entries(yearly)) years[year]=[...map.values()].sort((a,b)=>b.score-a.score || a.name.localeCompare(b.name,'ko'));
const knees={};for(const [year,rows] of Object.entries(years)) knees[year]=hk.kneePercentile(rows.map(x=>x.score));
const v=model.observations.filter(o=>o.historyCount>=1 && o.priorMean>0);
let maxRowDifference=0;
if(v.length!==ctx.validationRows.length) throw Error('Core/page validation sample count mismatch');
v.forEach((o,i)=>{const p=ctx.validationRows[i];for(const [a,b] of [[o.baseSp,p.actual],[o.historySum,p.pastSum],[o.historyCount,p.n],[o.priorMean,p.mu]]) maxRowDifference=Math.max(maxRowDifference,Math.abs(a-b));});
if(maxRowDifference>1e-12) throw Error('Core/page sample mismatch '+maxRowDifference);
const datedValidation=v.map(o=>({date:o.openDt,actual:o.baseSp,pastSum:o.historySum,n:o.historyCount,mu:o.priorMean}));
console.log(JSON.stringify({lambda:ctx.audit,cutoff,config:model.config,coreMovieCount:model.eligibleMovies.length,coreObservationCount:model.observations.length,crossCheck:{maxRowDifference,sampleCount:v.length},years,coverage,knees,datedValidation}));
"""


def sha256(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def run_js(root, data):
    result = subprocess.run(['node', '-e', NODE_BRIDGE, str(Path(root).resolve()), str(Path(data).resolve())], check=True, capture_output=True, text=True)
    return json.loads(result.stdout)


def kneedle(scores, sensitivity=1):
    """Ranks are one-based. A missing knee remains None, never a forced rank."""
    scores = sorted((float(s) for s in scores if s > 0 and math.isfinite(s)), reverse=True)
    if len(scores) < 3 or scores[0] == scores[-1]:
        return None
    with warnings.catch_warnings():
        warnings.simplefilter('ignore', RuntimeWarning)
        fit = KneeLocator(np.arange(1, len(scores) + 1), np.log1p(scores), S=sensitivity,
                          curve='convex', direction='decreasing', online=False, interp_method='interp1d')
    if fit.knee is None:
        return None
    rank = int(fit.knee)
    return {'rank': rank, 'percent': 100 * rank / len(scores), 'scoreThreshold': scores[rank - 1]}


def metric_rows(rows):
    out = []
    for lam in range(1, 51):
        errors = [r['actual'] - (r['pastSum'] + lam*r['mu'])/(r['n'] + lam) for r in rows]
        out.append({'lambda': lam, 'mae': statistics.mean(map(abs, errors)) if errors else None,
                    'rmse': math.sqrt(statistics.mean(e*e for e in errors)) if errors else None,
                    'sampleCount': len(rows)})
    return out


def number_summary(values):
    if not values:
        return {'count': 0, 'mean': None, 'median': None, 'min': None, 'max': None, 'loyoMedianRange': None}
    left_out = [statistics.median(values[:i]+values[i+1:]) for i in range(len(values))] if len(values)>1 else []
    return {'count': len(values), 'mean': statistics.mean(values), 'median': statistics.median(values),
            'min': min(values), 'max': max(values),
            'loyoMedianRange': [min(left_out), max(left_out)] if left_out else None}


def recurrence(years, included, percent):
    """The FDR number is only a descriptive exchangeability/independence benchmark."""
    if percent is None:
        return {'persistentCounts': {}, 'persistentActors': {}, 'annualActualFractions': [], 'fdrDiagnostic': None}
    hits, active, actual = Counter(), Counter(), []
    for year in included:
        rows = years[year]
        count = min(len(rows), max(1, math.floor(len(rows)*percent/100)))
        hits.update(r['actorKey'] for r in rows[:count])
        active.update(r['actorKey'] for r in rows)
        actual.append({'year': year, 'actorCount': len(rows), 'topCount': count, 'nominalPercent': percent,
                       'actualPercent': 100*count/len(rows)})
    sets = {str(m): sorted(k for k, n in hits.items() if n>=m) for m in (2, 3, 4, 5)}
    tests = sorted((float(binom.sf(hits[k]-1, n, percent/100)), k) for k, n in active.items())
    accepted = [i for i,(p,_) in enumerate(tests) if p<=.05*(i+1)/len(tests)]
    significant = [k for _, k in tests[:accepted[-1]+1] if hits[k]>=2] if accepted else []
    return {'persistentCounts': {m:len(s) for m,s in sets.items()}, 'persistentActors': sets,
            'annualActualFractions': actual,
            'fdrDiagnostic': {'hawkCount':len(significant), 'testedActorCount':len(tests),'q':.05,
                              'assumptions':'Unverified exchangeable actors, independent active years, nominal p; descriptive benchmark only.'}}


def overlap(a, b):
    a, b = set(a), set(b)
    return {'leftCount':len(a),'rightCount':len(b),'intersection':len(a&b),'union':len(a|b),
            'jaccard':len(a&b)/len(a|b) if a|b else None}


def analyze(root, data, status):
    js = run_js(root, data)
    rows = []
    for c in js['coverage']['rows']:
        year = c['year']
        row = dict(c)
        reasons = []
        if c['partialYear']: reasons.append('partial_cutoff_year')
        if c['lowCoverage']: reasons.append('actor_count_below_Tukey_lower_fence')
        row['exclusionReason'] = ';'.join(reasons) or None
        row['maxDeviation'] = js['knees'].get(year)
        for s in (1,2,5): row[f'kneedS{s}'] = kneedle([x['score'] for x in js['years'][year]], s)
        rows.append(row)
    valid = [r for r in rows if r['includedForAggregate']]
    valid_years = [r['year'] for r in valid]
    methods = {}
    for method in METHODS:
        values = [r[method]['percent'] for r in valid if r[method] is not None]
        stat = number_summary(values)
        stat.update({'noKneeCount': sum(r[method] is None for r in valid), 'eligibleYearCount':len(valid),
                     'noKneeFrequency':sum(r[method] is None for r in valid)/len(valid) if valid else None})
        stat.update(recurrence(js['years'], valid_years, stat['median']))
        stat['meanThresholdPersistence'] = recurrence(js['years'], valid_years, stat['mean'])
        methods[method] = stat
    scale_sensitivity = {}
    for multiplier in (0.0001, 1.0, 100.0):
        percents = []
        for y in valid_years:
            values = sorted((r['score']*multiplier for r in js['years'][y] if r['score']>0), reverse=True)
            if len(values)<3 or values[0]==values[-1]: continue
            ys = np.log1p(values)
            distances = 1-np.arange(len(values))/(len(values)-1)-(ys-ys[-1])/(ys[0]-ys[-1])
            index = 1+int(np.argmax(distances[1:-1]))
            percents.append(100*(index+1)/len(values))
        scale_sensitivity[str(multiplier)] = number_summary(percents)
    comparisons = {method:{str(m):overlap(methods['maxDeviation']['persistentActors'].get(str(m),[]),
                              methods[method]['persistentActors'].get(str(m),[])) for m in (2,3,4,5)}
                   for method in METHODS[1:]}
    train = [r for r in valid if int(r['year'])<=2018]
    holdout = [r for r in valid if 2019<=int(r['year'])<=2025]
    hold_methods = {}
    for method in METHODS:
        calibration = number_summary([r[method]['percent'] for r in train if r[method] is not None])
        diagnostic = number_summary([r[method]['percent'] for r in holdout if r[method] is not None])
        hold_methods[method] = {'calibration':calibration, 'holdoutKneeDiagnostic':diagnostic,
                               **recurrence(js['years'], [r['year'] for r in holdout], calibration['median'])}
    # Validate a separately expressed predictor against the exact page for all 50 lambda values.
    independent = metric_rows(js['datedValidation'])
    max_metric_diff = max(abs(a[k]-b[k]) for a,b in zip(independent,js['lambda']['results']) for k in ('mae','rmse'))
    if max_metric_diff>1e-12: raise AssertionError(f'Page/Python lambda mismatch: {max_metric_diff}')
    train_v = [r for r in js['datedValidation'] if r['date']<'20190101']
    test_v = [r for r in js['datedValidation'] if '20190101'<=r['date']<'20260101']
    train_metrics, test_metrics = metric_rows(train_v), metric_rows(test_v)
    selected = min(train_metrics,key=lambda r:(r['mae'],r['rmse'],r['lambda']))['lambda'] if train_v else None
    js['lambda'].update({'adoptedLambda':js['config']['lambda'],'pythonCrossCheckMaximumAbsoluteDifference':max_metric_diff,
                         'chronologicalDiagnostic':{'calibrationSampleCount':len(train_v),'holdoutSampleCount':len(test_v),
                             'selectedOnCalibration':selected,'calibrationResults':train_metrics,'holdoutResults':test_metrics,
                             'selectedHoldoutResult':test_metrics[selected-1] if selected else None}})
    return {'status':status,'dataSha256':sha256(data),'cutoff':js['cutoff'], 'lambda':js['lambda'],
            'coreCrossCheck':js['crossCheck'], 'coreMovieCount':js['coreMovieCount'],
            'coreObservationCount':js['coreObservationCount'],
            'scaleSensitivity':scale_sensitivity,'coverage':js['coverage'],'yearly':rows,'methods':methods,'methodOverlap':comparisons,
            'holdout':{'splitRationale':'Fixed calendar diagnostic: completed years <=2018 calibration; 2019–2025 validation. Not selected using event/hypothesis outcomes. Retrospective final audience snapshots prevent prospective as-of validation.',
                       'calibrationYears':[r['year'] for r in train],'validationYears':[r['year'] for r in holdout], 'methods':hold_methods},
            'sourceHashes':{p:sha256(Path(root)/p) for p in ('docs/assets/starpower-core.js','docs/assets/hawk-criterion-core.js','docs/experiment.html')}}


def write_csv(path, rows):
    if not rows: return
    with path.open('w',newline='',encoding='utf-8-sig') as file:
        writer=csv.DictWriter(file,fieldnames=list(rows[0]));writer.writeheader();writer.writerows(rows)


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--baseline', type=Path, help='Original project folder; necessary for recovery comparison')
    parser.add_argument('--data', type=Path, default=ROOT/'docs/data/search_index.json')
    parser.add_argument('--output', type=Path, default=ROOT/'docs/data/research')
    args=parser.parse_args()
    args.output.mkdir(parents=True,exist_ok=True)
    source=Path(inspect.getfile(KneeLocator))
    result={'schemaVersion':1,'notes':[
        'All results are retrospective descriptive diagnostics; no cutoff is scientifically uniquely optimal.',
        'Original data and original calibration are superseded by repaired duplicate selection; recomputation is required.',
        'Adopted lambda is read from each snapshot core; repaired snapshot uses MAE-optimal 12. This is calibration, not an untouched predictive test.',
        'Annual audience denominators and audience totals use final snapshots, including later-in-year data. Chronological histories alone do not establish prospective as-of validity.',
        'Coverage fences are computed on the entire snapshot, including holdout years: split is a threshold transport diagnostic, not an untouched prospective test. Sparse collection coverage is not verified against an external population.',
        'LOYO median range is a deletion diagnostic, not a bootstrap confidence interval or independence guarantee.',
        'Persistence uses all coverage-valid completed years; no-knee years remain eligible for applying a calibrated fixed percentile. Only years with a detected knee contribute to percentile calibration.',
        'Top selection follows page floor rule with a minimum of one actor; annual actual fractions expose rounding.',
        'Binomial/BH-FDR is an unvalidated reference model, not a discovery or hypothesis guarantee.'
    ],'provenance':{'python':platform.python_version(),'numpy':np.__version__,'scipy':scipy.__version__,'kneed':kneed.__version__,
       'kneedSourceSha256':sha256(source),'scriptSha256':sha256(__file__),
       'kneedParameters':{'S':[1,2,5],'curve':'convex','direction':'decreasing','online':False,'interp_method':'interp1d','transform':'log1p(Annual SP)','rankOrigin':1},
       'references':['https://github.com/arvkevi/kneed/tree/v0.8.5','https://raghavan.usc.edu/papers/kneedle-simplex11.pdf']}}
    if args.baseline:
        result['baseline']=analyze(args.baseline,args.baseline/'docs/data/search_index.json','invalidated_original_snapshot')
    result['recovered']=analyze(ROOT,args.data,'recovered_retrospective_snapshot')
    if 'baseline' in result:
        result['beforeAfterOverlap']={method:{str(m):overlap(result['baseline']['methods'][method]['persistentActors'].get(str(m),[]),result['recovered']['methods'][method]['persistentActors'].get(str(m),[])) for m in (2,3,4,5)} for method in METHODS}
    # Keep report valid strict JSON; non-finite page metrics (e.g. empty buckets) arrive as null.
    (args.output/'hawk_validation.json').write_text(json.dumps(result,ensure_ascii=False,indent=2,allow_nan=False)+'\n',encoding='utf-8')
    for version in ('baseline','recovered'):
        if version not in result: continue
        report=result[version]
        write_csv(args.output/f'lambda_{version}.csv',[{'lambda':r['lambda'],'sampleCount':report['lambda']['sampleCount'], 'mae':r['mae'],'rmse':r['rmse'],'mae_x10000':r['mae']*10000,'rmse_x10000':r['rmse']*10000} for r in report['lambda']['results']])
        flat=[]
        for r in report['yearly']:
            for method in METHODS:
                knee=r[method] or {}
                flat.append({k:r[k] for k in ('year','actorCount','includedForAggregate','exclusionReason')}|{'method':method,'rank':knee.get('rank'),'percent':knee.get('percent'),'scoreThreshold':knee.get('scoreThreshold'),'noKnee':r[method] is None})
        write_csv(args.output/f'yearly_knees_{version}.csv',flat)
        write_csv(args.output/f'actual_top_fractions_{version}.csv',[{'method':method,**r} for method in METHODS for r in report['methods'][method]['annualActualFractions']])
        print(json.dumps({'version':version,'movies':report['coreMovieCount'],'observations':report['coreObservationCount'],'lambdaSamples':report['lambda']['sampleCount'],'recommendedLambda':report['lambda']['recommendedLambda'],'methods':{m:{k:report['methods'][m][k] for k in ('count','median','mean','noKneeCount','loyoMedianRange','persistentCounts')} for m in METHODS}},ensure_ascii=False))

if __name__=='__main__':
    main()
