(function (root, factory) {
  const api = factory();
  if (typeof module === 'object' && module.exports) module.exports = api;
  if (root) root.HawkCriterionCore = api;
})(typeof globalThis !== 'undefined' ? globalThis : this, function () {
  'use strict';

  function mean(values) {
    return values.length ? values.reduce((a, b) => a + b, 0) / values.length : NaN;
  }

  function median(values) {
    if (!values.length) return NaN;
    const xs = [...values].sort((a, b) => a - b);
    const mid = Math.floor(xs.length / 2);
    return xs.length % 2 ? xs[mid] : (xs[mid - 1] + xs[mid]) / 2;
  }

  function quantile(values, q) {
    if (!values.length) return NaN;
    const xs = [...values].sort((a, b) => a - b);
    const pos = (xs.length - 1) * q;
    const lo = Math.floor(pos);
    const hi = Math.ceil(pos);
    if (lo === hi) return xs[lo];
    const f = pos - lo;
    return xs[lo] * (1 - f) + xs[hi] * f;
  }

  function summarizeNumbers(values) {
    return {
      count: values.length,
      mean: mean(values),
      median: median(values),
      min: values.length ? Math.min(...values) : NaN,
      max: values.length ? Math.max(...values) : NaN
    };
  }

  function buildAnnualScores(starPowerModel) {
    const yearly = Object.create(null);
    if (!starPowerModel || !Array.isArray(starPowerModel.observations)) return yearly;
    for (const obs of starPowerModel.observations) {
      const year = String(obs.year || '').trim();
      if (!year) continue;
      if (!yearly[year]) yearly[year] = new Map();
      const map = yearly[year];
      const key = obs.actorKey;
      const row = map.get(key) || {
        actorKey: key,
        id: obs.actorId || '',
        name: obs.actorName || '',
        score: 0,
        filmCount: 0
      };
      row.score += Number(obs.starPower) || 0;
      row.filmCount += 1;
      map.set(key, row);
    }
    return yearly;
  }

  function coverageDiagnostics(yearlyScores, cutoffDate) {
    const cutoff = String(cutoffDate || '').replace(/\D/g, '').slice(0, 8);
    const cutoffYear = cutoff.slice(0, 4);
    const partialCutoffYear = cutoff && !cutoff.endsWith('1231');
    const years = Object.keys(yearlyScores || {}).sort();
    const fullYearCounts = years
      .filter(y => !(partialCutoffYear && y === cutoffYear))
      .map(y => yearlyScores[y] instanceof Map ? yearlyScores[y].size : 0)
      .filter(n => n > 0);
    const q1 = quantile(fullYearCounts, 0.25);
    const q3 = quantile(fullYearCounts, 0.75);
    const iqr = q3 - q1;
    const lowerFence = q1 - 1.5 * iqr;
    const rows = years.map(year => {
      const actorCount = yearlyScores[year] instanceof Map ? yearlyScores[year].size : 0;
      const partialYear = Boolean(partialCutoffYear && year === cutoffYear);
      const lowCoverage = Number.isFinite(lowerFence) && actorCount < lowerFence;
      return {
        year,
        actorCount,
        partialYear,
        lowCoverage,
        includedForAggregate: !partialYear && !lowCoverage
      };
    });
    return { q1, q3, iqr, lowerFence, rows };
  }

  // Kneedle-inspired offline knee for a monotone decreasing heavy-tail curve.
  // Annual SP is sorted descending, log1p transformed, normalized to [0,1],
  // and the point with the largest downward deviation from the endpoint diagonal is selected.
  function kneePercentile(values) {
    const xs = (values || []).filter(v => Number.isFinite(v) && v > 0).sort((a, b) => b - a);
    const n = xs.length;
    if (n < 3) return null;
    const ys = xs.map(v => Math.log1p(v));
    const minY = ys[n - 1];
    const maxY = ys[0];
    if (!(maxY > minY)) return null;
    let bestIndex = 1;
    let bestDistance = -Infinity;
    for (let i = 1; i < n - 1; i++) {
      const xNorm = i / (n - 1);
      const yNorm = (ys[i] - minY) / (maxY - minY);
      const diagonal = 1 - xNorm;
      const distance = diagonal - yNorm;
      if (distance > bestDistance) {
        bestDistance = distance;
        bestIndex = i;
      }
    }
    const rank = bestIndex + 1;
    return {
      rank,
      percent: 100 * rank / n,
      scoreThreshold: xs[bestIndex],
      distance: bestDistance,
      actorCount: n
    };
  }

  function summarizeYearlyKnees(yearlyScores, cutoffDate) {
    const coverage = coverageDiagnostics(yearlyScores, cutoffDate);
    const coverageByYear = new Map(coverage.rows.map(r => [r.year, r]));
    const rows = [];
    for (const year of Object.keys(yearlyScores || {}).sort()) {
      const map = yearlyScores[year];
      const values = map instanceof Map ? [...map.values()].map(r => r.score) : [];
      const knee = kneePercentile(values);
      const c = coverageByYear.get(year);
      if (!knee || !c) continue;
      rows.push({
        year,
        ...knee,
        lowCoverage: c.lowCoverage,
        partialYear: c.partialYear,
        includedForAggregate: c.includedForAggregate
      });
    }
    const included = rows.filter(r => r.includedForAggregate).map(r => r.percent);
    return {
      coverage,
      rows,
      includedPercents: included,
      ...summarizeNumbers(included)
    };
  }

  function topSetForYear(yearMap, percent) {
    const rows = [...yearMap.values()].sort((a, b) => b.score - a.score || a.name.localeCompare(b.name, 'ko'));
    const topCount = Math.max(1, Math.floor(rows.length * (percent / 100)));
    return { rows, topCount, topRows: rows.slice(0, topCount), topKeys: new Set(rows.slice(0, topCount).map(r => r.actorKey)) };
  }

  function fixedRecurrenceSummary(yearlyScores, percent, minCount, includedYears) {
    const years = includedYears || Object.keys(yearlyScores || {}).sort();
    const hits = new Map();
    let totalTopSlots = 0;
    for (const year of years) {
      const map = yearlyScores[year];
      if (!(map instanceof Map) || !map.size) continue;
      const top = topSetForYear(map, percent);
      totalTopSlots += top.topCount;
      for (const row of top.topRows) {
        const item = hits.get(row.actorKey) || { actorKey: row.actorKey, id: row.id, name: row.name, hits: 0 };
        item.hits += 1;
        hits.set(row.actorKey, item);
      }
    }
    const hawks = [...hits.values()].filter(x => x.hits >= minCount).sort((a, b) => b.hits - a.hits || a.name.localeCompare(b.name, 'ko'));
    return { percent, minCount, totalTopSlots, hawks, hawkCount: hawks.length };
  }

  function logChoose(n, k) {
    if (k < 0 || k > n) return -Infinity;
    k = Math.min(k, n - k);
    let s = 0;
    for (let i = 1; i <= k; i++) s += Math.log(n - k + i) - Math.log(i);
    return s;
  }

  function binomialUpperTail(n, k, p) {
    if (k <= 0) return 1;
    if (k > n) return 0;
    if (p <= 0) return 0;
    if (p >= 1) return 1;
    let sum = 0;
    for (let x = k; x <= n; x++) {
      const lp = logChoose(n, x) + x * Math.log(p) + (n - x) * Math.log1p(-p);
      sum += Math.exp(lp);
    }
    return Math.min(1, Math.max(0, sum));
  }

  function benjaminiHochberg(items, q) {
    const sorted = [...items].sort((a, b) => a.pValue - b.pValue);
    let maxIndex = -1;
    const m = sorted.length;
    for (let i = 0; i < m; i++) {
      const threshold = q * (i + 1) / m;
      if (sorted[i].pValue <= threshold) maxIndex = i;
    }
    if (maxIndex < 0) return [];
    const cutoff = sorted[maxIndex].pValue;
    return sorted.filter(x => x.pValue <= cutoff).map(x => ({ ...x, fdrCutoff: cutoff }));
  }

  function persistentHawksFdr(yearlyScores, percent, q, includedYears) {
    const years = includedYears || Object.keys(yearlyScores || {}).sort();
    const activeYears = new Map();
    const hits = new Map();
    const identity = new Map();
    for (const year of years) {
      const map = yearlyScores[year];
      if (!(map instanceof Map) || !map.size) continue;
      for (const row of map.values()) {
        activeYears.set(row.actorKey, (activeYears.get(row.actorKey) || 0) + 1);
        identity.set(row.actorKey, row);
      }
      const top = topSetForYear(map, percent);
      for (const row of top.topRows) hits.set(row.actorKey, (hits.get(row.actorKey) || 0) + 1);
    }
    const p = percent / 100;
    const tests = [];
    for (const [actorKey, activeCount] of activeYears.entries()) {
      const hitCount = hits.get(actorKey) || 0;
      const row = identity.get(actorKey) || {};
      tests.push({
        actorKey,
        id: row.id || '',
        name: row.name || actorKey,
        activeCount,
        hitCount,
        pValue: hitCount > 0 ? binomialUpperTail(activeCount, hitCount, p) : 1
      });
    }
    const significant = benjaminiHochberg(tests, q)
      .filter(x => x.hitCount >= 2)
      .sort((a, b) => a.pValue - b.pValue || b.hitCount - a.hitCount || a.name.localeCompare(b.name, 'ko'));
    return { percent, q, testedActorCount: tests.length, tests, significant, hawkCount: significant.length };
  }

  return Object.freeze({
    mean,
    median,
    quantile,
    summarizeNumbers,
    buildAnnualScores,
    coverageDiagnostics,
    kneePercentile,
    summarizeYearlyKnees,
    topSetForYear,
    fixedRecurrenceSummary,
    binomialUpperTail,
    benjaminiHochberg,
    persistentHawksFdr
  });
});
