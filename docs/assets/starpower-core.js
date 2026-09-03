(function (root, factory) {
  const api = factory();
  if (typeof module === 'object' && module.exports) module.exports = api;
  if (root) root.StarPowerCore = api;
})(typeof globalThis !== 'undefined' ? globalThis : this, function () {
  'use strict';

  const CONFIG = Object.freeze({
    lambda: 14,
    cutoffDate: '20260903',
    domesticNations: Object.freeze(['한국', '대한민국']),
    displayScale: 10000
  });
  const DOMESTIC_NATIONS = new Set(CONFIG.domesticNations);

  function safeNumber(value) {
    if (typeof value === 'number') return Number.isFinite(value) ? value : 0;
    const n = Number(String(value ?? '').replace(/,/g, '').trim());
    return Number.isFinite(n) ? n : 0;
  }

  function normalizeDate(value) {
    const digits = String(value ?? '').replace(/[^0-9]/g, '');
    return digits.length >= 8 ? digits.slice(0, 8) : '';
  }

  function isDomesticMovie(movie) {
    const nation = String(movie?.nation ?? '').trim();
    return DOMESTIC_NATIONS.has(nation);
  }

  function isEligibleMovie(movie) {
    const openDt = normalizeDate(movie?.openDt);
    return Boolean(
      isDomesticMovie(movie) &&
      openDt &&
      openDt <= CONFIG.cutoffDate &&
      safeNumber(movie?.audiAcc) > 0
    );
  }

  function canonicalActorKey(actor) {
    const id = String(actor?.id ?? '').trim();
    const name = String(actor?.name ?? '').trim();
    return id ? `id:${id}` : (name ? `name:${name}` : '');
  }

  function lookupActorKey(actorIdOrKey) {
    const raw = String(actorIdOrKey ?? '').trim();
    if (!raw) return '';
    return raw.startsWith('id:') || raw.startsWith('name:') ? raw : `id:${raw}`;
  }

  function movieIdentity(movie, index) {
    const movieCd = String(movie?.movieCd ?? '').trim();
    if (movieCd) return `cd:${movieCd}`;
    const movieNm = String(movie?.movieNm ?? '').trim();
    const openDt = normalizeDate(movie?.openDt);
    return `fallback:${movieNm}|${openDt}`;
  }

  function dedupeEligibleMovies(movies) {
    const unique = new Map();
    (Array.isArray(movies) ? movies : []).forEach((movie, index) => {
      if (!isEligibleMovie(movie)) return;
      const key = movieIdentity(movie, index);
      const normalized = {
        ...movie,
        openDt: normalizeDate(movie.openDt),
        audiAcc: safeNumber(movie.audiAcc),
        actors: Array.isArray(movie.actors) ? movie.actors : []
      };
      if (!unique.has(key)) {
        unique.set(key, normalized);
        return;
      }
      const current = unique.get(key);
      if (
        normalized.audiAcc > current.audiAcc ||
        (!current.actors.length && normalized.actors.length)
      ) unique.set(key, normalized);
    });
    return [...unique.values()];
  }

  function buildModel(movies) {
    const eligibleMovies = dedupeEligibleMovies(movies);
    const yearTotals = Object.create(null);

    for (const movie of eligibleMovies) {
      const year = movie.openDt.slice(0, 4);
      yearTotals[year] = (yearTotals[year] || 0) + movie.audiAcc;
    }

    const baseObservations = [];
    for (const movie of eligibleMovies) {
      const year = movie.openDt.slice(0, 4);
      const totalY = yearTotals[year] || 0;
      if (!(totalY > 0)) continue;
      const marketWeight = movie.audiAcc / totalY;
      const seen = new Set();

      movie.actors.forEach((actor, index) => {
        const actorKey = canonicalActorKey(actor);
        if (!actorKey || seen.has(actorKey)) return;
        seen.add(actorKey);

        const roleRank = index + 1;
        const roleWeight = 1 / Math.log2(roleRank + 1);
        const baseSp = marketWeight * roleWeight;
        if (!Number.isFinite(baseSp) || !(baseSp > 0)) return;

        baseObservations.push({
          actorKey,
          actorId: String(actor?.id ?? '').trim(),
          actorName: String(actor?.name ?? '').trim(),
          gender: String(actor?.gender ?? '').trim(),
          movieCd: String(movie?.movieCd ?? '').trim(),
          movieNm: String(movie?.movieNm ?? '').trim(),
          openDt: movie.openDt,
          year,
          audience: movie.audiAcc,
          marketWeight,
          roleRank,
          roleWeight,
          baseSp
        });
      });
    }

    baseObservations.sort((a, b) =>
      a.openDt.localeCompare(b.openDt) ||
      a.movieCd.localeCompare(b.movieCd) ||
      a.actorKey.localeCompare(b.actorKey)
    );

    const actorHistory = new Map();
    let globalPastSum = 0;
    let globalPastCount = 0;
    const observations = [];
    const observationByActorMovie = new Map();

    let pos = 0;
    while (pos < baseObservations.length) {
      const currentDate = baseObservations[pos].openDt;
      let end = pos + 1;
      while (end < baseObservations.length && baseObservations[end].openDt === currentDate) end++;

      const priorMean = globalPastCount > 0 ? globalPastSum / globalPastCount : NaN;

      for (let i = pos; i < end; i++) {
        const base = baseObservations[i];
        const hist = actorHistory.get(base.actorKey) || { sum: 0, count: 0 };
        let historyEstimate;
        let historyWeight = 1;

        if (Number.isFinite(priorMean) && priorMean > 0) {
          historyEstimate = (hist.sum + CONFIG.lambda * priorMean) / (hist.count + CONFIG.lambda);
          const ratio = historyEstimate / priorMean;
          historyWeight = Number.isFinite(ratio) && ratio >= 0 ? Math.sqrt(ratio) : 1;
        } else {
          historyEstimate = base.baseSp;
        }

        const starPower = CONFIG.displayScale * base.baseSp * historyWeight;
        const observation = {
          ...base,
          historyCount: hist.count,
          historySum: hist.sum,
          priorMean: Number.isFinite(priorMean) ? priorMean : null,
          historyEstimate,
          historyWeight,
          starPower
        };
        observations.push(observation);
        const movieKey = base.movieCd || `${base.movieNm}|${base.openDt}`;
        observationByActorMovie.set(`${base.actorKey}|${movieKey}`, observation);
      }

      // Same-day observations become available only after all same-day scores are computed.
      for (let i = pos; i < end; i++) {
        const base = baseObservations[i];
        const hist = actorHistory.get(base.actorKey) || { sum: 0, count: 0 };
        hist.sum += base.baseSp;
        hist.count += 1;
        actorHistory.set(base.actorKey, hist);
        globalPastSum += base.baseSp;
        globalPastCount += 1;
      }

      pos = end;
    }

    const actorStats = new Map();
    for (const obs of observations) {
      let stat = actorStats.get(obs.actorKey);
      if (!stat) {
        stat = {
          key: obs.actorKey,
          id: obs.actorId,
          name: obs.actorName,
          sex: obs.gender,
          movieCount: 0,
          totalStarPower: 0,
          totalAudi: 0,
          score: 0,
          observations: []
        };
        actorStats.set(obs.actorKey, stat);
      }
      stat.movieCount += 1;
      stat.totalStarPower += obs.starPower;
      stat.totalAudi += obs.audience;
      stat.observations.push(obs);
    }

    for (const stat of actorStats.values()) {
      stat.score = stat.movieCount > 0 ? stat.totalStarPower / stat.movieCount : 0;
    }

    const actorRanks = [...actorStats.values()]
      .filter(stat => Boolean(stat.id))
      .sort((a, b) => b.score - a.score || a.name.localeCompare(b.name, 'ko'))
      .map((stat, index) => ({
        id: stat.id,
        key: stat.key,
        name: stat.name,
        sex: stat.sex,
        score: stat.score,
        movieCount: stat.movieCount,
        totalAudi: stat.totalAudi,
        rank: index + 1
      }));

    const actorRankMap = new Map(actorRanks.map(item => [item.key, item]));

    return {
      config: CONFIG,
      eligibleMovies,
      yearTotals,
      observations,
      actorStats,
      actorRanks,
      actorRankMap,
      observationByActorMovie
    };
  }

  function getActorObservation(model, actorIdOrKey, movieCdOrKey) {
    if (!model) return null;
    const actorKey = lookupActorKey(actorIdOrKey);
    const movieKey = String(movieCdOrKey ?? '').trim();
    return model.observationByActorMovie.get(`${actorKey}|${movieKey}`) || null;
  }

  function getActorRank(model, actorIdOrKey) {
    if (!model) return null;
    return model.actorRankMap.get(lookupActorKey(actorIdOrKey)) || null;
  }

  return Object.freeze({
    CONFIG,
    safeNumber,
    normalizeDate,
    isDomesticMovie,
    isEligibleMovie,
    buildModel,
    getActorObservation,
    getActorRank
  });
});
