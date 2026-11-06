/* 실험 1 전용 계산.
 * CONFIG에서 상수, FORMULA에서 수식, methodsFor에서 표시 문구를 수정합니다.
 * 앱은 열릴 때마다 같은 모델에서 그래프·후보 목록·재출연율을 다시 만듭니다.
 */
(function (root, make) {
  'use strict';
  const api = make();
  if (typeof module === 'object' && module.exports) module.exports = api;
  if (root) root.Exp1Model = api;
})(typeof globalThis === 'object' ? globalThis : this, function () {
  'use strict';

  const CONFIG = Object.freeze({
    cutoffDate: null, // null이면 오늘(UTC)까지 자료에 있는 마지막 국내 개봉일
    domesticNations: Object.freeze(['한국', '대한민국']),
    lambda: 12,            // 과거 이력의 평활화 계수
    displayScale: 10000,
    scoreMode: 'paper',    // 초안의 기본식
    roleExponent: 1,
    historyExponent: 0.5,
    daysPerYear: 365.25,
    percentile: 0.95,     // 기울기 변화 절댓값의 95백분위
    topFraction: 0.25     // 계산 가능한 동료의 상위 25%
  });
  const MODES = ['paper', 'no_history', 'market_only', 'reciprocal_rank'];
  const FORMULA = Object.freeze({
    // 논문 초안 식 (1)~(3). 순위와 이력은 개별 함수에서 변경합니다.
    market(audience, annualTotal) {
      return audience > 0 && annualTotal > 0 ? audience / annualTotal : null;
    },
    role(rank, c) {
      if (c.scoreMode === 'market_only') return 1;
      if (c.scoreMode === 'reciprocal_rank') return 1 / rank;
      return 1 / Math.pow(Math.log2(rank + 1), c.roleExponent);
    },
    history(pastSum, pastCount, mu, c) {
      if (!(mu > 0)) return {estimate: null, weight: 1};
      const estimate = (pastSum + c.lambda * mu) / (pastCount + c.lambda);
      const weight = c.scoreMode === 'paper' && pastCount ? Math.pow(estimate / mu, c.historyExponent) : 1;
      return {estimate, weight};
    },
    score(marketWeight, roleWeight, historyWeight, c) {
      return c.displayScale * marketWeight * roleWeight * historyWeight;
    },
    // 초안 식 (5). A→B에서는 동료 B의 과거 점수를 비교합니다.
    synergy(withMean, withoutMean) {
      return withoutMean > 0 ? 100 * (withMean / withoutMean - 1) : null;
    },
    slope(left, right, c) {
      return (right.sp - left.sp) * c.daysPerYear / (right.epoch - left.epoch);
    }
  });
  function methodsFor(c) {
    // Keep the user-facing methods and formula choices beside the editable equations.
    const role = c.scoreMode === 'market_only' ? '1' : c.scoreMode === 'reciprocal_rank' ? '1 / k' :
      '1 / log₂(k + 1)' + (c.roleExponent === 1 ? '' : '^(' + c.roleExponent + ')');
    const history = c.scoreMode !== 'paper' ? '1' : (c.historyExponent === 0.5 ? '√(h / μ)' : '(h / μ)^(' + c.historyExponent + ')');
    const text = {
      base:'W_market(시장 점유율) = 작품 누적관객 / 해당 연도 적격 국내 영화의 누적관객 합계. 기본 점수 b = W_market × W_role. 출연진 정보가 없어도 양의 관객이 있는 적격 영화는 연간 분모에 포함합니다.',
      role:'W_role(출연 순서 가중치) = ' + role + '. k는 원본 출연진 목록의 1부터 시작하는 순서입니다. 같은 영화의 중복 ID는 첫 순서를 유지합니다.',
      prior:'h = (배우의 과거 b 합 + ' + c.lambda + ' × μ) / (배우의 과거 작품 수 + ' + c.lambda + '). μ는 개봉일 전까지 모든 배우–작품 b의 산술평균입니다.',
      history:'W_history(과거 이력 가중치) = ' + history + '. 이전 전체 평균이 없거나 배우에게 과거 작품이 없으면 1을 적용합니다. 같은 날 작품은 모두 계산한 뒤 이력에 넣습니다.',
      starPower:'SP = ' + c.displayScale.toLocaleString('en-US') + ' × W_market × W_role × W_history. 세 가중치의 정의는 위와 같습니다.',
      dateMean:'같은 개봉일의 측정 가능한 배우–작품 SP 산술평균을 씁니다. 결측은 0으로 넣지 않으며, 일부만 계산된 날과 계산할 수 없는 작품도 표시합니다.',
      slope:'기울기 = (나중 날짜 평균 SP − 앞 날짜 평균 SP) × ' + c.daysPerYear + ' / 실제 경과 일수. 단위: SP/년.',
      change:'서로 다른 연속 세 측정일의 가운데 날짜가 기준점입니다. 변화량 = 뒤 구간 기울기 − 앞 구간 기울기. 뒤 측정일이 있어야 판정되므로 실시간 예측이 아닙니다.',
      threshold:'전체 변화량 절댓값을 작은 순서로 정렬해 올림(' + c.percentile + ' × 전체 수)번째 값을 구합니다. 임계값 이상인 동률을 모두 포함하고, 0은 분포에는 포함하되 급변점으로 표시하지 않습니다.',
      synergy:'S(A→B) = 100 × (A와 함께한 B의 과거 평균 SP / A 없이 출연한 B의 과거 평균 SP − 1). 기준일 전 작품만 쓰며, 두 집합이 모두 있고 분모가 양수인 안정 ID 동료만 계산합니다.',
      pool:'계산 가능한 동료 M명을 내림차순 정렬해 상위 올림(' + c.topFraction + ' × M)명과 경계 동률을 포함한 뒤, 시너지가 양수인 동료만 남깁니다. 이 명단을 기준일과 이후 모든 개봉일의 비교에 고정합니다.',
      next:'기준일과 그 뒤 국내 작품의 개봉일마다 같은 날의 모든 작품에서 재등장을 찾습니다. 모집단 비율은 기준일과 그 직후 첫 개봉일을 각각 한 번씩 집계합니다. 다음 작품에 SP가 없어도 건너뛰지 않습니다. 안정 ID가 없는 개봉 기록은 특정 배우의 필모그래피에 연결할 수 없습니다.',
      rate:'특정 시점의 재등장은 한 명 이상 일치해야 합니다. 계산 가능한 풀이 없거나 고시너지 동료의 부재를 확인하기에 출연 정보가 부족하면 분모에서 제외합니다. 배우별로 한 번이라도 일치하면 재등장, 전부 확정 불일치면 미재등장, 나머지는 판정불가입니다.',
      event:'기존 조사 대장에서 배우 ID가 일치하고 출처 링크가 있는 공개 월만 표시합니다. 직전 측정일부터 뒤 측정일까지 월 단위로 비교합니다. 시작·끝과 같은 달에는 실제 공개일이 관측 구간 밖일 수도 있습니다. 활동 중단의 근거 상태는 별도로 표시합니다. 시간상 겹침은 원인·캐스팅 의도의 증거가 아닙니다.',
      scope:'국내 국적 표시 영화에 연결된 배우 ID를 모집단으로 씁니다. 인물의 국적은 확인할 수 없으며, ID가 없는 배역은 작품별로만 표시하고 배우 인원에 합치지 않습니다.',
      snapshot:'집계 종료일은 ' + c.cutoffDate + '입니다. 누적 관객과 연간 분모는 보관 자료의 사후 집계이며 미관측 작품·관객은 보정하지 않습니다. 최신 연도는 부분연도입니다.'
    };
    return {config:c, text, scoreModes:MODES.slice()};
  }
  const MS_DAY = 86400000;
  const trim = v => String(v == null ? '' : v).trim();
  const number = v => {
    if (v == null || trim(v) === '') return null;
    const n = Number(trim(v).replace(/,/g, '')); return Number.isFinite(n) ? n : null;
  };
  function iso(value) {
    const s = trim(value);
    const m = /^(\d{4})[-./]?(\d{2})[-./]?(\d{2})$/.exec(s);
    if (!m) return '';
    const d = m[1] + '-' + m[2] + '-' + m[3], t = Date.parse(d + 'T00:00:00Z');
    return Number.isFinite(t) && new Date(t).toISOString().slice(0, 10) === d ? d : '';
  }
  function configOf(rows, options) {
    const c = Object.assign({}, CONFIG, options);
    const countries = new Set(c.domesticNations || []);
    if (!countries.size) throw new Error('domesticNations cannot be empty');
    if (c.cutoffDate == null) {
      const today = new Date().toISOString().slice(0, 10);
      let end = '';
      for (const m of rows) if (countries.has(trim(m.nation))) {
        const d = iso(m.openDt); if (d && d > end && d <= today) end = d;
      }
      c.cutoffDate = end || today;
    } else {
      c.cutoffDate = iso(c.cutoffDate);
      if (!c.cutoffDate) throw new Error('Invalid cutoffDate');
    }
    for (const k of ['lambda','displayScale','roleExponent','historyExponent','daysPerYear','percentile','topFraction']) {
      c[k] = Number(c[k]);
      if (!Number.isFinite(c[k]) || (['roleExponent','historyExponent'].includes(k) ? c[k]<0 : c[k]<=0)) throw new Error('Invalid ' + k);
    }
    if (c.percentile > 1 || c.topFraction > 1 || !MODES.includes(c.scoreMode)) throw new Error('Invalid percentile, topFraction or scoreMode');
    c.domesticNations = [...countries];
    return c;
  }
  function byDate(a,b) {return a.date < b.date ? -1 : a.date > b.date ? 1 : 0;}
  function byId(a,b) {return a.id < b.id ? -1 : a.id > b.id ? 1 : 0;}
  function nearestRank(values, q) {
    if (!values.length) return null;
    const sorted = values.slice().sort((a,b) => a-b);
    return sorted[Math.max(0, Math.ceil(q * sorted.length) - 1)];
  }
  function selectHigh(pool, fraction) {
    if (!pool.length) return {high: [], boundary: null};
    const sorted = pool.slice().sort((a,b) => b.value-a.value || byId(a,b));
    const boundary = sorted[Math.ceil(sorted.length * fraction) - 1].value;
    return {high: sorted.filter(p => p.value > 0 && p.value >= boundary), boundary};
  }
  const cleanSources = xs => (Array.isArray(xs) ? xs : []).filter(s=>/^https?:\/\//i.test(trim(s && s.url))).map(s=>({title:trim(s.title)||trim(s.url),url:trim(s.url)}));
  function eventMap(registry) {
    const out = new Map();
    for (const raw of (Array.isArray(registry?.events) ? registry.events : [])) {
      const id = trim(raw.id), month = trim(raw.eventMonth), sources = cleanSources(raw.sources);
      if (!id || !/^\d{4}-(0[1-9]|1[0-2])$/.test(month) || !sources.length) continue;
      const evt = {month, label:trim(raw.eventLabel), sources, pauseStatus:trim(raw.pauseStatus)||'unresolved',
        reviewNote:trim(raw.reviewNote), pauseEvidence:cleanSources(raw.pauseEvidence), evidence:'archived-linked'};
      if (!out.has(id)) out.set(id, []);
      out.get(id).push(evt);
    }
    for (const list of out.values()) list.sort((a,b)=>a.month.localeCompare(b.month));
    return out;
  }
  function build(index, registry, options={}) {
    if (!Array.isArray(index)) throw new TypeError('Expected an array of film records');
    const c = configOf(index, options), nations = new Set(c.domesticNations), methods = methodsFor(c);
    const eventsByActor = eventMap(registry);
    const coverage = {indexFilms:index.length,domesticFilms:0,afterCutoffFilms:0,inWindowFilms:0,datedFilms:0,undatedFilms:0,
      scoredFilms:0,unscoredDatedFilms:0,emptyCastFilms:0,partialCastFilms:0,noStableCastFilms:0,
      unlinkedCredits:0,unscoredActors:0,scoredCredits:0,scoredStableCredits:0,partialScoreDays:0,unscoredDays:0,
      unknownDateCredits:0,registryActors:eventsByActor.size,eventOverlapPoints:0,lastStableScoreDate:null};
    const unique = new Map();
    for (const row of index) {
      if (!nations.has(trim(row.nation))) continue;
      const date = iso(row.openDt), id = trim(row.movieCd) || ('title:' + trim(row.movieNm) + '|' + date);
      const audience = number(row.audiAcc), cast = Array.isArray(row.actors) ? row.actors : [];
      const old = unique.get(id);
      // Match historical deduplication precedence; actual archive has unique film codes.
      if (!old || (audience || 0) > (old.audience || 0) || (!old.rawCast.length && cast.length))
        unique.set(id, {id,title:trim(row.movieNm)||id,date:date||null,audience,rawCast:cast});
    }
    coverage.domesticFilms = unique.size;
    const films = [], totals = Object.create(null), annual = Object.create(null), actors = new Map(), unlinkedCredits = [];
    for (const m of unique.values()) {
      if (m.date && m.date > c.cutoffDate) {coverage.afterCutoffFilms++;continue;}
      films.push(m); coverage.inWindowFilms++;
      const keys = new Set(); const cast = []; const credits=[];
      m.missingIds=0;
      for (let j=0;j<m.rawCast.length;j++) {
        const a = m.rawCast[j] || {}, id=trim(a.id), name=trim(a.name);
        // An anonymous credit is not an identity. Preserve each individual anonymous slot.
        const key = id?'id:'+id:name?'name:'+name:'slot:'+j;
        if (keys.has(key)) continue;
        keys.add(key);
        cast.push({id,name});
        if (!id) {m.missingIds++;unlinkedCredits.push({name,filmId:m.id,title:m.title,date:m.date,rank:j+1});}
        if (id || name) credits.push({id,name,key,rank:j+1});
        if (id && !actors.has(id)) actors.set(id,{id,name:name||id,records:[],days:[],scored:[],undated:[],points:[]});
        else if (id && actors.get(id).name===id && name) actors.get(id).name=name;
      }
      m.cast=cast; m.credits=credits; delete m.rawCast;
      if (!cast.length) coverage.emptyCastFilms++;
      if (m.missingIds) coverage.partialCastFilms++;
      if (!cast.some(a=>a.id)) coverage.noStableCastFilms++;
      if (!m.date) {coverage.undatedFilms++;continue;}
      coverage.datedFilms++;
      const yr=m.date.slice(0,4), y=annual[yr]||(annual[yr]={year:yr,datedFilms:0,scoredFilms:0,audience:0});
      y.datedFilms++;
      if (m.audience>0) {
        coverage.scoredFilms++;y.scoredFilms++;y.audience+=m.audience;totals[yr]=y.audience;
      } else coverage.unscoredDatedFilms++;
    }
    coverage.unlinkedCredits=unlinkedCredits.length;
    const ordered = films.filter(m=>m.date).sort((a,b)=>byDate(a,b)||byId(a,b));
    function addRecord(a,m,score) {
      if (!a.id) return;
      const actor = actors.get(a.id);
      const rec = {id:m.id,title:m.title,date:m.date,rank:a.rank,audience:m.audience,
        yearTotal:m.date ? totals[m.date.slice(0,4)]||null : null, marketWeight:null,roleWeight:null,baseSp:null,
        historyWeight:null,historyCount:null,historySum:null,priorMean:null,historyEstimate:null,sp:null,cast:m.cast,missingIds:m.missingIds,
        ...score};
      actor.records.push(rec);
      if (m.date) {
        let day=actor.days[actor.days.length-1];
        if (!day || day.date!==m.date) {
          day={date:m.date,epoch:Date.parse(m.date+'T00:00:00Z')/MS_DAY,sp:null,films:[],scoredCount:0,sum:0};
          actor.days.push(day);
        }
        day.films.push(rec);
        if (rec.sp!=null){day.sum+=rec.sp;day.scoredCount++;actor.scored.push(rec);}
      } else {actor.undated.push(rec);coverage.unknownDateCredits++;}
    }
    const hist = new Map(); let globalSum=0, globalN=0;
    for (let i=0;i<ordered.length;) {
      let end=i+1; while(end<ordered.length && ordered[end].date===ordered[i].date) end++;
      const mu=globalN ? globalSum/globalN : null, updates=[];
      for (let j=i;j<end;j++) {
        const m = ordered[j];
        const q = FORMULA.market(m.audience, totals[m.date.slice(0, 4)]);
        // Match original model's film/actor-key summation order where possible.
        const credits=m.credits.slice().sort((a,b)=>a.key.localeCompare(b.key));
        for (const a of credits) {
          let score={};
          if (q!=null) {
            const h=hist.get(a.key), count=h?h.count:0, sum=h?h.sum:0;
            const rw=FORMULA.role(a.rank,c), base=q*rw, hw=FORMULA.history(sum,count,mu,c);
            score={marketWeight:q,roleWeight:rw,baseSp:base,historyCount:count,historySum:sum,priorMean:mu,
              historyEstimate:hw.estimate,historyWeight:hw.weight,sp:FORMULA.score(q,rw,hw.weight,c)};
            updates.push({key:a.key,base});coverage.scoredCredits++;
            if (a.id) {coverage.scoredStableCredits++;coverage.lastStableScoreDate=m.date;}
          }
          addRecord(a,m,score);
        }
      }
      // A whole release day is scored before any member contributes to history.
      for (const u of updates) {
        let h=hist.get(u.key); if(!h) {h={sum:0,count:0};hist.set(u.key,h);}
        h.sum+=u.base;h.count++;globalSum+=u.base;globalN++;
      }
      i=end;
    }
    for (const m of films) if(!m.date) for(const a of m.credits) addRecord(a,m,{});
    const triples=[], values=[];
    let scoredActors=0, eligibleActors=0;
    for (const a of actors.values()) {
      const sdays=[];
      for(let i=0;i<a.days.length;i++){
        const d=a.days[i];d.index=i;
        d.sp=d.scoredCount ? d.sum/d.scoredCount : null;
        d.partial=d.scoredCount>0 && d.scoredCount<d.films.length;
        if(d.partial) coverage.partialScoreDays++;
        if(d.sp===null) coverage.unscoredDays++; else sdays.push(d);
        delete d.sum;
      }
      a.dateCount=sdays.length;
      if(sdays.length) scoredActors++;
      if(sdays.length>=3) eligibleActors++;
      for(let i=1;i+1<sdays.length;i++) {
        const p=sdays[i-1], d=sdays[i], n=sdays[i+1];
        const before=FORMULA.slope(p,d,c),after=FORMULA.slope(d,n,c),change=after-before;
        triples.push({a,p,d,n,before,after,change});values.push(Math.abs(change));
      }
    }
    coverage.unscoredActors=actors.size-scoredActors;
    const cutoff=nearestRank(values,c.percentile);
    const counters=()=>({hits:0,evaluable:0,unknown:0,total:0,misses:0});
    const comparisonCounts=()=>({pivot:{points:counters(),actors:counters()},next:{points:counters(),actors:counters()}});
    const comparisons=comparisonCounts();
    const byDirection={rise:{actors:0,points:0,comparisons:comparisonCounts()},fall:{actors:0,points:0,comparisons:comparisonCounts()}};
    const addCount=(cnt,status)=>{cnt.total++;if(status==='unknown')cnt.unknown++;else{cnt.evaluable++;if(status==='hit')cnt.hits++;else cnt.misses++;}};
    function pools(a,d) {
      const aMovies=new Set(), pastCo=new Set();
      // Observed co-actors without any joint scored film are not computable.
      for(const r of a.scored) {
        if (r.date>=d.date) break;
        aMovies.add(r.id);
        for(const b of r.cast) if(b.id && b.id!==a.id) pastCo.add(b.id);
      }
      const pool=[];
      for(const bid of pastCo) {
        const b=actors.get(bid); if(!b)continue;
        let withSum=0,withoutSum=0,withCount=0,withoutCount=0;
        for(const r of b.scored){
          if(r.date>=d.date)break;
          if(aMovies.has(r.id)){withSum+=r.sp;withCount++;}else{withoutSum+=r.sp;withoutCount++;}
        }
        if(!withCount || !withoutCount || !(withoutSum>0))continue;
        const withMean=withSum/withCount,withoutMean=withoutSum/withoutCount;
        pool.push({id:b.id,name:b.name,value:FORMULA.synergy(withMean,withoutMean),withCount,withoutCount,withMean,withoutMean});
      }
      const selected=selectHigh(pool,c.topFraction);
      return {poolSize:pool.length,high:selected.high,boundary:selected.boundary,highCount:selected.high.length};
    }
    function compare(a,day,pool) {
      const r={date:day?day.date:null,films:day?day.films:[],status:'unknown',reason:'no_target',matches:[],incompleteCast:false};
      if(!day)return r;
      const ids=new Set();
      for(const m of day.films){
        let companions=0;
        for(const k of m.cast) if(k.id && k.id!==a.id){ids.add(k.id);companions++;}
        if(!companions || m.missingIds)r.incompleteCast=true;
      }
      if(!pool.poolSize){r.reason='no_pool';return r;}
      r.matches=pool.high.filter(h=>ids.has(h.id));
      if(r.matches.length){r.status='hit';r.reason='match';}
      else if(!pool.highCount){r.status='miss';r.reason='no_positive';}
      else if(r.incompleteCast){r.reason='incomplete_cast';}
      else {r.status='miss';r.reason='no_match';}
      return r;
    }
    let markedActors=0,pointCount=0;const candidates={rise:[],fall:[]};
    for(const {a,p,d,n,before,after,change} of triples) {
      if(change===0 || Math.abs(change)<cutoff)continue;
      const pool=pools(a,d),nextDay=a.days[d.index+1]||null;
      const ev=(eventsByActor.get(a.id)||[]).filter(x=>x.month>=p.date.slice(0,7) && x.month<=n.date.slice(0,7));
      const point={date:d.date,prevDate:p.date,nextDate:n.date,detectedDate:n.date,beforeSlope:before,afterSlope:after,change,
        prevSp:p.sp,sp:d.sp,nextSp:n.sp, skippedBefore:d.index-p.index-1,skippedAfter:n.index-d.index-1,
        pivot:compare(a,d,pool),next:compare(a,nextDay,pool),events:ev,...pool};
      if(ev.length)coverage.eventOverlapPoints++;
      a.points.push(point);pointCount++;
      const group=byDirection[change>0?'rise':'fall'];group.points++;
      for(const k of ['pivot','next']) {
        addCount(comparisons[k].points,point[k].status);
        addCount(group.comparisons[k].points,point[k].status);
      }
    }
    const roster=[];
    function actorBase(a) {
      return {id:a.id,name:a.name,filmCount:a.records.length,scoredCount:a.scored.length,dateCount:a.dateCount,
        datedCount:a.days.length,pointCount:a.points.length,firstRise:a.firstRise||null,firstFall:a.firstFall||null};
    }
    for(const a of actors.values()){
      if(a.points.length) {
        markedActors++;
        for(const k of ['pivot','next']) {
          const labels=a.points.map(p=>p[k].status);
          addCount(comparisons[k].actors,labels.includes('hit')?'hit':labels.includes('unknown')?'unknown':'miss');
        }
        for(const [k,sign] of [['rise',1],['fall',-1]]){
          const p=a.points.find(p=>p.change*sign>0);
          if(p) {
            const group=byDirection[k];group.actors++;
            for(const target of ['pivot','next']){
              const labels=a.points.filter(pt=>pt.change*sign>0).map(pt=>pt[target].status);
              addCount(group.comparisons[target].actors,labels.includes('hit')?'hit':labels.includes('unknown')?'unknown':'miss');
            }
            a[k==='rise'?'firstRise':'firstFall']=p.date;
            candidates[k].push({id:a.id,name:a.name,date:p.date,change:p.change,detectedDate:p.detectedDate});
          }
        }
      }
      roster.push(actorBase(a));
    }
    for (const arr of Object.values(candidates))arr.sort((a,b)=>byDate(a,b)||Math.abs(b.change)-Math.abs(a.change)||byId(a,b));
    roster.sort((a,b)=>a.name.localeCompare(b.name,'ko')||byId(a,b));
    coverage.latestStableScoreDate=coverage.lastStableScoreDate;
    const summary={actors:actors.size,scoredActors,eligibleActors,markedActors,points:pointCount,triples:triples.length,cutoff,comparisons,byDirection,coverage,candidates,
      annual:Object.values(annual).sort((a,b)=>a.year.localeCompare(b.year)),firstDate:ordered.length?ordered[0].date:null,
      lastDate:ordered.length?ordered[ordered.length-1].date:null};
    function actor(id) {
      const a = actors.get(trim(id).replace(/^id:/, ''));
      if (!a) return null;
      // Only the selected actor needs the extended follow-up. Reuse each day's
      // film array (and the first comparison itself) in the worker payload.
      const dayIndex = new Map(a.days.map(day => [day.date, day.index]));
      const points = a.points.map(point => {
        const later = [];
        const firstIndex = dayIndex.get(point.date) + 1;
        if (firstIndex < a.days.length) {
          later.push(point.next);
          for (let i = firstIndex + 1; i < a.days.length; i++) {
            later.push(compare(a, a.days[i], point));
          }
        }
        return {...point, later};
      });
      return {...actorBase(a), days:a.days, undated:a.undated, points, events:eventsByActor.get(a.id)||[]};
    }
    return {roster,unlinkedCredits,summary,actor,methods};
  }
  return Object.freeze({CONFIG,FORMULA,build,nearestRank,selectHigh});
});
