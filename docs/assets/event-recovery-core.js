/* Release-based exploratory case study. Existing SP/Hawk cores are unchanged. */
(function(root,factory){
  const api = typeof module==='object' && module.exports
    ? factory(require('./starpower-core.js'),require('./hawk-criterion-core.js'))
    : factory(root.StarPowerCore,root.HawkCriterionCore);
  if(typeof module==='object' && module.exports) module.exports=api;
  else root.EventRecoveryCore=api;
})(typeof globalThis!=='undefined'?globalThis:this,function(S,H){
  'use strict';
  const mean=xs=>xs.length?xs.reduce((s,x)=>s+x,0)/xs.length:null;
  const ratio=(n,d)=>Number.isFinite(n)&&Number.isFinite(d)&&d>0?n/d:null;
  const compact=d=>d.toISOString().slice(0,10).replace(/-/g,'');
  const plus=(d,n)=>{const x=new Date(d);x.setUTCDate(x.getUTCDate()+n);return compact(x);};
  function eventWindow(month){
    if(!/^\d{4}-(0[1-9]|1[0-2])$/.test(month))throw new Error('사건 월은 YYYY-MM 형식이어야 합니다.');
    const [y,m]=month.split('-').map(Number), start=new Date(Date.UTC(y,m-1,1)),end=new Date(Date.UTC(y,m,0));
    return {monthStart:compact(start),monthEnd:compact(end),preCutoff:plus(start,-1),returnStart:plus(end,365),alternativeStart:plus(start,365)};
  }
  function firstReturns(model,id,start){
    const obs=(model.actorStats.get(`id:${id}`)?.observations||[]).filter(x=>x.openDt>=start);
    const date=obs.reduce((d,x)=>!d||x.openDt<d?x.openDt:d,null);
    return obs.filter(x=>x.openDt===date).sort((a,b)=>a.movieCd.localeCompare(b.movieCd));
  }
  function highSynergy(pool,percent){
    if(!Number.isFinite(percent)||percent<=0||percent>100)throw new Error('percent must be in (0,100]');
    const ranked=pool.filter(x=>Number.isFinite(x.value)).sort((a,b)=>b.value-a.value||a.key.localeCompare(b.key));
    const nominalCount=Math.ceil(ranked.length*percent/100),threshold=nominalCount?ranked[nominalCount-1].value:null;
    const keys=ranked.filter(x=>x.value>0&&x.value>=threshold).map(x=>x.key);
    return {percent,computableCount:ranked.length,nominalCount,threshold,keys,tiesIncluded:true};
  }
  function sequenceEvidence(rows){
    let phase=0,contiguousHDH=false;
    const adjacent=[];
    for(let i=0;i<rows.length;i++){
      const r=rows[i],previous=rows[i-1];
      if(previous&&r.year!==previous.year+1)phase=0;
      if(r.state==='Unknown'||r.state==='Mixed'){phase=0;continue;}
      if(previous&&r.year===previous.year+1&&['Hawk','Dove'].includes(previous.state)&&previous.state!==r.state)
        adjacent.push({fromYear:previous.year,toYear:r.year,from:previous.state,to:r.state});
      if(r.state==='Hawk'){if(phase===2)contiguousHDH=true;phase=1;}
      else if(phase===1)phase=2;
    }
    return {contiguousHDH,adjacent};
  }
  function fdrFor(yearly,summary){
    const includedYears=summary.coverage.rows.filter(x=>x.includedForAggregate).map(x=>x.year);
    return {includedYears,...H.persistentHawksFdr(yearly,summary.median,.05,includedYears)};
  }
  function briefTest(f,id){
    const t=f.tests.find(x=>x.id===id);
    return {isHawk:f.significant.some(x=>x.id===id),activeCount:t?.activeCount||0,hitCount:t?.hitCount||0,pValue:t?.pValue??null,qValue:t?.qValue??null};
  }
  function analyze(movies,candidates){
    const model=S.buildModel(movies),yearly=H.buildAnnualScores(model),summary=H.summarizeYearlyKnees(yearly,model.config.cutoffDate),fdr=fdrFor(yearly,summary);
    const fullCoverage=new Map(summary.coverage.rows.map(x=>[x.year,x]));
    const movieMap=new Map(model.eligibleMovies.map(x=>[x.movieCd,x]));
    const results=[];
    for(const candidate of [...candidates].sort((a,b)=>a.priority-b.priority)){
      const {id}=candidate,key=`id:${id}`,window=eventWindow(candidate.eventMonth),eventYear=Number(candidate.eventMonth.slice(0,4));
      const pre=H.buildPreEventAnalysis(S,movies,{eventDate:candidate.eventMonth+'-01',lambda:S.CONFIG.lambda});
      const pf=fdrFor(pre.yearly,pre.summary),current=briefTest(fdr,id),preTest=briefTest(pf,id);
      const preObs=pre.model.actorStats.get(key)?.observations||[],allObs=model.actorStats.get(key)?.observations||[];
      const preMovieIds=new Set(preObs.map(x=>x.movieCd)),poolKeys=new Set();
      for(const film of pre.model.eligibleMovies)if(preMovieIds.has(film.movieCd))for(const a of film.actors)if(a.id&&a.id!==id)poolKeys.add(`id:${a.id}`);
      const synergyPool=[...poolKeys].map(otherKey=>{
        const d=S.getDirectedSynergy(pre.model,key,otherKey),reverse=S.getDirectedSynergy(pre.model,otherKey,key);
        const jointFilms=(pre.model.actorStats.get(otherKey)?.observations||[]).filter(o=>preMovieIds.has(o.movieCd)).map(o=>({movieCd:o.movieCd,movieNm:o.movieNm,openDt:o.openDt}));
        return {key:otherKey,id:otherKey.slice(3),name:pre.model.actorStats.get(otherKey)?.name||otherKey,...d,jointFilms,reverseValue:reverse?.value??null,lowSample:(d?.withCount||0)<3||(d?.withoutCount||0)<3};
      }).sort((a,b)=>(b.value??-Infinity)-(a.value??-Infinity)||a.key.localeCompare(b.key));
      const levels=[10,25,50].map(p=>highSynergy(synergyPool,p));
      const returnObs=firstReturns(model,id,window.returnStart),alternative=firstReturns(model,id,window.alternativeStart);
      const returnFilms=returnObs.map(o=>{
        const film=movieMap.get(o.movieCd),prior=allObs.filter(x=>x.openDt<o.openDt),priorEvent=allObs.filter(x=>x.openDt<window.monthStart);
        const castKeys=new Set((film?.actors||[]).filter(a=>a.id&&a.id!==id).map(a=>`id:${a.id}`));
        const coactors=[...castKeys].map(k=>{
          const p=synergyPool.find(x=>x.key===k),a=film.actors.find(x=>`id:${x.id}`===k);
          return p?{...p,priorCollaborator:true}:{key:k,id:a.id,name:a.name,priorCollaborator:false,value:null,withCount:0,withoutCount:null,lowSample:true};
        });
        const baseline=mean(prior.map(x=>x.starPower)),preBaseline=mean(priorEvent.map(x=>x.starPower));
        return {movieCd:o.movieCd,movieNm:o.movieNm,openDt:o.openDt,audience:o.audience,starPower:o.starPower,roleRank:o.roleRank,baselineCount:prior.length,baselineMean:baseline,ratioToPrior:ratio(o.starPower,baseline),preEventBaselineCount:priorEvent.length,preEventBaselineMean:preBaseline,ratioToPreEvent:ratio(o.starPower,preBaseline),coactors,
          sensitivity:levels.map(level=>({...level,matched:coactors.filter(x=>level.keys.includes(x.key)).map(x=>({id:x.id,name:x.name,value:x.value,withCount:x.withCount,withoutCount:x.withoutCount,lowSample:x.lowSample})),observed:level.computableCount?coactors.some(x=>level.keys.includes(x.key)):null})),
          scoreBasis:'fixed_final_snapshot_release_year_cohort'};
      });
      const preStates=H.annualStates(pre.yearly,pre.summary.median,{includedYears:pf.includedYears});
      const previousState=preStates[key]?.[eventYear-1]||'Unknown';
      const rows=[];
      for(let y=eventYear-1;y<=Number(model.config.cutoffDate.slice(0,4));y++){
        const year=String(y),map=yearly[year],row=map?.get(key),coverage=fullCoverage.get(year),top=map?H.topSetForYear(map,pre.summary.median):null;
        let state=y===eventYear-1?previousState:y===eventYear?'Mixed':coverage?.includedForAggregate&&row?(top.topKeys.has(key)?'Hawk':'Dove'):'Unknown';
        const preRow=pre.yearly[year]?.get(key);
        rows.push({year:y,state,annualSP:y===eventYear-1?(preRow?.score??null):(row?.score??null),filmCount:y===eventYear-1?(preRow?.filmCount||0):(row?.filmCount||0),rank:top?.rows.findIndex(x=>x.actorKey===key)>=0?top.rows.findIndex(x=>x.actorKey===key)+1:null,actorCount:map?.size||0,partialYear:coverage?.partialYear||false,lowCoverage:coverage?.lowCoverage||false});
      }
      const eligibilityReasons=[];
      if(!current.isHawk)eligibilityReasons.push('현재 반복성·BH-FDR Hawk 기준 미충족');
      if(!preTest.isHawk)eligibilityReasons.push('사건 이전 반복성·BH-FDR Hawk 기준 미충족');
      if(!returnFilms.length)eligibilityReasons.push('365일 이후 적격 개봉작 없음');
      const preEvent={...preTest,percent:pre.summary.median,filmCount:preObs.length,previousYear:eventYear-1,previousState,cutoffDate:pre.cutoffDate,testedActors:pf.testedActorCount,includedYears:pf.includedYears};
      const sinceIncident=movies.filter(m=>Array.isArray(m.actors)&&m.actors.some(a=>a.id===id)&&S.normalizeDate(m.openDt)>=window.returnStart&&S.normalizeDate(m.openDt)<=S.CONFIG.cutoffDate&&!S.isEligibleMovie(m));
      results.push({...candidate,window,current,preEvent,eligible:!eligibilityReasons.length,eligibilityReasons,selected:false,returnFilms,alternativeReturnFilms:alternative.map(o=>({movieCd:o.movieCd,movieNm:o.movieNm,openDt:o.openDt})),monthBoundaryChangesReturn:returnObs.map(x=>x.movieCd).join('|')!==alternative.map(x=>x.movieCd).join('|'),synergyPool,annualStates:rows,sequence:sequenceEvidence(rows),omittedReturnCandidates:sinceIncident.map(m=>({movieCd:m.movieCd,movieNm:m.movieNm,openDt:m.openDt,audiAcc:m.audiAcc,nation:m.nation})),filmHistory:allObs.map(o=>({movieCd:o.movieCd,movieNm:o.movieNm,openDt:o.openDt,starPower:o.starPower,roleRank:o.roleRank,audience:o.audience}))});
    }
    let selected=0;for(const r of results)if(r.eligible&&selected<3){r.selected=true;selected++;}
    return {schemaVersion:1,analysisKind:'exploratory_release_based_case_series',config:{...S.CONFIG,globalHawkPercent:summary.median,globalHawkCount:fdr.hawkCount,fdrQ:.05,minHits:2,eventBasis:'public_disclosure_month',returnRule:'month_end_plus_365_days_inclusive',preEventRule:'strictly_before_month_start',synergyDirection:'target_A_to_coactor_B',primarySynergyPercent:25,synergySensitivities:[10,25,50],highSynergyRule:'positive AND top ceil(N*p/100) among computable prior ID coactors; boundary ties included',annualStateRule:'pre-event-calibrated percentile frozen; incident year Mixed; missing/partial/low-coverage Unknown',scoreBasis:'retrospective audience snapshot; lambda calibrated on full snapshot; not historical as-of'},counts:{eligibleMovies:model.eligibleMovies.length,observations:model.observations.length,selectedCases:selected},cases:results};
  }
  return Object.freeze({eventWindow,firstReturns,highSynergy,ratio,sequenceEvidence,analyze});
});
