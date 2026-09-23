#!/usr/bin/env node
'use strict';
const fs=require('node:fs'),path=require('node:path'),crypto=require('node:crypto');
const root=path.resolve(__dirname,'..'),read=p=>fs.readFileSync(path.join(root,p)),hash=p=>crypto.createHash('sha256').update(read(p)).digest('hex');
const S=require('../docs/assets/starpower-core.js'),H=require('../docs/assets/hawk-criterion-core.js'),E=require('../docs/assets/event-recovery-core.js'),X=require('../docs/assets/event-recovery-ex-core.js');
const index='docs/data/search_index.json',registryPath='docs/data/research/event_recovery_ex_registry.json';
const movies=JSON.parse(read(index)),registry=JSON.parse(read(registryPath)),model=S.buildModel(movies),yearly=H.buildAnnualScores(model),summary=H.summarizeYearlyKnees(yearly,model.config.cutoffDate);
const fdr=H.persistentHawksFdr(yearly,summary.median,.05,summary.coverage.rows.filter(r=>r.includedForAggregate).map(r=>r.year));
const ids=new Set(fdr.significant.map(r=>r.id));
if(new Set(registry.events.map(e=>e.id)).size!==registry.events.length)throw Error('대표 사건은 배우별 1건만 허용합니다.');
if(registry.events.some(e=>e.pauseStatus==='documented'&&!e.pauseEvidence?.length))throw Error('공백 확인 사례에는 출처가 필요합니다.');
const candidates=registry.events.filter(e=>ids.has(e.id));
const analysis=E.analyze(movies,candidates),cases=analysis.cases.map(c=>{
 const pre=H.buildPreEventAnalysis(S,movies,{eventDate:c.eventMonth+'-01',lambda:S.CONFIG.lambda});
 const pf=H.persistentHawksFdr(pre.yearly,pre.summary.median,.05,pre.summary.coverage.rows.filter(r=>r.includedForAggregate).map(r=>r.year));
 const partnerTests=new Map(pf.tests.map(t=>[t.id,t])),partnerHawks=new Set(pf.significant.map(t=>t.id));
 for(const f of c.returnFilms)for(const a of f.coactors){a.partnerPreHawk=partnerTests.has(a.id)?partnerHawks.has(a.id):null;a.partnerPreQ=partnerTests.get(a.id)?.qValue??null;}
 const returnDate=c.returnFilms[0]?.openDt||null,comparison=X.compare(c.filmHistory,c.eventMonth,returnDate,model.config.cutoffDate);
 const stage1=c.preEvent.isHawk&&c.pauseStatus==='documented',opportunity=c.window.returnStart<=model.config.cutoffDate;
 const patterns=[10,25,50].map(percent=>{const synergy=returnDate?c.returnFilms.some(f=>f.sensitivity.find(s=>s.percent===percent)?.observed===true):null;const base=X.pattern({preHawk:c.preEvent.isHawk,pause:c.pauseStatus==='documented',returned:!!returnDate,synergy,comparison});return {percent,...base};});
 const postRows=c.annualStates.filter(r=>r.year>Number(c.eventMonth.slice(0,4))),d=postRows.find(r=>r.state==='Dove'),h=d?postRows.find(r=>r.year>d.year&&r.state==='Hawk'):null;
 return {...c,selected:stage1,stage1,opportunity,returnObserved:!!returnDate,comparison,patterns,observedAnnualRecovery:{doveYear:d?.year||null,hawkYear:h?.year||null,observed:!!h,contiguous:c.sequence.contiguousHDH},returnObservationStatus:returnDate?'observed':opportunity?'not_in_snapshot':'followup_not_elapsed'};
});
const roster=fdr.significant.map(h=>{const audit=registry.actors.find(a=>a.id===h.id)||{status:'not_reviewed',note:'현재 Hawk로 새로 편입됨. 공개 자료 조사 필요.',sources:[],queries:[]};const c=cases.find(c=>c.id===h.id);return {...h,...audit,reviewed:!!audit.reviewedAt,eventId:c?.eventId||null,preHawk:c?.preEvent.isHawk??null,stage1:!!c?.stage1,returnObserved:!!c?.returnObserved};});
const total=roster.length,stage1=cases.filter(c=>c.stage1),returned=stage1.filter(c=>c.returnObserved),opportunity=stage1.filter(c=>c.opportunity),documented=cases.filter(c=>c.pauseStatus==='documented');
const rates=[10,25,50].map(percent=>{const paired=returned.filter(c=>c.returnFilms.some(f=>f.sensitivity.find(s=>s.percent===percent)?.observed===true)),observed=returned.filter(c=>c.patterns.find(p=>p.percent===percent).status==='observed'),evaluable=returned.filter(c=>c.comparison.pre.mean!==null&&c.comparison.interim.mean!==null);return {percent,pausedOfAll:X.fraction(stage1.length,total),returnedOfPaused:X.fraction(returned.length,stage1.length),returnedOfMatured:X.fraction(returned.length,opportunity.length),returnedOfAll:X.fraction(returned.length,total),pairedOfReturned:X.fraction(paired.length,returned.length),pairedOfAll:X.fraction(paired.length,total),patternOfAll:X.fraction(observed.length,total),patternOfReturned:X.fraction(observed.length,returned.length),dualBaselineEvaluable:X.fraction(evaluable.length,returned.length),patternOfEvaluable:X.fraction(observed.length,evaluable.length),documentedPauseOfAll:X.fraction(documented.length,total)};});
const result={schemaVersion:1,config:{...model.config,globalHawkPercent:summary.median,fdrQ:.05,minHits:2,primarySynergyPercent:25},screening:{reviewedAt:registry.reviewedAt,scope:registry.scope,limit:registry.screeningLimit,primaryEventRule:registry.primaryEventRule},counts:{hawkCount:total,screened:roster.filter(r=>r.reviewed).length,eventCandidates:cases.length,documentedPause:documented.length,stage1:stage1.length,returnObserved:returned.length,followupNotElapsed:stage1.filter(c=>!c.opportunity).length},rates,roster,cases,provenance:{generatedAt:new Date().toISOString(),inputs:{[index]:hash(index),[registryPath]:hash(registryPath)},code:{}}};
for(const p of ['docs/assets/starpower-core.js','docs/assets/hawk-criterion-core.js','docs/assets/event-recovery-core.js','docs/assets/event-recovery-ex-core.js','scripts/build_event_recovery_ex.js'])result.provenance.code[p]=hash(p);
fs.writeFileSync(path.join(root,'docs/data/research/event_recovery_ex.json'),JSON.stringify(result,null,2)+'\n');
console.log(JSON.stringify({counts:result.counts,cases:cases.map(c=>({name:c.name,pre:c.preEvent.isHawk,pause:c.pauseStatus,stage1:c.stage1,return:c.returnFilms.map(f=>f.movieNm),interim:c.comparison.interim,preMean:c.comparison.pre.mean,ratios:[c.comparison.toPre.ratio,c.comparison.toInterim.ratio],pattern:c.patterns}))},null,2));
