#!/usr/bin/env node
'use strict';
// Reuses the site's unchanged scoring core. No network requests or source mutations.
const fs=require('node:fs'),path=require('node:path'),os=require('node:os'),crypto=require('node:crypto'),cp=require('node:child_process');
const ROOT=path.resolve(__dirname,'..');
const read=p=>fs.readFileSync(path.join(ROOT,p));
const load=p=>JSON.parse(read(p));
const sha=p=>crypto.createHash('sha256').update(read(p)).digest('hex');
const S=require('../docs/assets/starpower-core.js');
const H=require('../docs/assets/hawk-criterion-core.js');
const indexPath='docs/data/search_index.json', registryPath='docs/data/research/recovery_followup_registry.json', baselinePath='docs/data/research/recovery_followup_baseline.json';
const baseline=load(baselinePath), registry=load(registryPath);
const model=S.buildModel(load(indexPath),baseline.config);
const annual=H.buildAnnualScores(model),summary=H.summarizeYearlyKnees(annual,model.config.cutoffDate),annualRows=[];
for(const c of summary.coverage.rows){
 const top=H.topSetForYear(annual[c.year],baseline.config.percent);
 for(const r of top.rows)if(r.id)annualRows.push({id:r.id,name:r.name,year:Number(c.year),score:r.score,films:r.filmCount,usable:c.includedForAggregate,high:top.topKeys.has(r.actorKey)});
}
const snapshot={config:{...model.config,percent:baseline.config.percent},counts:{indexRecords:load(indexPath).length,eligibleFilms:model.eligibleMovies.length,observations:model.observations.length,idObservations:model.observations.filter(x=>x.actorId).length},observations:model.observations.map(o=>({id:o.actorId,key:o.actorKey,name:o.actorName,movie:o.movieCd,title:o.movieNm,date:o.openDt,score:o.starPower,rank:o.roleRank,audience:o.audience})),annual:annualRows,events:registry.events};
const tmp=fs.mkdtempSync(path.join(os.tmpdir(),'recovery-followup-'));
const destination=path.join(ROOT,'docs/data/research/recovery_followup.json');
try{
 fs.writeFileSync(path.join(tmp,'snapshot.json'),JSON.stringify(snapshot));
 const py=process.env.PYTHON||'python3';
 cp.execFileSync(py,[path.join(ROOT,'scripts/analyze_recovery_followup.py'),path.join(tmp,'snapshot.json'),path.join(tmp,'result.json')],{stdio:'inherit'});
 const result=JSON.parse(fs.readFileSync(path.join(tmp,'result.json'),'utf8'));
 const checks=result.audit?.checks||[];
 const add=(name,passed,detail)=>checks.push({name,passed:Boolean(passed),detail});
 const canonical=snapshot.observations.map(o=>[o.id||null,o.key||null,o.movie,o.date,o.score]).sort((a,b)=>JSON.stringify(a).localeCompare(JSON.stringify(b),'en'));
 const observationSha256=crypto.createHash('sha256').update(JSON.stringify(canonical)).digest('hex');
 add('논문 전체 배우–영화 점수 스냅샷',observationSha256===baseline.observationSha256,observationSha256);
 for(const k of ['eligibleFilms','observations','idObservations'])add(`논문 입력 ${k}`,snapshot.counts[k]===baseline.counts[k],`${snapshot.counts[k]} / 기존 ${baseline.counts[k]}`);
 for(const [p,digest]of Object.entries(baseline.coreSha256))add(`기존 계산 코드 ${path.basename(p)}`,sha(p)===digest,'기존 논문 계산 코어와 SHA-256 대조');
 const observed=result.cases.filter(c=>c.first).map(c=>String(c.id)).sort();
 add('첫 개봉작 관찰 배우 6명',JSON.stringify(observed)===JSON.stringify([...baseline.primaryIds].sort()),observed.join(', '));
 const close=(a,b)=>Number.isFinite(a)&&Number.isFinite(b)&&Math.abs(a-b)<=1e-7*Math.max(1,Math.abs(b));
 for(const expected of baseline.firstReleases){
  const c=result.cases.find(c=>String(c.id)===String(expected.id));
  const ok=c&&c.first&&c.first.date===expected.date&&close(c.first.score,expected.score)&&close(c.preMean,expected.preMean)&&Boolean(c.first.synergy?.forward?.x)===expected.forwardAny25;
  add(`${expected.name}: 논문 첫 작품·SP·평균·재출연`,ok,`기준 개봉일 ${expected.date}, SP ${expected.score.toFixed(8)}`);
 }
 result.audit={...result.audit,checks,passed:checks.every(c=>c.passed),scope:'Input counts, unchanged scoring cores and six manuscript first-release observations; not all manuscript analyses.'};
 const inputPaths=[indexPath,registryPath,baselinePath];
 const codePaths=['docs/assets/starpower-core.js','docs/assets/hawk-criterion-core.js','scripts/build_recovery_followup.js','scripts/analyze_recovery_followup.py','docs/assets/recovery-followup-page.js','docs/assets/recovery-followup.css','docs/recovery-followup.html'];
 result.provenance={inputs:Object.fromEntries(inputPaths.map(p=>[p,sha(p)])),code:Object.fromEntries(codePaths.filter(p=>fs.existsSync(path.join(ROOT,p))).map(p=>[p,sha(p)])),generatedAt:new Date().toISOString(),newDataCollected:false,registryNote:registry.note};
 fs.writeFileSync(destination,JSON.stringify(result,null,2)+'\n');
 const cols=['id','name','eventMonth','preMean','firstDate','firstTitles','firstSP','secondDate','secondTitles','secondSP','delta','normalizedDelta','ratioToPre','ratioToFirst','improved','aboveBaseline','firstForwardReunion','secondForwardReunion','secondReverseReunion'];
 const csv=x=>'"'+String(x??'').replaceAll('"','""')+'"';
 const rows=result.cases.map(c=>({ ...c,firstDate:c.first?.date,firstTitles:c.first?.movies.map(m=>m.title).join(' / '),firstSP:c.first?.score,secondDate:c.second?.date,secondTitles:c.second?.movies.map(m=>m.title).join(' / '),secondSP:c.second?.score,firstForwardReunion:c.first?Boolean(c.first.synergy.forward.x):null,secondForwardReunion:c.second?Boolean(c.second.synergy.forward.x):null,secondReverseReunion:c.second?Boolean(c.second.synergy.reverse.x):null}));
 fs.writeFileSync(path.join(ROOT,'docs/data/research/recovery_followup.csv'),'\ufeff'+[cols.join(','),...rows.map(r=>cols.map(k=>csv(r[k])).join(','))].join('\n')+'\n');
 console.log(JSON.stringify({study:'fixed-second-release',...result.counts,baselineChecksPassed:result.audit.passed,experiment2Implemented:false,output:'docs/data/research/recovery_followup.json'},null,2));
 if(!result.audit.passed){
  console.error('Baseline comparison differs. Publication stopped: inspect failed checks before replacing the frozen manuscript results.');
  process.exitCode=2;
 }
}finally{fs.rmSync(tmp,{recursive:true,force:true});}
