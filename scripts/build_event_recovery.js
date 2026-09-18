#!/usr/bin/env node
'use strict';
const fs=require('node:fs'),path=require('node:path'),crypto=require('node:crypto');
const root=path.resolve(__dirname,'..');
const read=p=>fs.readFileSync(path.join(root,p));
const digest=b=>crypto.createHash('sha256').update(b).digest('hex');
const E=require('../docs/assets/event-recovery-core.js');
const indexPath='docs/data/search_index.json',casePath='docs/data/research/event_cases.json';
const raw=read(indexPath),spec=read(casePath);
const result=E.analyze(JSON.parse(raw),JSON.parse(spec).candidates);
result.provenance={generatedAt:new Date().toISOString(),inputs:{[indexPath]:digest(raw),[casePath]:digest(spec)},code:{}};
for(const p of ['docs/assets/starpower-core.js','docs/assets/hawk-criterion-core.js','docs/assets/event-recovery-core.js','scripts/build_event_recovery.js'])result.provenance.code[p]=digest(read(p));
fs.writeFileSync(path.join(root,'docs/data/research/event_recovery.json'),JSON.stringify(result,null,2)+'\n');
console.log(JSON.stringify({counts:result.counts,cases:result.cases.map(c=>({name:c.name,selected:c.selected,reasons:c.eligibilityReasons,preState:c.preEvent.previousState,returns:c.returnFilms.map(f=>({name:f.movieNm,date:f.openDt,sp:f.starPower,mean:f.baselineMean,ratio:f.ratioToPrior,preMean:f.preEventBaselineMean,preRatio:f.ratioToPreEvent,matched:f.sensitivity.find(x=>x.percent===25).matched})),states:c.annualStates.map(x=>`${x.year}:${x.state}`)}))},null,2));
