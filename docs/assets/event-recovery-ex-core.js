/* Additive EX analyses. SP, lambda and Hawk definitions stay in the existing cores. */
(function(root,factory){const api=factory();if(typeof module==='object'&&module.exports)module.exports=api;else root.EventRecoveryEX=api;})(typeof globalThis!=='undefined'?globalThis:this,function(){
'use strict';
const mean=a=>a.length?a.reduce((s,x)=>s+x,0)/a.length:null;
function summary(rows){return {count:rows.length,mean:mean(rows.map(r=>r.starPower)),sum:rows.length?rows.reduce((s,r)=>s+r.starPower,0):null};}
function change(value,base){const ok=Number.isFinite(value)&&Number.isFinite(base)&&base>0;return {value,base,delta:Number.isFinite(value)&&Number.isFinite(base)?value-base:null,ratio:ok?value/base:null,percent:ok?100*(value/base-1):null};}
function fraction(n,d){return {n,d,percent:d>0?100*n/d:null};}
function compare(history,eventMonth,returnDate,cutoff){
 const start=eventMonth.replace('-','')+'01',eventYear=Number(start.slice(0,4)),returnYear=returnDate?Number(returnDate.slice(0,4)):null;
 const rows=(history||[]).filter(r=>r.openDt<=cutoff&&Number.isFinite(r.starPower));
 const pre=summary(rows.filter(r=>r.openDt<start));
 const interim=summary(returnDate?rows.filter(r=>r.openDt.slice(0,6)>start.slice(0,6)&&r.openDt<returnDate):[]);
 const inclusiveInterim=summary(returnDate?rows.filter(r=>r.openDt>=start&&r.openDt<returnDate):[]);
 const first=summary(returnDate?rows.filter(r=>r.openDt===returnDate):[]);
 const post=summary(returnDate?rows.filter(r=>r.openDt>=returnDate):[]);
 const eventAnnual={year:eventYear,...summary(rows.filter(r=>Number(r.openDt.slice(0,4))===eventYear))};
 const returnAnnual={year:returnYear,...summary(returnYear?rows.filter(r=>Number(r.openDt.slice(0,4))===returnYear):[])};
 const years=[];for(let y=eventYear-3;y<=Number(cutoff.slice(0,4));y++){const s=summary(rows.filter(r=>Number(r.openDt.slice(0,4))===y));years.push({year:y,...s,partial:y===Number(cutoff.slice(0,4))&&!cutoff.endsWith('1231'),event:y===eventYear,return:y===returnYear});}
 return {pre,interim,inclusiveInterim,first,post,eventAnnual,returnAnnual,annualChange:change(returnAnnual.sum,eventAnnual.sum),toPre:change(first.mean,pre.mean),toInterim:change(first.mean,interim.mean),postToPre:change(post.mean,pre.mean),postToInterim:change(post.mean,interim.mean),interimToPre:change(interim.mean,pre.mean),years,window:{start,returnDate,cutoff,interimRule:'strictly_after_public_month_before_first_return'},postFollowupDays:returnDate?Math.round((Date.parse(cutoff.replace(/(\d{4})(\d{2})(\d{2})/,'$1-$2-$3'))-Date.parse(returnDate.replace(/(\d{4})(\d{2})(\d{2})/,'$1-$2-$3')))/86400000):null};
}
function pattern({preHawk,pause,returned,synergy,comparison:c}){
 const gates={preHawk,pause,returned,highSynergy:synergy,decline:c.interimToPre.ratio===null?null:c.interimToPre.ratio<1,recoveryPre:c.toPre.ratio===null?null:c.toPre.ratio>1,recoveryInterim:c.toInterim.ratio===null?null:c.toInterim.ratio>1};
 const values=Object.values(gates),status=values.some(x=>x===false)?'not_observed':values.some(x=>x===null||x===undefined)?'unknown':'observed';return {status,gates};
}
return Object.freeze({summary,change,fraction,compare,pattern});
});
