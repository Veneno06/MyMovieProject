'use strict';
const assert=require('node:assert/strict');
const S=require('../docs/assets/starpower-core.js');
const movies=[
 {movieCd:'1',movieNm:'before',openDt:'20190101',nation:'한국',audiAcc:100,actors:[{id:'A',name:'A'},{id:'B',name:'B'}]},
 {movieCd:'2',movieNm:'alone',openDt:'20190201',nation:'한국',audiAcc:50,actors:[{id:'B',name:'B'}]},
 {movieCd:'3',movieNm:'future',openDt:'20210101',nation:'한국',audiAcc:10000,actors:[{id:'A',name:'A'},{id:'B',name:'B'}]}
];
const past=S.buildModel(movies,{cutoffDate:'20191231'});
assert.equal(past.eligibleMovies.length,2,'event cutoff must exclude future movies from whole model');
assert.equal(past.config.cutoffDate,'20191231');
assert.deepEqual(past.observations,S.buildModel(movies.slice(0,2),{cutoffDate:'20191231'}).observations,'future film cannot affect past-only model');
assert.equal(S.CONFIG.cutoffDate,'20260903','options must not mutate global default');
const syn=S.getDirectedSynergy(past,'A','B');
assert.equal(syn.withCount,1);assert.equal(syn.withoutCount,1);assert.ok(Number.isFinite(syn.value));
assert.equal(S.getDirectedSynergy(past,'B','A').value,null,'missing denominator must remain N/A');
assert.throws(()=>S.buildModel(movies,{lambda:-1}),/lambda/);
console.log('SP temporal and directional regression tests passed');
