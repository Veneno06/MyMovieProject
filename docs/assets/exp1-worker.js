'use strict';
let model;
let index;
let registry;
let registryAvailable = false;

const dataURL = name => new URL('../data/' + name, self.location.href);
async function readJSON(name) {
  const response = await fetch(dataURL(name));
  if (!response.ok) throw new Error('HTTP ' + response.status);
  return response.json();
}

// GitHub Actions updates search_index.json. Experiment 1 uses a separate, byte-verified copy.
const cohortFile = 'research/exp1-search-index-20261109.json';
async function readCohort() {
  const response = await fetch(dataURL(cohortFile));
  if (!response.ok) throw new Error('고정 자료 docs/data/' + cohortFile + ' 파일을 확인해 주세요.');
  const bytes = await response.arrayBuffer();
  const digest = await crypto.subtle.digest('SHA-256', bytes);
  const hex = Array.from(new Uint8Array(digest), byte => byte.toString(16).padStart(2, '0')).join('');
  if (hex !== '1983ffdc7c40335b029e6b1fbdc78b52a34411dd3fedb98ff2dc94885ea05b7d') throw new Error('고정 자료가 11월 9일 첨부본과 다릅니다. ZIP의 원본 search_index.json을 다시 복사해 주세요.');
  return JSON.parse(new TextDecoder().decode(bytes));
}

function sendReady(request) {
  self.postMessage({
    type: 'ready', request, roster: model.roster, summary: model.summary,
    methods: model.methods, unlinkedCredits: model.unlinkedCredits || [], registryAvailable
  });
}

self.onmessage = async ({data}) => {
  try {
    if (data.type === 'init') {
      importScripts('exp1-model.js?v=20261109-signed-r3');
      [index, registry] = await Promise.all([
        readCohort(),
        readJSON('research/recovery_followup_registry.json').catch(() => null)
      ]);
      registryAvailable = Array.isArray(registry?.events);
      if (!registryAvailable) registry = {events: []};
      self.postMessage({type: 'progress', message: '전체 배우의 스타 파워와 급변점을 계산하고 있습니다…'});
      model = self.Exp1Model.build(index, registry, {cutoffDate:'2026-11-04'});
      sendReady(data.request);
    } else if (data.type === 'calculate') {
      if (!model) throw new Error('자료를 먼저 불러와 주세요.');
      // 같은 자료와 마감일에서 기준만 바꿉니다. 계산 성공 후 모델을 교체합니다.
      const options = {...model.methods.config,
        thresholdMode: data.options.thresholdMode,
        percentile: data.options.percentile,
        absoluteThreshold: data.options.absoluteThreshold};
      const nextModel = self.Exp1Model.build(index, registry, options);
      model = nextModel;
      sendReady(data.request);
    } else if (data.type === 'actor' && model) {
      self.postMessage({type: 'actor', request: data.request, actor: model.actor(String(data.id))});
    }
  } catch (err) {
    self.postMessage({type: 'error', operation: data.type, request: data.request,
      message: String(err?.message || err)});
  }
};
