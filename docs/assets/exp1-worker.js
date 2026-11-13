'use strict';
let model;
let index;
let registry;
let registryAvailable = false;

const dataURL = name => new URL('../data/' + name, self.location.href);
async function readJSON(name) {
  const response = await fetch(dataURL(name), {cache:'no-cache'});
  if (!response.ok) throw new Error('HTTP ' + response.status);
  return response.json();
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
      importScripts('exp1-model.js?v=20261113-metric-r1');
      [index, registry] = await Promise.all([
        readJSON('search_index.json'),
        readJSON('research/recovery_followup_registry.json').catch(() => null)
      ]);
      registryAvailable = Array.isArray(registry?.events);
      if (!registryAvailable) registry = {events: []};
      self.postMessage({type: 'progress', message: '전체 배우의 지표와 변곡점을 계산중…'});
      model = self.Exp1Model.build(index, registry);
      sendReady(data.request);
    } else if (data.type === 'calculate') {
      if (!model) throw new Error('자료를 먼저 불러와 주세요.');
      // 같은 자료와 마감일에서 기준만 바꿉니다. 계산 성공 후 모델을 교체합니다.
      const options = {...model.methods.config,
        metric: data.options.metric ?? model.methods.config.metric,
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
