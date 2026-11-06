'use strict';
let model;
const dataURL = name => new URL('../data/' + name, self.location.href);
async function readJSON(name) {
  const response = await fetch(dataURL(name));
  if (!response.ok) throw new Error('HTTP ' + response.status);
  return response.json();
}
self.onmessage = async ({data}) => {
  try {
    if (data.type === 'init') {
      importScripts('exp1-model.js');
      const [index, registry] = await Promise.all([
        readJSON('search_index.json'),
        readJSON('research/recovery_followup_registry.json').catch(() => null)
      ]);
      const registryAvailable = Array.isArray(registry?.events);
      self.postMessage({type:'progress', message:'전체 배우의 스타 파워와 변곡점을 계산하고 있습니다…'});
      model = self.Exp1Model.build(index, registryAvailable ? registry : {events:[]});
      self.postMessage({type:'ready', roster:model.roster, summary:model.summary, methods:model.methods,
        unlinkedCredits:model.unlinkedCredits || [], registryAvailable});
    } else if (data.type === 'actor' && model) {
      self.postMessage({type:'actor', request:data.request, actor:model.actor(String(data.id))});
    }
  } catch (err) {
    self.postMessage({type:'error', request:data.request, message:String(err?.message || err)});
  }
};
