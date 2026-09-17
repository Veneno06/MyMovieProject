(function(root,factory){const api=factory();if(typeof module==='object'&&module.exports)module.exports=api;if(root)root.SentimentClient=api;})(typeof globalThis!=='undefined'?globalThis:this,function(){
  'use strict';
  function idsForName(name,movies){
    const ids=new Set();
    for(const movie of movies||[])for(const actor of movie.actors||[])if(actor.name===name&&actor.id)ids.add(String(actor.id));
    return ids;
  }
  function resolveId(name,id,movies){
    if(id&&!String(id).startsWith('name:'))return String(id).replace(/^id:/,'');
    const ids=idsForName(name,movies);return ids.size===1?[...ids][0]:'';
  }
  function compatible(data,name,id,movies){
    if(!data||!data.timeline||!id)return false;
    if(data.date_basis&&!['comment_published_at','comment_publishedAt'].includes(data.date_basis))return false;
    if(Object.keys(data.timeline).some(k=>!/^\d{4}-W\d{2}$/.test(k)))return false;
    const stored=String(data.actor_id||data.peopleCd||'');
    if(stored)return stored===id;
    const ids=idsForName(name,movies);
    return data.actor_name===name&&ids.size===1&&ids.has(id);
  }
  async function fetchActor(name,actorId,movies){
    const id=resolveId(name,actorId,movies);if(!id)return null;
    for(const file of [`${id}.json`,`${name}.json`])for(const prefix of ['data/sentiment/','docs/data/sentiment/']){
      try{const response=await fetch(prefix+encodeURIComponent(file)+'?v='+Date.now(),{cache:'no-store'});
        if(!response.ok)continue;const data=await response.json();
        if(compatible(data,name,id,movies))return data;
      }catch(error){/* Try the next supported path. */}
    }
    return null;
  }
  return Object.freeze({resolveId,compatible,fetchActor});
});
