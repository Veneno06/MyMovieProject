(() => {
  'use strict';
  const $ = id => document.getElementById(id);
  const finite = v => typeof v === 'number' && Number.isFinite(v);
  const number = (v, digits=2) => finite(v) ? v.toLocaleString('ko-KR',{maximumFractionDigits:digits}) : '—';
  const signed = v => finite(v) ? (v>0 ? '+' : '') + number(v,3) : '—';
  const fmtSP = v => finite(v) ? number(v,6) : '산출 불가';
  const exact = v => finite(v) ? String(v) : '산출 불가';
  const normalize = s => String(s ?? '').normalize('NFKC').toLocaleLowerCase().replace(/\s+/g,'');
  const count = n => number(n,0);
  function h(tag,attrs={},...children) {
    const node=document.createElement(tag);
    Object.entries(attrs).forEach(([k,v])=>{
      if(v===null||v===undefined||v===false)return;
      if(k==='class')node.className=v;
      else if(k.startsWith('on')&&typeof v==='function')node.addEventListener(k.slice(2),v);
      else node.setAttribute(k,String(v));
    });
    children.flat(Infinity).forEach(c=>{if(c!==undefined&&c!==null&&c!==false)node.append(c);});
    return node;
  }
  const p = (s,c='help') => h('p',{class:c},s);
  const btn = (s,fn,attrs={}) => h('button',{type:'button',class:'btn',onclick:fn,...attrs},s);
  const detail = (s,...kids) => h('details',{class:'support'},h('summary',{},s),...kids);
  function table(headers,rows,label) {
    return h('div',{class:'scroller',tabindex:'0','aria-label':label},h('table',{'aria-label':label},
      h('thead',{},h('tr',{},headers.map(x=>h('th',{scope:'col'},x)))),
      h('tbody',{},rows.map(cells=>h('tr',{},cells.map(x=>h('td',{},x)))))));
  }
  const typedDate = d => /^\d{4}-\d{2}-\d{2}$/.test(d||'') ? Date.parse(d+'T00:00:00Z') : NaN;
  const classOf = delta => delta>0 ? 'up' : 'down';
  const direction = delta => delta>0 ? '기울기 증가' : '기울기 감소';
  const status = (str) => { $('load-status').textContent=str; $('load-status').hidden=!str; };
  const storeKey='exp1-temporary-candidates-v1';
  let shortlist=new Set();
  try { const a=JSON.parse(sessionStorage.getItem(storeKey)||'[]'); if(Array.isArray(a))shortlist=new Set(a.filter(i=>typeof i==='string')); } catch (_) {}
  function persistList(){try {sessionStorage.setItem(storeKey,JSON.stringify([...shortlist]));}catch(_){}}
  let worker,roster=[],byId=new Map(),unlinked=[],summary=null,registryAvailable=false;
  let isExample=false,selectedId=null,current=null,sequence=0,desiredDate='',openResults=false,resultPage=0,actorRows=[],disposeChart=()=>{};
  let appliedConfig=null, previousResult=null, calculationRequest=null;
  let calculating=false, resultsWereOpen=false;
  const pageSize=6;
  function toggleResults(open){
    openResults=open;
    $('actor-results').hidden=!open;
    $('results-toggle').setAttribute('aria-expanded',String(open));
    $('results-toggle').textContent=open?'배우 목록 닫기':'배우 목록 보기';
    if(open)renderRows();
  }
  function syncListLabel(){
    $('actor-mode').querySelector('option[value="list"]').textContent='임시 후보 목록 ('+count(shortlist.size)+')';
  }
  function findRows(){
    const q=normalize($('actor-query').value), mode=$('actor-mode').value;
    const matches=x=>!q||normalize(x.name).includes(q)||normalize(x.id).includes(q);
    if(mode==='rise'||mode==='fall'){
      return (summary?.candidates?.[mode]||[]).filter(matches).map(c=>({record:byId.get(String(c.id)),date:c.date,change:c.change})).filter(x=>x.record);
    }
    const list=mode==='list' ? [...shortlist].map(id=>byId.get(id)).filter(Boolean) : roster;
    const rows=list.filter(matches).map(record=>({record}));
    if(q&&mode==='all')unlinked.forEach(record=>{if(normalize(record.name).includes(q))rows.push({record,unlinked:true});});
    return rows;
  }
  function updateRows(){resultPage=0;actorRows=findRows();toggleResults(true);}
  function renderRows(){
    const box=$('results-list'),pager=$('results-pages'),mode=$('actor-mode').value;
    const pages=Math.ceil(actorRows.length/pageSize);
    resultPage=Math.max(0,Math.min(resultPage,pages-1));
    const start=resultPage*pageSize, end=Math.min(start+pageSize,actorRows.length);
    const unit=mode==='all'?'건':'명';
    const note=mode==='rise'||mode==='fall' ? ' · 배우별 해당 방향의 첫 변곡일 순' : mode==='list'?' · 이 탭에서만 보관':'';
    $('results-count').textContent=actorRows.length?'검색 결과 '+count(actorRows.length)+unit+' · '+count(start+1)+'–'+count(end)+note:
      (mode==='list'&&!shortlist.size?'아직 임시 후보가 없습니다. 배우를 연 뒤 ‘임시 후보에 추가’를 누르세요.':'조건에 맞는 배우 또는 출연 기록이 없습니다.');
    box.replaceChildren();pager.replaceChildren();
    actorRows.slice(start,end).forEach(item=>{
      const a=item.record;
      const sub=item.unlinked ? 'ID 미기재 · '+(a.title||'제목 미상')+' · '+(a.date||'개봉일 미상') :
        'ID '+a.id+' · '+ (item.date?item.date+' · '+signed(item.change):'국내 '+count(a.filmCount)+'편 · SP '+count(a.scoredCount)+'편');
      box.append(btn('',()=>{item.unlinked?showUnlinked(a):choose(a.id,item.date);},{
        class:'btn result','aria-current':!item.unlinked&&String(a.id)===selectedId?'true':null
      }));
      box.lastChild.append(h('strong',{},a.name||'이름 미기재'),h('span',{class:'meta'},sub));
    });
    if(pages>1)pager.append(
      btn('이전',()=>{resultPage--;renderRows();},{disabled:resultPage===0}),
      h('span',{class:'meta'},count(resultPage+1)+' / '+count(pages)),
      btn('다음',()=>{resultPage++;renderRows();},{disabled:resultPage>=pages-1}));
  }
  function choose(id,date,close=true,example=false){
    id=String(id);
    if(calculating||!byId.has(id))return;
    if(close)toggleResults(false);
    selectedId=id;desiredDate=date||'';isExample=example;
    if(current?.id===id){renderActor(current);return;}
    current=null;disposeChart();$('actor-panel').hidden=true;
    status((byId.get(id).name||'선택한 배우')+'의 출연 기록을 불러오는 중…');
    worker.postMessage({type:'actor',request:++sequence,id});
  }
  function showUnlinked(a){
    ++sequence; selectedId=null;current=null;disposeChart();status('');toggleResults(false);
    const panel=$('actor-panel');panel.hidden=false;
    panel.replaceChildren(h('h2',{},(a.name||'이름 미기재')+' · ID 없는 출연 기록'),
      p('배우 ID가 없어 다른 작품의 동명 인물과 연결하지 않았습니다. 이 기록만으로는 개인 시계열·변곡점·재출연을 판정할 수 없습니다.'),
      h('p',{},filmName({id:a.filmId,title:a.title}),' · '+(a.date||'개봉일 미상')));
  }
  function filmName(f) {
    const name=f.title||'제목 미상';
    return /^\d+$/.test(String(f.id||''))?h('a',{href:'detail.html?movieCd='+encodeURIComponent(f.id)},name):h('span',{},name);
  }
  function makeListButton(a){
    const refresh=el=>{const on=shortlist.has(String(a.id));el.textContent=on?'임시 후보에서 빼기':'임시 후보에 추가';el.setAttribute('aria-pressed',String(on));};
    const b=btn('',()=>{const id=String(a.id);if(shortlist.has(id))shortlist.delete(id);else shortlist.add(id);
      persistList();syncListLabel();refresh(b);if($('actor-mode').value==='list'){actorRows=findRows();if(openResults)renderRows();}
    },{class:'btn small'});refresh(b);return b;
  }
  function renderActor(a){
    disposeChart();disposeChart=()=>{};status('');
    const panel=$('actor-panel');panel.hidden=false;
    panel.replaceChildren(h('div',{class:'panel-title'},h('div',{},h('h2',{},a.name||'이름 미기재'),
      p((isExample?'기본 예시 · ':'')+'배우 ID '+a.id+' · 국내 영화 '+count(a.filmCount)+'편 · SP 산출 '+count(a.scoredCount)+'편 · 변곡점 '+count(a.pointCount)+'곳','meta')), makeListButton(a)));
    const days=a.days||[], pts=a.points||[], valid=days.filter(d=>finite(d.sp)&&finite(typedDate(d.date)));
    const idx=Math.max(0,pts.findIndex(x=>x.date===desiredDate));
    let selectedPoint=pts[idx]||null;
    const figure=h('figure',{class:'chart-box'});
    figure.append(h('div',{class:'subhead'},h('h3',{},'개봉일별 스타 파워'),h('div',{class:'chart-key'},
      h('span',{class:'legend-sp'},'SP'),h('span',{class:'legend-rise'},'기울기 증가'),h('span',{class:'legend-fall'},'기울기 감소'))));
    panel.append(figure);
    const changeArea=h('section',{class:'section','aria-label':'변곡점과 재출연'}), eventArea=h('section',{class:'section','aria-label':'사건 자료와 관측 시점'});
    const setPoint=(d,movePlot=false)=>{
      const found=pts.find(x=>x.date===d);if(!found)return;
      desiredDate=d;selectedPoint=found;renderPoint(a,found,changeArea,date=>{setPoint(date,true);});renderEventArea(a,found,eventArea);
      if(movePlot)chart?.select(d);
    };
    let chart=null;
    if(valid.length){chart=mountChart(figure,days,pts,selectedPoint?.date,d=>{if(d!==selectedPoint?.date)setPoint(d);}); disposeChart=chart.dispose;}
    else figure.append(p('SP를 계산할 수 있는 개봉일이 없습니다. 아래 출연 기록에서 확인할 수 있습니다.','empty'));
    const filmDetails=detail('작품별 점수와 출연 기록 ('+count(a.filmCount)+'편)');
    filmDetails.className='observations';
    filmDetails.addEventListener('toggle',()=>{
      if(!filmDetails.open||filmDetails.childElementCount>1)return;
      const rows=[];
      for(const day of [...days,{date:null,films:a.undated||[]}]){
        for(const f of day.films||[])rows.push([
          day.date&&finite(day.sp)&&chart?btn(day.date,()=>{chart.select(day.date);setPoint(day.date);},{class:'text-btn'}):(day.date||'개봉일 미상'),
          filmName(f),h('span',{title:exact(f.sp)},fmtSP(f.sp)), day.date?h('span',{title:exact(day.sp)},fmtSP(day.sp)):'—'
        ]);
      }
      filmDetails.append(rows.length?table(['개봉일','작품','작품 SP','날짜 평균 SP'],rows,'배우의 작품별 점수'):p('등록된 작품이 없습니다.'));
    });
    panel.append(filmDetails,changeArea,eventArea);
    if(selectedPoint)setPoint(selectedPoint.date);else{
      changeArea.replaceChildren(h('h3',{},'변곡점과 재출연'),p(valid.length<3?'점수가 있는 서로 다른 개봉일이 세 번 미만이어서 두 기울기를 비교할 수 없습니다.':'이 배우에게 전체 기준을 충족하는 기울기 변화가 없습니다.'));
      renderEventArea(a,null,eventArea);
    }
  }
  function mountChart(figure,days,points,initial,onPick) {
    const ns='http://www.w3.org/2000/svg';
    function s(tag,attrs,text){const n=document.createElementNS(ns,tag);Object.entries(attrs||{}).forEach(([k,v])=>n.setAttribute(k,v));if(text!=null)n.textContent=text;return n;}
    const all=days.filter(d=>finite(typedDate(d.date)));
    const values=all.filter(d=>finite(d.sp)), flagged=new Map(points.map(p=>[p.date,p]));
    let selected=Math.max(0,values.findIndex(d=>d.date===initial)),shown=selected,positions=[],w=0,raf=0,observer,marker;
    const plot=h('div',{class:'plot'}),svg=s('svg',{tabindex:'0',role:'group','aria-label':'개봉일별 스타 파워 꺾은선 그래프. 좌우 방향키로 점을 이동하고 Enter로 변곡점을 선택합니다.'});
    const output=h('div',{class:'chart-focus'});plot.append(svg);figure.append(plot,output,
      h('figcaption',{},'점은 같은 개봉일의 유효 작품 평균이며, SP가 없는 개봉일에서는 선이 끊깁니다. 점을 누르거나 방향키로 이동하면 작품명과 점수를 볼 수 있습니다.'));
    function display(i){
      shown=i;const d=values[i],pos=positions[i];if(!d||!pos)return;
      marker.setAttribute('cx',pos.x);marker.setAttribute('cy',pos.y);
      const sp=h('strong',{title:exact(d.sp)},'SP '+fmtSP(d.sp));
      output.replaceChildren(h('strong',{},d.date),' · ',sp,' · '+(d.films||[]).map(f=>f.title||'제목 미상').join(' / '));
      if(flagged.has(d.date)){const f=flagged.get(d.date);output.append(' · ',h('span',{class:'tag '+classOf(f.change)},direction(f.change)));}
    }
    function select(date){const idx=values.findIndex(x=>x.date===date);if(idx>=0){selected=idx;display(idx);}}
    function draw() {
      const width=Math.max(235,Math.floor(plot.getBoundingClientRect().width||640));
      if(width===w&&positions.length)return;
      w=width;const ht=width<470?236:276,left=width<470?43:57,right=17,top=26,bottom=39,ph=ht-top-bottom,pw=w-left-right;
      svg.setAttribute('viewBox','0 0 '+w+' '+ht);svg.replaceChildren();
      let minT=typedDate(all[0].date),maxT=typedDate(all[all.length-1].date),maxV=Math.max(...values.map(v=>v.sp),0);
      if(maxT===minT){minT-=86400000;maxT+=86400000;}
      const order=Math.pow(10,Math.floor(Math.log10((maxV||1)/4))),step=[1,2,2.5,5,10].map(k=>k*order).find(k=>k>=(maxV||1)/4)||order*10;
      const maxY=Math.ceil((maxV||1)/step)*step;
      const x=t=>left+(t-minT)/(maxT-minT)*pw,y=v=>top+ph-(v/maxY)*ph;
      svg.append(s('text',{x:left,y:14},'SP'));
      for(let i=0;i<=Math.round(maxY/step);i++){
        const val=step*i,yp=y(val);svg.append(s('line',{x1:left,y1:yp,x2:w-right,y2:yp,stroke:'#e3e7ed'}),
          s('text',{x:left-6,y:yp+4,'text-anchor':'end'},val>=100000?val.toExponential(0):val>0&&val<.01?val.toExponential(0):number(val,3)));
      }
      const countX=all.length===1?1:w<420?2:4;
      for(let i=0;i<countX;i++){
        const t=countX===1?typedDate(all[0].date):minT+(maxT-minT)*i/(countX-1),dx=x(t);
        const iso=new Date(t).toISOString().slice(0,10),
          lab=maxT-minT<200*86400000?iso.slice(5):iso.slice(0,7);
        svg.append(s('line',{x1:dx,y1:top+ph,x2:dx,y2:top+ph+4,stroke:'#a5adba'}),
          s('text',{x:dx,y:top+ph+18,'text-anchor':countX===1?'middle':i===0?'start':i===countX-1?'end':'middle'},lab));
      }
      svg.append(s('text',{x:left+pw/2,y:ht-2,'text-anchor':'middle'},'개봉일'));
      let path='',blocked=true;
      positions=[];
      all.forEach(d=>{
        if(!finite(d.sp)){blocked=true;return;}
        const px=x(typedDate(d.date)),py=y(d.sp);
        positions.push({x:px,y:py});path+=(blocked?'M':'L')+px.toFixed(2)+' '+py.toFixed(2)+' ';blocked=false;
      });
      svg.append(s('path',{d:path,stroke:'#245bc5','stroke-width':2,fill:'none','stroke-linejoin':'round'}));
      values.forEach((d,i)=>{
        const {x:px,y:py}=positions[i],p=flagged.get(d.date);
        if(!p)svg.append(s('circle',{cx:px,cy:py,r:3.3,fill:'#245bc5'}));
        else {
          const sign=p.change>0?1:-1;
          svg.append(s('path',{d:['M',px,py-6*sign,'L',px+5.5,py+4.5*sign,px-5.5,py+4.5*sign,'Z'].join(' '),fill:sign>0?'#006b67':'#a5481d',stroke:'white','stroke-width':1}));
        }
      });
      marker=s('circle',{r:8,fill:'none',stroke:'#263445','stroke-width':1.4,'pointer-events':'none'});svg.append(marker);display(shown);
    }
    function nearest(e){
      const rect=svg.getBoundingClientRect(),v=(e.clientX-rect.left)*w/(rect.width||1);
      let idx=0;for(let i=1;i<positions.length;i++)if(Math.abs(positions[i].x-v)<Math.abs(positions[idx].x-v))idx=i;
      return idx;
    }
    svg.addEventListener('pointermove',e=>{if(e.pointerType!=='touch')display(nearest(e));});
    svg.addEventListener('pointerleave',()=>display(selected));
    svg.addEventListener('click',e=>{selected=nearest(e);display(selected);onPick(values[selected].date);});
    svg.addEventListener('keydown',e=>{
      if(!['ArrowLeft','ArrowRight','Home','End','Enter',' '].includes(e.key))return;
      e.preventDefault();
      if(e.key==='ArrowLeft')selected=Math.max(0,selected-1);
      if(e.key==='ArrowRight')selected=Math.min(values.length-1,selected+1);
      if(e.key==='Home')selected=0;
      if(e.key==='End')selected=values.length-1;
      display(selected);if(e.key==='Enter'||e.key===' ')onPick(values[selected].date);
    });
    draw();const resize=()=>{cancelAnimationFrame(raf);raf=requestAnimationFrame(draw);};
    if('ResizeObserver' in window){observer=new ResizeObserver(resize);observer.observe(plot);}else window.addEventListener('resize',resize);
    return {select,dispose:()=>{observer?.disconnect();window.removeEventListener('resize',resize);cancelAnimationFrame(raf);}};
  }
  const resultText={match:'재출연 확인',no_match:'재출연 없음',no_positive:'양의 고시너지 동료 없음',no_pool:'계산 가능한 과거 동료 없음',no_target:'후속 개봉작 미관측',incomplete_cast:'출연진 식별 자료 부족'};
  function comparison(c,a){
    if(!c)return [p('판정 자료 없음')];
    const names=(c.matches||[]).map(m=>m.name||m.id).join(', ');
    const res=h('span',{class:'outcome '+(c.status==='hit'?'hit':'')},c.status==='hit'?'재출연 확인':c.status==='miss'?'재출연 없음':'판정 불가');
    const films=(c.films||[]).map(f=>{
      const partners=(f.cast||[]).filter(x=>!x.id||String(x.id)!==String(a.id));
      const seen=new Set((c.matches||[]).map(m=>String(m.id)));
      const parts=partners.flatMap((person,i)=>[i?', ':'',seen.has(String(person.id))?h('strong',{},person.name||person.id):(person.name||person.id||'이름 미기재')]);
      const cast=detail('공동출연진 '+count(partners.length)+'명',h('div',{class:'meta'},parts.length?parts:'다른 출연진 기록 없음'));
      return h('div',{class:'film-entry'},filmName(f),h('span',{class:'meta',title:exact(f.sp)},'작품 SP '+fmtSP(f.sp)),cast);
    });
    return [h('div',{},c.date||'개봉일 미관측'),h('div',{},films.length?films:'—'),h('div',{},res,
      names?h('div',{class:'meta'},names):c.reason&&c.reason!=='no_match'?h('div',{class:'meta'},resultText[c.reason]||'판정 자료 부족'):null)];
  }
  function renderPoint(a,point,area,switchTo){
    const sel=h('select',{class:'point-tabs','aria-label':'살펴볼 변곡점',onchange:e=>switchTo(e.target.value)},a.points.map(p=>h('option',{value:p.date},p.date+' · '+direction(p.change))));
    sel.value=point.date;
    const follow=(point.later?.length?point.later:[point.next]).filter(Boolean), futureTable=h('div',{});
    const datePicker=h('select',{'aria-label':'비교할 후속 개봉일'});
    for(const [i,day] of follow.entries())datePicker.append(h('option',{value:String(i)},(i+1)+'번째 · '+(day.date||'미관측')));
    function paintFollow(index){
      const observed=follow[index]||point.next,which=index===0?'바로 다음 개봉일':(index+1)+'번째 후속 개봉일';
      futureTable.replaceChildren(table(['관측 시점','개봉일','작품과 공동출연진','과거 고시너지 동료'],[
        ['변곡점',...comparison(point.pivot,a)],[which,...comparison(observed,a)]
      ],'변곡일과 선택한 후속 개봉일의 재출연'));
    }
    datePicker.addEventListener('change',()=>paintFollow(Number(datePicker.value)));paintFollow(0);
    area.replaceChildren(h('div',{},h('div',{class:'subhead'},h('h3',{},'변곡점과 재출연'),sel),
      h('p',{class:'point-summary'},h('strong',{class:classOf(point.change)},direction(point.change)+' '+signed(point.change)),' SP/년 · 앞 기울기 '+signed(point.beforeSlope)+' → 뒤 기울기 '+signed(point.afterSlope)),
      p('세 측정일: '+point.prevDate+' (SP '+fmtSP(point.prevSp)+') → '+point.date+' (SP '+fmtSP(point.sp)+', 변곡일) → '+point.detectedDate+' (SP '+fmtSP(point.nextSp)+', 변화 확인일).','point-info'),
      (point.skippedBefore||point.skippedAfter)?p('세 측정일 사이에 SP를 산출할 수 없는 개봉일이 '+count((point.skippedBefore||0)+(point.skippedAfter||0))+'회 있어 기울기 계산에서는 건너뛰었습니다.'):null,
      follow.length>1?h('label',{class:'follow-choice'},'후속 개봉일 ',datePicker):null,futureTable,
      p('모든 후속 개봉일은 변곡일 전날까지의 같은 고시너지 동료 집합과 비교합니다. 전체 집계에는 SP 산출 여부와 관계없이 바로 다음 개봉일만 사용합니다.')));
    const pool=detail('변곡일 이전 동료: 계산 가능 '+count(point.poolSize)+'명 · 고시너지 '+count(point.highCount)+'명');
    const high=point.high||[];
    pool.addEventListener('toggle',()=>{
      if(!pool.open||pool.childElementCount>1)return;
      pool.append(p('중심 배우 A와 함께 출연했을 때 동료 B의 평균 SP가 A 없이 출연했을 때보다 얼마나 달랐는지 비교합니다. 함께 / 따로는 변곡일 이전 B의 작품 수입니다.'),
        high.length?table(['동료 B','배우 ID','시너지 A→B (%)','함께 / 따로 (편)'],high.map(m=>[m.name||'이름 미기재',m.id,signed(m.value),count(m.withCount)+' / '+count(m.withoutCount)]),'과거 고시너지 동료'):p(point.poolSize?'양의 고시너지 조건에 해당하는 동료가 없습니다.':'이전 출연 기록으로 시너지를 계산할 수 있는 동료가 없습니다.'));
    });
    area.append(pool);
  }
  function safeSource(source) {
    try {
      const u=new URL(String(source?.url||''));
      if(!['https:','http:'].includes(u.protocol)||!u.hostname||u.username||u.password)return null;
      return {url:u.href,title:String(source.title||u.hostname)};
    }catch(_){return null;}
  }
  function uniqueSources(items){
    const seen=new Set();return (items||[]).map(safeSource).filter(x=>{if(!x||seen.has(x.url))return false;seen.add(x.url);return true;});
  }
  function eventCard(e) {
    const evidence=uniqueSources(e.sources),pause=uniqueSources(e.pauseEvidence);
    const mapped={documented:'활동 공백 근거 수록',unresolved:'활동 공백 근거 미확인',not_supported:'활동 공백을 지지하는 근거 미확인'};
    const sources=uniqueSources([...evidence,...pause]).map(x=>h('li',{},h('a',{href:x.url,target:'_blank',rel:'noopener noreferrer'},x.title)));
    return h('div',{class:'event'},h('p',{},h('strong',{},(e.month||'월 미상')+' · '+(e.label||'공개 기록'))),
      p((evidence.length?'조사 대장에 출처 수록':'공개 출처 링크 미확인')+' · '+(mapped[e.pauseStatus]||'활동 공백 판정 미확인'),'meta'),
      e.reviewNote?p(e.reviewNote,'meta'):null,sources.length?h('ul',{class:'source-list'},sources):null);
  }
  function renderEventArea(a,point,area){
    area.replaceChildren(h('h3',{},'곡선과 대조할 사건 자료'));
    if(!registryAvailable){area.append(p('기존 조사 대장을 불러오지 못해 이 배우의 공개 기록을 대조할 수 없습니다.'));return;}
    const all=a.events||[],linked=point?.events||[];
    if(point){
      area.append(p('확인 구간: '+point.prevDate+' – '+point.detectedDate+' · 사건은 기존 조사 대장의 공개 월을 기준으로 비교합니다.'));
      if(linked.length)area.append(...linked.map(eventCard));
      else area.append(p('이 구간과 월이 겹치는 사건 기록은 대장에서 확인되지 않았습니다. 사건이 없었다는 뜻은 아닙니다.','point-info'));
      const other=all.filter(e=>!linked.some(x=>x.month===e.month&&x.label===e.label));
      if(other.length)area.append(detail('구간 밖 이 배우의 기존 기록 '+count(other.length)+'건',other.map(eventCard)));
    }else if(all.length)area.append(p('대장에 수록된 배우 기록입니다. 비교할 변곡점은 없습니다.'),...all.map(eventCard));
    else area.append(p('이 배우는 기존 조사 대장에서 확인되지 않았습니다. 사건의 유무는 알 수 없습니다.'));
    if(all.length)area.append(p('공개 월의 겹침은 사건이 SP 변화를 일으켰다는 증거가 아닙니다. 의혹·보도는 확정 판결과 구별해야 합니다.'));
  }
  function rate(c){return c.evaluable?h('strong',{},number(100*c.hits/c.evaluable,1)+'%'):'산출 불가';}
  function rateTable(comps,name){
    const rows=[];
    for(const [unit,unitLabel] of [['actors','배우'],['points','변곡점']]){
      for(const [target,targetLabel] of [['pivot','변곡점 개봉일'],['next','바로 다음 개봉일']]){
        const c=comps[target][unit];
        rows.push([unitLabel,targetLabel,rate(c),count(c.hits)+' / '+count(c.evaluable),count(c.unknown),count(c.total)]);
      }
    }
    return table(['집계 단위','작품 시점','재출연율','확인 / 판정 가능','판정 불가','전체'],rows,name);
  }
  function renderSummary(methods){
    const c=summary.coverage||{},e=summary.comparisons;
    $('cohort').hidden=false;
    const scope=p('국내 영화 출연 배우 '+count(summary.actors)+'명 중 SP 산출 '+count(summary.scoredActors)+'명, 세 개봉일 이상 비교 가능 '+count(summary.eligibleActors)+'명, 기준을 넘은 배우 '+count(summary.markedActors)+'명입니다. 이들에게서 변곡점 '+count(summary.points)+'곳을 찾았습니다.','point-summary');
    const body=$('cohort-body');
    body.replaceChildren(scope,rateTable(e,'전체 배우와 변곡점의 시점별 재출연율'),
      p('재출연율 = 확인 ÷ 판정 가능. 배우 단위에서는 한 번이라도 재출연하면 확인, 모든 변곡점에서 불일치하면 없음, 나머지는 판정 불가입니다. 변곡점 단위에서는 각 날짜를 한 번씩 셉니다.'));
    if(summary.actors&&e){
      const allRatio=t=>count(e[t].actors.hits)+' / '+count(summary.actors)+'명 ('+number(100*e[t].actors.hits/summary.actors,2)+'%)';
      body.append(p('검색 대상 전체에서 재출연이 확인된 배우의 비중: 변곡점 개봉일 '+allRatio('pivot')+', 다음 개봉일 '+allRatio('next')+'. 변곡점이 없는 배우와 판정 불가도 포함한 분모입니다.'));
    }
    if(summary.byDirection){
      const d=detail('기울기 증가·감소를 나누어 보기');
      for(const [key,label] of [['rise','기울기 증가'],['fall','기울기 감소']]){
        const g=summary.byDirection[key]; if(g)d.append(h('h4',{},label+' · 배우 '+count(g.actors)+'명, 변곡점 '+count(g.points)+'곳'),rateTable(g.comparisons,label+' 재출연율'));
      }
      d.append(p('두 방향의 변곡점이 있는 배우는 각 방향 표에 모두 포함되므로 배우 수를 더하지 않습니다.'));body.append(d);
    }
    const m=methods.text||{};
    $('formula-preview').textContent=m.starPower ? '· '+m.starPower.split('. ')[0] : '';
    const method=$('method-body');
    const part=(title,keys)=>h('div',{},h('h4',{},title),keys.filter(k=>m[k]).map(k=>p(m[k],'point-info')));
    method.replaceChildren(
      part('대상과 자료 범위',['scope','snapshot']),
      p('이 자료의 국내 영화 개봉일 범위: '+(summary.firstDate||'미상')+' – '+(summary.lastDate||'미상')+'. 배우 ID가 있는 마지막 SP 산출일: '+(c.lastStableScoreDate||'미상')+'. SP가 없는 배우 '+count(c.unscoredActors)+'명도 검색에 남깁니다. ID 미기재 출연 기록 '+count(c.unlinkedCredits)+'건은 이름으로 검색하면 작품별로 표시합니다.'),
      part('작품별 SP와 날짜별 점수',['base','role','prior','history','starPower','dateMean']),
      part('큰 기울기 변화의 판정',['slope','change','threshold']),
      p('전체 비교 '+count(summary.triples)+'건의 |기울기 변화| 기준값: '+(finite(summary.cutoff)?fmtSP(summary.cutoff)+' SP/년':'계산 불가')+'. 짧은 개봉 간격은 연 단위 기울기를 크게 만들 수 있습니다. 이 기준은 통계적 유의성 검정이 아닙니다.'),
      part('과거 고시너지와 재출연',['synergy','pool','next','rate']),
      part('기존 조사 대장과 해석',['event']),
      p('이 분석은 작품별 성과의 동시 관측을 다룹니다. 개봉일은 촬영·캐스팅 결정일이 아니며, SP는 평판이나 사건의 영향을 직접 측정한 값이 아닙니다.'),
      h('p',{class:'help'},h('a',{href:'data/search_index.json'},'영화·출연 원자료'),' · ',
        h('a',{href:'data/research/recovery_followup_registry.json'},'기존 사건 조사 대장')));
  }
  function fail(message,retryAll){
    status('');const box=$('load-status');box.hidden=false;
    box.append(document.createTextNode(message+' '),btn('다시 시도',()=>retryAll?location.reload():choose(selectedId),{class:'btn small'}));
  }
  function criterionLabel(config) {
    return config.absoluteThreshold === null
      ? number(config.percentile * 100, 6) + '백분위'
      : '직접 입력 ' + number(config.absoluteThreshold, 6) + ' SP/년';
  }
  function comparisonRow(label, config, result) {
    const reunion = result.comparisons.next.points;
    return [label, criterionLabel(config),
      h('span', {title: exact(result.cutoff)}, fmtSP(result.cutoff)),
      count(result.markedActors), count(result.points),
      count(reunion.hits) + ' / ' + count(reunion.evaluable), rate(reunion)];
  }
  function showCriterionResult() {
    $('threshold-status').textContent = '현재 적용 ' + criterionLabel(appliedConfig)
      + ' · 실제 임계값 ' + fmtSP(summary.cutoff) + ' SP/년'
      + ' · 후보 ' + count(summary.markedActors) + '명 / ' + count(summary.points) + '곳';
    const box = $('threshold-comparison');
    box.hidden = !previousResult;
    if (!previousResult) return;
    box.replaceChildren(table(
      ['적용 시점', '판정 기준', '임계값 (SP/년)', '후보 배우', '급변점', '첫 후속 재출연 / 판정 가능', '재출연율'],
      [comparisonRow('직전 적용', previousResult.config, previousResult.summary),
       comparisonRow('현재 적용', appliedConfig, summary)], '급변점 기준 적용 전후 비교'),
      p('같은 자료에서 기준만 바꾼 결과입니다. 재출연율은 급변점 단위로 첫 후속 개봉일을 집계합니다.'));
  }
  function updateCriterionInput(value) {
    const input = $('threshold-value');
    const absolute = $('threshold-mode').value === 'absolute';
    input.setCustomValidity('');
    if (absolute) input.removeAttribute('max');
    else input.max = '100';
    $('threshold-value-label').textContent = absolute
      ? '기울기 변화 절댓값 (SP/년)' : '백분위 (0 초과~100)';
    $('threshold-help').textContent = absolute
      ? '앞뒤 기울기 차이의 절댓값이 입력값 이상이면 선택합니다. 0을 입력해도 변화량이 정확히 0인 지점은 제외합니다.'
      : '전체 배우의 변화량 절댓값을 비교합니다. 백분위를 낮추면 후보가 늘고, 높이면 큰 변화만 선택됩니다.';
    input.value = String(value);
  }
  function setCalculating(busy) {
    calculating = busy;
    for (const id of ['threshold-mode', 'threshold-value', 'threshold-apply', 'threshold-reset',
                     'actor-query', 'actor-mode', 'results-toggle']) {
      $(id).disabled = busy;
    }
    $('threshold-form').setAttribute('aria-busy', String(busy));
    $('actor-panel').hidden = busy || !current;
    $('cohort').hidden = busy || !summary;
    $('actor-results').hidden = busy || !openResults;
    if (busy) $('threshold-comparison').hidden = true;
  }
  function applyCriterion() {
    if (calculating || !appliedConfig) return;
    const input = $('threshold-value');
    const value = input.valueAsNumber;
    const absolute = $('threshold-mode').value === 'absolute';
    input.setCustomValidity('');
    if (!Number.isFinite(value) || (absolute ? value < 0 : value <= 0 || value > 100)) {
      input.setCustomValidity(absolute ? '0 이상의 유한한 숫자를 입력해 주세요.' : '0보다 크고 100 이하인 백분위를 입력해 주세요.');
    }
    if (!input.reportValidity()) return;
    resultsWereOpen = openResults;
    calculationRequest = ++sequence; // 이전 배우 조회 응답을 무효화합니다.
    setCalculating(true);
    $('threshold-status').textContent = '입력한 기준으로 전체 배우를 다시 계산하고 있습니다…';
    status('');
    worker.postMessage({type: 'calculate', request: calculationRequest,
      options: {percentile: absolute ? appliedConfig.percentile : value / 100,
                absoluteThreshold: absolute ? value : null}});
  }
  function bindControls() {
    $('threshold-form').addEventListener('submit', event => {
      event.preventDefault();
      applyCriterion();
    });
    $('threshold-mode').addEventListener('change', () => {
      const absolute = $('threshold-mode').value === 'absolute';
      updateCriterionInput(absolute ? summary.cutoff ?? 0 : appliedConfig.percentile * 100);
    });
    $('threshold-value').addEventListener('input', () => $('threshold-value').setCustomValidity(''));
    $('threshold-reset').addEventListener('click', () => {
      $('threshold-mode').value = 'percentile';
      updateCriterionInput(95);
      applyCriterion();
    });
    $('actor-query').addEventListener('input',updateRows);
    $('actor-query').addEventListener('keydown',e=>{if(e.key==='Escape')toggleResults(false);if(e.key==='Enter'&&openResults)$('results-list').querySelector('button')?.click();});
    $('actor-mode').addEventListener('change',updateRows);
    $('results-toggle').addEventListener('click',()=>{if(!openResults){actorRows=findRows();resultPage=0;}toggleResults(!openResults);});
  }
  function start(){
    if(typeof Worker!=='function'){status('이 브라우저는 분석 계산에 필요한 웹 워커를 지원하지 않습니다. 최신 브라우저에서 열어 주세요.');return;}
    try {worker=new Worker(new URL('assets/exp1-worker.js?v=20261106-thresholds',document.baseURI));} catch(_){fail('계산을 시작하지 못했습니다. 웹사이트 주소에서 다시 열어 주세요.',true);return;}
    bindControls();
    worker.addEventListener('error',()=>{
      setCalculating(true);
      $('threshold-status').textContent='계산이 중단되었습니다. 다시 시도를 눌러 주세요.';
      fail('분석 계산을 시작하지 못했습니다. 연결 상태를 확인한 뒤 다시 시도해 주세요.',true);
    });
    worker.addEventListener('message',({data})=>{
      if(data.type==='progress'){status(data.message);return;}
      if(data.type==='error'){
        if(data.request!=null&&data.request!==sequence)return;
        if (data.operation === 'calculate' && appliedConfig) {
          calculationRequest = null;
          setCalculating(false);
          showCriterionResult();
          $('threshold-status').textContent += ' · 입력 기준을 적용하지 못했습니다. 값을 확인하고 다시 적용해 주세요.';
          current = null;
          if (selectedId) choose(selectedId, desiredDate, false, isExample);
          return;
        }
        fail(data.request==null?'전체 영화 자료를 불러오거나 계산하지 못했습니다.':'배우의 기록을 계산하지 못했습니다.',data.request==null);return;
      }
      if(data.type==='actor'){
        if(data.request!==sequence)return;
        if(!data.actor){fail('배우의 기록을 찾을 수 없습니다.',false);return;}
        current=data.actor;renderActor(current);return;
      }
      if(data.type!=='ready')return;
      const recalculated = data.request != null;
      if (recalculated && data.request !== calculationRequest) return;
      if (recalculated) previousResult = {config: appliedConfig, summary};
      const keepId = selectedId;
      roster=data.roster;unlinked=data.unlinkedCredits||[];summary=data.summary;
      appliedConfig=data.methods.config;
      registryAvailable=data.registryAvailable;byId=new Map(roster.map(a=>[String(a.id),a]));
      for(const id of shortlist)if(!byId.has(id))shortlist.delete(id);
      calculationRequest=null;
      current=null;
      setCalculating(false);
      syncListLabel();renderSummary(data.methods);
      $('threshold-mode').value=appliedConfig.absoluteThreshold === null ? 'percentile' : 'absolute';
      updateCriterionInput(appliedConfig.absoluteThreshold ?? appliedConfig.percentile * 100);
      showCriterionResult();
      actorRows=findRows();resultPage=0;
      if(recalculated)toggleResults(resultsWereOpen);
      if(recalculated && keepId && byId.has(keepId)) {
        choose(keepId,desiredDate,false,isExample);
        return;
      }
      const requested=new URL(document.baseURI).searchParams.get('actor');
      const example=roster.reduce((best,a)=>a.pointCount&&(!best||a.scoredCount>best.scoredCount)?a:best,null)||roster[0];
      if(requested&&byId.has(requested))choose(requested);
      else if(example)choose(example.id,null,true,true);
      else {status('검색할 배우 ID가 없습니다. 이름을 입력하면 ID 없는 출연 기록을 확인할 수 있습니다.');}
    });
    worker.postMessage({type:'init'});
  }
  start();
})();
