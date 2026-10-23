/* Standalone observation UI. No scores or observation dates are selected in this view. */
(function () {
  'use strict';
  const $ = id => document.getElementById(id);
  const list = value => Array.isArray(value) ? value : [];
  const finite = value => typeof value === 'number' && Number.isFinite(value);
  const fmt = (value, digits = 2) => finite(value) ? value.toLocaleString('ko-KR', { minimumFractionDigits: digits, maximumFractionDigits: digits }) : '계산 불가';
  const signed = (value, digits = 2) => finite(value) ? (value > 0 ? '+' : '') + fmt(value, digits) : '계산 불가';
  const count = value => finite(value) ? fmt(value, 0) : '미확인';
  const text = value => value === null || value === undefined ? '미확인' : (typeof value === 'object' ? JSON.stringify(value) : String(value));
  const date = value => { const s = String(value || '').replace(/-/g, ''); return /^\d{8}$/.test(s) ? s.slice(0, 4) + '-' + s.slice(4, 6) + '-' + s.slice(6, 8) : (value ? String(value) : '미관측'); };
  const titles = point => list(point && point.movies).map(movie => movie.title || movie.name || movie.id || '제목 미확인').join(' · ') || (point ? '작품명 미확인' : '미관측');
  const ratio = (value, baseline) => finite(value) && finite(baseline) && baseline !== 0 ? value / baseline : null;
  let data = null;

  function el(tag, content, className) {
    const node = document.createElement(tag);
    if (className) node.className = className;
    if (content !== undefined && content !== null) node.textContent = String(content);
    return node;
  }
  function cell(content, className) {
    const node = el('td', undefined, className);
    if (content instanceof Node) node.append(content); else node.textContent = text(content);
    return node;
  }
  function table(headers, rows) {
    const node = el('table'), head = el('thead'), hr = el('tr'), body = el('tbody');
    headers.forEach(label => { const th = el('th', label); th.scope = 'col'; hr.append(th); });
    head.append(hr);
    rows.forEach(cells => { const row = el('tr'); cells.forEach(c => row.append(c instanceof HTMLTableCellElement ? c : cell(c))); body.append(row); });
    node.append(head, body); return node;
  }
  function badge(label, state) { return el('span', label, 'badge ' + (state === true ? 'yes' : state === false ? '' : 'unknown')); }
  function metric(label, value, note) {
    const node = el('div', undefined, 'metric'); node.append(el('span', label), el('strong', value));
    if (note) node.append(el('small', note)); return node;
  }
  function chosenCases() { return list(data.cases).filter(item => !$('followup-actor').value || String(item.id) === $('followup-actor').value); }
  function synergy(point, direction) { return point && point.synergy && point.synergy[direction] || {}; }
  function matchNames(point, direction) {
    if (!point) return '미관측';
    const s = synergy(point, direction), names = list(s.matches).map(m => m.name || m.id || '이름 미확인');
    return names.length ? names.join(' · ') : (finite(s.x) && s.x === 0 ? '관측 없음' : '미확인');
  }
  function pointCell(point) {
    const box = el('div', point ? titles(point) : '미관측');
    if (point) box.append(el('span', date(point.date) + ' · SP ' + fmt(point.score), 'sub'));
    return cell(box, 'title-cell');
  }
  function changes(item) {
    const delta = item.first && item.second && finite(item.first.score) && finite(item.second.score) ? item.second.score - item.first.score : null;
    return { delta, normalized: ratio(delta, item.preMean), above: item.second && finite(item.second.score) && finite(item.preMean) ? item.second.score > item.preMean : null };
  }
  function renderCounts() {
    const c = data.counts || {}, root = $('followup-counts'); root.replaceChildren();
    [['조사 대장 배우', c.registryActors], ['사건 전 이력 적격', c.historyEligible], ['첫 관측 있음', c.firstObserved], ['두 번째 관측 있음', c.secondObserved], ['첫 관측 이전 추적 종료', c.censoredBeforeFirst], ['두 번째 이전 추적 종료', c.censoredBeforeSecond]].forEach(([label, value]) => root.append(metric(label, count(value), '배우 수')));
  }
  function renderSummary(direction) {
    const cases = list(data.cases).filter(item => item.eligible !== false);
    const paired = cases.filter(item => item.first && item.second && finite(item.first.score) && finite(item.second.score));
    const improved = paired.filter(item => item.second.score > item.first.score);
    const baselinePairs = paired.filter(item => finite(item.preMean));
    const above = baselinePairs.filter(item => item.second.score > item.preMean);
    const belowFirst = cases.filter(item => item.first && finite(item.first.score) && finite(item.preMean) && item.first.score < item.preMean);
    const belowPaired = belowFirst.filter(item => item.second && finite(item.second.score));
    const belowImproved = belowPaired.filter(item => item.second.score > item.first.score);
    const knownSynergy = paired.filter(item => finite(synergy(item.second, direction).x));
    const reunited = knownSynergy.filter(item => synergy(item.second, direction).x > 0);
    const root = $('followup-summary'); root.replaceChildren();
    const headline = el('p'); headline.append(el('strong', '비교 가능 ' + paired.length + '명 중 SP 상승 ' + improved.length + '명'));
    root.append(headline,
      el('p', '전체 적격 ' + cases.length + '명 중 첫·두 번째 SP가 모두 있는 ' + paired.length + '명을 비교했습니다. 두 번째 관측이 없는 ' + (cases.length - paired.length) + '명은 결과 미확인으로 남깁니다. 두 번째 SP가 사건 전 평균을 초과한 배우는 기준값이 있는 ' + baselinePairs.length + '명 중 ' + above.length + '명입니다.'),
      el('p', '첫 관측이 사건 전 평균보다 낮은 ' + belowFirst.length + '명 중 후속 비교가 가능한 배우는 ' + belowPaired.length + '명이며, 이 가운데 SP가 상승한 배우는 ' + belowImproved.length + '명입니다.'),
      el('p', (direction === 'forward' ? 'A → B' : 'B → A') + ' 방향의 두 번째 관측 고시너지 동료 재출연: 시너지 자료가 있는 비교 가능 ' + knownSynergy.length + '명 중 ' + reunited.length + '명. 재출연과 SP 변화의 동시 관측이며, 상승의 원인을 입증하지 않습니다.', 'muted'));
  }
  function renderMethod() {
    const config = data.config || {};
    $('followup-method').textContent = '기존 SP 계산식의 λ = ' + count(config.lambda) + ', 자료 마감일 ' + date(config.cutoffDate) + '를 사용합니다. 사건 연도보다 앞선 분석 가능 연도에서 연간 SP 상위 ' + fmt(config.percent) + '%에 든 이력이 2개 연도 이상이고 공개된 공백 근거가 있는 배우를 포함합니다. ' + (config.primaryFdrGate === false ? 'FDR 판정을 적격 조건으로 요구하지 않습니다. ' : 'FDR 적격 조건은 세부 설정을 확인하세요. ') + (config.currentHawkGate === false ? '현재 Hawk 여부도 적격 조건이 아닙니다. ' : '현재 Hawk 적격 조건은 세부 설정을 확인하세요. ') + '고시너지 동료는 사건 공개 월 이전 자료만으로 방향별 계산 가능 동료 중 상위 ' + fmt(config.highSynergyPercent, 0) + '%를 고르고 경계 동률을 포함하되, 시너지 값이 양수인 경우로 제한합니다. 사건 전 기준은 적격 작품별 SP의 평균입니다.';
  }
  function renderComparison(items, direction) {
    const rows = items.map(item => {
      const c = changes(item), status = c.above === null ? '미판정' : c.above ? '초과' : '초과 아님';
      return [cell(item.name || item.id, 'name-cell'), item.eventMonth || '미확인', cell(fmt(item.preMean), 'num'), pointCell(item.first), pointCell(item.second), cell(signed(c.delta), 'num'), cell(finite(c.normalized) ? signed(c.normalized * 100, 1) + '%' : '계산 불가', 'num'), badge(status, c.above), matchNames(item.first, direction), matchNames(item.second, direction)];
    });
    const root = $('followup-comparison'); root.replaceChildren();
    root.append(rows.length ? table(['배우', '사건 공개 월', '사건 전 평균 SP', '첫 관측 · 작품 / 날짜 / SP', '두 번째 관측 · 작품 / 날짜 / SP', 'ΔSP', '정규화 변화', '두 번째 > 사건 전', '첫 관측 고시너지 재출연', '두 번째 고시너지 재출연'], rows) : el('p', '표시할 적격 사례가 없습니다.', 'empty'));
  }
  function svgNode(tag, attrs, content) {
    const node = document.createElementNS('http://www.w3.org/2000/svg', tag);
    Object.entries(attrs || {}).forEach(([name, value]) => node.setAttribute(name, String(value)));
    if (content !== undefined) node.textContent = content;
    return node;
  }
  function dateMs(value) {
    const s = String(value || '').replace(/-/g, '');
    if (!/^\d{6}(\d{2})?$/.test(s)) return null;
    const year = Number(s.slice(0, 4)), month = Number(s.slice(4, 6)), day = Number(s.slice(6, 8) || 1);
    if (month < 1 || month > 12 || day < 1 || day > 31) return null;
    return Date.UTC(year, month - 1, day);
  }
  function trajectory(item) {
    const wrapper = el('div', undefined, 'chart');
    if (!finite(item.preMean) || item.preMean === 0) { wrapper.append(el('p', '사건 전 평균이 없거나 0이므로 정규화 그래프를 계산할 수 없습니다.', 'empty')); return wrapper; }
    const raw = list(item.trajectory).length ? list(item.trajectory) : [item.first, item.second].filter(Boolean);
    const points = raw.map(point => ({ point, time: dateMs(point.date), value: ratio(point.score, item.preMean) })).filter(p => p.time !== null && finite(p.value)).sort((a, b) => a.time - b.time);
    if (!points.length) { wrapper.append(el('p', '사건 공개 이후 관측이 없습니다. 그래프에 0점이나 가상의 후속 점을 추가하지 않습니다.', 'empty')); return wrapper; }
    const width = 920, height = 320, left = 65, right = 35, top = 35, bottom = 65;
    const eventTime = dateMs(item.eventMonth), times = points.map(p => p.time);
    let minX = Math.min(...times, eventTime === null ? times[0] : eventTime), maxX = Math.max(...times);
    const monthMs = 30.4375 * 86400000;
    if (maxX <= minX) maxX = minX + monthMs;
    const padX = Math.max((maxX - minX) * .045, monthMs / 3); minX -= padX; maxX += padX;
    let minY = Math.min(0, 100, ...points.map(p => p.value * 100)), maxY = Math.max(110, ...points.map(p => p.value * 100));
    const padY = Math.max(10, (maxY - minY) * .12); maxY += padY;
    const x = v => left + (v - minX) / (maxX - minX) * (width - left - right), y = v => height - bottom - (v - minY) / (maxY - minY) * (height - top - bottom);
    const svg = svgNode('svg', { viewBox: '0 0 ' + width + ' ' + height, role: 'img', 'aria-label': (item.name || item.id) + ' 사건 전 평균을 100으로 정규화한 실제 개봉 날짜별 SP' });
    svg.append(svgNode('title', {}, (item.name || item.id) + ' 관측 궤적'), svgNode('desc', {}, '가로축은 실제 달력 시간, 세로축은 사건 전 평균 SP를 100으로 놓은 값입니다. 관측되지 않은 값은 표시하지 않습니다.'));
    for (let i = 0; i <= 4; i++) {
      const value = minY + (maxY - minY) * i / 4;
      svg.append(svgNode('line', { x1: left, x2: width - right, y1: y(value), y2: y(value), class: 'grid' }), svgNode('text', { x: left - 10, y: y(value) + 4, 'text-anchor': 'end' }, fmt(value, 0)));
    }
    svg.append(svgNode('line', { x1: left, x2: width - right, y1: y(100), y2: y(100), class: 'baseline' }), svgNode('text', { x: width - right, y: y(100) - 7, 'text-anchor': 'end' }, '사건 전 평균 100'));
    if (eventTime !== null) svg.append(svgNode('line', { x1: x(eventTime), x2: x(eventTime), y1: top, y2: height - bottom, class: 'event-line' }), svgNode('text', { x: x(eventTime) + 5, y: 18 }, '공개 ' + item.eventMonth));
    for (let i = 0; i <= 4; i++) {
      const t = minX + (maxX - minX) * i / 4, d = new Date(t), label = d.getUTCFullYear() + '-' + String(d.getUTCMonth() + 1).padStart(2, '0');
      svg.append(svgNode('text', { x: x(t), y: height - bottom + 25, 'text-anchor': i === 0 ? 'start' : i === 4 ? 'end' : 'middle' }, label));
    }
    if (points.length > 1) svg.append(svgNode('polyline', { points: points.map(p => x(p.time) + ',' + y(p.value * 100)).join(' '), class: 'trace' }));
    points.forEach(p => {
      const isFirst = item.first && p.point.date === item.first.date, isSecond = item.second && p.point.date === item.second.date;
      const circle = svgNode('circle', { cx: x(p.time), cy: y(p.value * 100), r: isFirst || isSecond ? 6 : 4, class: 'point ' + (isFirst ? 'first-point' : isSecond ? 'second-point' : '') });
      circle.append(svgNode('title', {}, date(p.point.date) + ' · ' + titles(p.point) + ' · SP ' + fmt(p.point.score) + ' · 사건 전 대비 ' + fmt(p.value, 3) + '배')); svg.append(circle);
      if (isFirst || isSecond) svg.append(svgNode('text', { x: x(p.time), y: y(p.value * 100) - (isFirst ? 12 : 28), 'text-anchor': x(p.time) > width - 100 ? 'end' : 'middle', class: 'chart-label' }, (isFirst ? '첫 관측 ' : '두 번째 ') + fmt(p.value * 100, 1)));
    });
    svg.append(svgNode('text', { x: left, y: height - 10, class: 'chart-label' }, '개봉 날짜 · 실제 시간 간격'), svgNode('text', { x: left, y: 18 }, '정규화 SP'));
    wrapper.append(svg);
    if (!item.second) wrapper.append(el('p', '두 번째 관측 미관측 — 후속 점수를 추정하지 않았습니다.', 'muted'));
    return wrapper;
  }
  function coactorBlock(point, label, direction) {
    const node = el('section', undefined, 'coactor-block'); node.append(el('h4', label));
    if (!point) { node.append(el('p', '미관측 — 공동출연 수와 시너지 값도 미확인입니다.', 'muted')); return node; }
    const s = synergy(point, direction);
    node.append(el('p', titles(point) + ' · ' + date(point.date) + ' · SP ' + fmt(point.score), 'muted'));
    node.append(el('p', '공동출연 동료 ' + count(point.coactorCount) + '명 · M ' + count(s.M) + ' / K ' + count(s.K) + ' / n ' + count(s.n) + ' / x ' + count(s.x), 'muted'));
    const matches = list(s.matches);
    if (!matches.length) { node.append(el('p', finite(s.x) && s.x === 0 ? '이 방향에서 확인된 고시너지 동료 재출연은 없습니다.' : '고시너지 동료 자료가 없습니다.', 'empty')); return node; }
    const t = table(['고시너지 동료', '방향', '시너지 (%)', '사건 전 공동출연 표본', '사건 전 비공동출연 표본'], matches.map(m => [m.name || m.id, direction === 'forward' ? 'A → B' : 'B → A', finite(m.value) ? fmt(m.value) + '%' : '계산 불가', count(m.joint), count(m.without)]));
    t.className = 'small-table'; const scroll = el('div', undefined, 'scroll'); scroll.append(t); node.append(scroll); return node;
  }
  function sourceLink(source) {
    const li = el('li');
    const raw = typeof source === 'string' ? source : source && (source.url || source.href), label = typeof source === 'string' ? source : source && (source.title || source.label || source.url);
    try { const url = new URL(raw); if (url.protocol !== 'https:' && url.protocol !== 'http:') throw new Error('unsupported'); const a = el('a', label || url.href); a.href = url.href; a.target = '_blank'; a.rel = 'noopener noreferrer'; li.append(a); } catch (_) { li.textContent = text(label || source); }
    return li;
  }
  function renderDetails(items, direction) {
    const root = $('followup-details'); root.replaceChildren();
    items.forEach(item => {
      const card = el('article', undefined, 'case-card'), head = el('div', undefined, 'case-head'), c = changes(item);
      head.append(el('h3', item.name || item.id), el('p', '사건 공개 월 ' + (item.eventMonth || '미확인'), 'muted'));
      const metrics = el('div', undefined, 'metrics');
      metrics.append(metric('사건 전 평균 SP', fmt(item.preMean), '적격 작품 ' + count(item.preN) + '편'), metric('첫 관측 SP', item.first ? fmt(item.first.score) : '미관측', item.first ? date(item.first.date) : '추적 종료 시점까지 관측 없음'), metric('두 번째 관측 SP', item.second ? fmt(item.second.score) : '미관측', item.second ? date(item.second.date) : '후속 결과 미확인'), metric('첫 관측 → 두 번째 ΔSP', signed(c.delta), finite(c.normalized) ? '사건 전 평균 대비 ' + signed(c.normalized * 100, 1) + '%' : '정규화 변화 계산 불가'));
      card.append(head, metrics);
      const rPre = item.second ? ratio(item.second.score, item.preMean) : null, rFirst = item.second && item.first ? ratio(item.second.score, item.first.score) : null;
      card.append(el('p', '두 번째 / 사건 전 평균: ' + (finite(rPre) ? fmt(rPre, 3) + '배' : '계산 불가') + ' · 두 번째 / 첫 관측: ' + (finite(rFirst) ? fmt(rFirst, 3) + '배' : '계산 불가'), 'muted'));
      card.append(trajectory(item));
      const details = el('details'); details.append(el('summary', '첫 관측·두 번째 관측의 고시너지 동료 상세'));
      details.append(el('p', 'M: 사건 전 시너지 계산 가능 동료, K: 그중 고시너지 동료, n: 현재 공동출연 중 M에 포함된 동료, x: 현재 재출연한 고시너지 동료. 방향에 따라 모집단과 결과가 달라질 수 있습니다.', 'muted'), coactorBlock(item.first, '첫 관측', direction), coactorBlock(item.second, '두 번째 관측', direction)); card.append(details);
      const notes = [...list(item.reasons), ...list(item.notes)];
      if (notes.length || list(item.preHighYears).length) { const evidence = el('details'); evidence.append(el('summary', '이력·판정 메모')); if (list(item.preHighYears).length) evidence.append(el('p', '사건 전 높은 SP 이력 연도: ' + item.preHighYears.map(text).join(', '), 'muted')); const ul = el('ul'); notes.forEach(n => ul.append(el('li', text(n)))); evidence.append(ul); card.append(evidence); }
      if (list(item.sources).length) { const details = el('details'), ul = el('ul', undefined, 'source-list'); details.append(el('summary', '사건 공개 근거')); item.sources.forEach(s => ul.append(sourceLink(s))); details.append(ul); card.append(details); }
      root.append(card);
    });
  }
  function renderAudits() {
    const audit = data.audit || {}, checks = list(audit.checks);
    $('followup-audit-status').textContent = checks.length ? ('생성 시 검산: ' + (audit.passed === true ? '모든 검사를 통과했습니다.' : audit.passed === false ? '통과하지 못한 검사가 있습니다.' : '종합 상태 미확인')) : '검산 결과가 제공되지 않았습니다.';
    $('followup-audits').replaceChildren(table(['검사', '결과', '상세'], checks.map(check => [check.name || '이름 없음', badge(check.passed === true ? '통과' : check.passed === false ? '실패' : '미확인', check.passed), text(check.detail)])));
    $('followup-provenance').textContent = JSON.stringify({ version: data.version, generatedAt: data.generatedAt, config: data.config, counts: data.counts, provenance: data.provenance }, null, 2);
    const registry = list(data.registry), registryRoot = $('followup-registry'); registryRoot.replaceChildren();
    if (registry.length) registryRoot.append(table(['배우', '공개 월', '적격', '판정 / 메모'], registry.map(item => [item.name || item.id || '미확인', item.eventMonth || '미확인', item.eligible === true ? '적격' : item.eligible === false ? '비적격' : '미확인', [...list(item.reasons), ...list(item.notes)].map(text).join(' · ') || text(item.status || '')])));
    else registryRoot.append(el('p', '조사 대장이 제공되지 않았습니다.', 'muted'));
  }
  function render() { const items = chosenCases(), direction = $('followup-direction').value; renderSummary(direction); renderComparison(items, direction); renderDetails(items, direction); }
  function download(content, type, name) {
    const blob = new Blob([content], { type }), url = URL.createObjectURL(blob), a = el('a'); a.href = url; a.download = name; a.hidden = true; document.body.append(a); a.click(); a.remove(); setTimeout(() => URL.revokeObjectURL(url), 1000);
  }
  function csvCell(value) {
    let s = value === null || value === undefined ? '' : String(value);
    if (typeof value === 'string' && /^[=+\-@\t\r]/.test(s)) s = "'" + s;
    return '"' + s.replace(/"/g, '""') + '"';
  }
  function exportCSV() {
    const direction = $('followup-direction').value;
    const rows = [['배우ID', '배우', '사건공개월', '사건전평균SP', '사건전작품수', '첫관측일', '첫관측작품', '첫관측SP', '두번째관측일', '두번째관측작품', '두번째관측SP', 'delta_SP', 'delta_사건전평균대비_pct', '두번째_사건전평균비율', '두번째_첫관측비율', '두번째_사건전초과', '시너지방향', '첫관측_고시너지동료', '두번째_고시너지동료']];
    chosenCases().forEach(item => { const c = changes(item); rows.push([item.id, item.name, item.eventMonth, item.preMean, item.preN, item.first && item.first.date, item.first ? titles(item.first) : '', item.first && item.first.score, item.second && item.second.date, item.second ? titles(item.second) : '', item.second && item.second.score, c.delta, finite(c.normalized) ? c.normalized * 100 : null, item.second ? ratio(item.second.score, item.preMean) : null, item.second && item.first ? ratio(item.second.score, item.first.score) : null, c.above, direction, matchNames(item.first, direction), matchNames(item.second, direction)]); });
    download('\uFEFF' + rows.map(row => row.map(csvCell).join(',')).join('\r\n'), 'text/csv;charset=utf-8', 'recovery-followup-' + direction + '.csv');
  }
  async function verify() {
    const button = $('followup-verify'), status = $('followup-verify-status'), output = $('followup-verify-results'); output.replaceChildren();
    if (!globalThis.crypto || !crypto.subtle) { status.textContent = '현재 환경에서 SHA-256을 사용할 수 없습니다. HTTPS 또는 localhost에서 다시 확인하세요.'; return; }
    const p = data.provenance || {}, allEntries = ['inputs', 'code'].flatMap(group => Object.entries(p[group] || {}).map(([path, hash]) => ({ group, path, hash: typeof hash === 'string' ? hash : hash && (hash.sha256 || hash.sha) })));
    const entries = allEntries.filter(entry => entry.path.startsWith('docs/'));
    const skipped = allEntries.length - entries.length;
    if (!entries.length) { status.textContent = '비교할 입력·코드 SHA 지문이 제공되지 않았습니다.'; return; }
    button.disabled = true; status.textContent = '현재 제공되는 파일 ' + entries.length + '개의 SHA-256을 계산하는 중…';
    try {
      const results = await Promise.all(entries.map(async entry => {
        let actual = '', result = '확인 불가';
        try {
          if (!/^[a-f0-9]{64}$/i.test(entry.hash || '')) throw new Error('유효한 SHA-256 지문 없음');
          const path = entry.path.replace(/^\/?docs\//, ''), url = new URL(path, location.href);
          if (url.origin !== location.origin || !/^https?:$/.test(url.protocol)) throw new Error('동일 출처 파일만 확인 가능');
          const response = await fetch(url.href, { cache: 'no-store' }); if (!response.ok) throw new Error('HTTP ' + response.status);
          const digest = await crypto.subtle.digest('SHA-256', await response.arrayBuffer()); actual = Array.from(new Uint8Array(digest), n => n.toString(16).padStart(2, '0')).join('');
          result = actual.toLowerCase() === entry.hash.toLowerCase() ? '일치' : '불일치';
        } catch (error) { result = '확인 불가: ' + error.message; }
        return { ...entry, actual, result };
      }));
      const passed = results.filter(r => r.result === '일치').length, mismatch = results.filter(r => r.result === '불일치').length;
      status.textContent = 'SHA 확인: ' + passed + '/' + results.length + ' 일치 · ' + mismatch + ' 불일치 · ' + (results.length - passed - mismatch) + ' 확인 불가. ' + (passed === results.length ? '현재 제공되는 파일과 기록된 지문이 모두 같습니다.' : '불일치 또는 확인 불가 파일을 아래에서 확인하세요.') + (skipped ? ' 배포 대상이 아닌 빌드 스크립트 ' + skipped + '개의 SHA는 JSON에서 확인할 수 있습니다. 브라우저에서는 저장소 scripts 파일을 검증하지 않습니다.' : '');
      output.append(table(['구분 / 파일', '결과', '기록된 SHA-256', '현재 SHA-256'], results.map(r => [r.group + ' / ' + r.path, r.result, cell(r.hash || '없음', 'sha'), cell(r.actual || '확인 불가', 'sha')])));
    } finally { button.disabled = false; }
  }
  $('followup-print').addEventListener('click', () => window.print());
  $('followup-actor').addEventListener('change', render);
  $('followup-direction').addEventListener('change', render);
  $('followup-csv').addEventListener('click', exportCSV);
  $('followup-json').addEventListener('click', () => download(JSON.stringify(data, null, 2), 'application/json;charset=utf-8', 'recovery-followup.json'));
  $('followup-verify').addEventListener('click', verify);
  fetch('data/research/recovery_followup.json', { cache: 'no-store' }).then(response => { if (!response.ok) throw new Error('HTTP ' + response.status); return response.json(); }).then(result => {
    if (!result || !Array.isArray(result.cases)) throw new Error('사례 목록이 없는 데이터입니다.');
    data = result;
    const select = $('followup-actor'); list(data.cases).forEach(item => { const option = el('option', item.name || item.id); option.value = String(item.id); select.append(option); });
    renderCounts(); render(); renderAudits(); renderMethod();
    document.querySelectorAll('[data-control]').forEach(control => { control.disabled = false; });
    $('followup-meta').textContent = '생성 시각: ' + (data.generatedAt || '미확인') + ' · 버전 ' + (data.version || '미확인') + ' · 적격 영화 ' + count(data.counts && data.counts.eligibleFilms) + '편 · 관측 ' + count(data.counts && data.counts.observations) + '개';
  }).catch(error => {
    $('followup-meta').textContent = '자료를 불러오지 못했습니다: ' + error.message + '. 데이터 생성·배포 상태를 확인한 뒤 새로고침하세요.'; $('followup-meta').classList.add('error');
    $('followup-comparison').replaceChildren(el('p', '자료가 없으므로 결과를 표시하지 않습니다.', 'empty'));
  });
}());
