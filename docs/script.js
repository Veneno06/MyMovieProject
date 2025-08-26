// docs/script.js
(async () => {
  try {
    // 캐시 회피용 쿼리 스트링
    const ts = Date.now();

    // 1) latest.json에서 최신 파일 메타 정보를 읽는다
    const metaResp = await fetch(`./data/latest.json?v=${ts}`);
    if (!metaResp.ok) throw new Error('latest.json을 찾을 수 없습니다.');
    const meta = await metaResp.json();

    // 2) 메타에 적힌 실제 데이터 파일을 로드한다
    const dataResp = await fetch(`${meta.url}?v=${ts}`);
    if (!dataResp.ok) throw new Error(`데이터 파일(${meta.url})을 찾을 수 없습니다.`);
    const data = await dataResp.json();

    // (A) 원본 JSON도 화면에 보여주기
    const pre = document.createElement('pre');
    pre.textContent = JSON.stringify(data, null, 2);
    document.body.appendChild(pre);

    // (B) 상위 10개 간단 리스트 (있을 때만)
    const list = data?.boxOfficeResult?.dailyBoxOfficeList;
    if (Array.isArray(list)) {
      const h2 = document.createElement('h2');
      h2.textContent = `최신 일일 박스오피스 TOP ${Math.min(10, list.length)} (기준일: ${meta.date})`;
      document.body.appendChild(h2);

      const ol = document.createElement('ol');
      list.slice(0, 10).forEach(item => {
        const li = document.createElement('li');
        li.textContent = `${item.rank}. ${item.movieNm} - ${Number(item.audiCnt).toLocaleString()}명`;
        ol.appendChild(li);
      });
      document.body.appendChild(ol);
    }
  } catch (err) {
    console.error(err);
    const p = document.createElement('p');
    p.style.color = 'red';
    p.textContent = `데이터 불러오기 실패: ${err.message}`;
    document.body.appendChild(p);
  }
})();
