fetch('./data/20250826.json')
  .then(r => r.json())
  .then(d => {
    const pre = document.createElement('pre');
    pre.textContent = JSON.stringify(d, null, 2);
    document.body.appendChild(pre);
  })
  .catch(err => console.error('불러오기 실패:', err));
