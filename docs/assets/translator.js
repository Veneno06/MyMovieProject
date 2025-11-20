/**
 * K-Movie A Archive 다국어 번역 스크립트 (i18n)
 * Fix: 버튼 태그 번역 로직 수정
 */

// 1. 번역 문자열 (KO/EN)
const STRINGS = {
    'ko': {
        'langToggle': "EN",
        // --- 공통 헤더 ---
        'home': "홈",
        'movieSearch': "영화 검색",
        'peopleSearch': "배우 검색",
        'costarSearch': "공동 출연 검색",
        'savedList': "저장 목록",
        'compareTitle': "스타 파워 비교",
        // --- 페이지 제목 ---
        'homeTitle': "K-Movie A Archive",
        'movieSearchTitle': "영화 검색(기간)",
        'peopleSearchTitle': "배우/감독 검색",
        'costarSearchTitle': "공동 출연 검색",
        'savedListTitle': "저장 목록",
        'movieDetailTitle': "영화 상세 정보",
        'actorProfileTitle': "배우 프로필",
        // --- index.html ---
        'monthlyTimeline': "월간 타임라인",
        'prevMonth': "◀ 이전 달",
        'nextMonth': "다음 달 ▶",
        'panelDefault': "날짜를 선택하면 해당 날짜의 개봉작을 보여줍니다.",
        'latestBoxOffice': "최신 일일 박스오피스 TOP 10",
        // --- search.html ---
        'dateStart': "시작일:",
        'dateEnd': "종료일:",
        'rating': "등급:",
        'ratingAll': "전체관람가",
        'rating12': "12세 이상 관람가",
        'rating15': "15세 이상 관람가",
        'ratingR': "청소년 관람불가",
        'nation': "국가:",
        'nationAll': "모두",
        'nationK': "국내 영화",
        'nationF': "국외 영화",
        'searchBtn': "검색",
        // --- people-search.html ---
        'nameLabel': "이름:",
        'actorOnly': "배우만",
        // --- costar-search.html ---
        'actorA': "배우 A:",
        'actorB': "배우 B:",
        'costarDefaultMsg': "검색할 두 배우의 이름을 입력하세요. (예: 황정민, 유해진)",
        // --- person.html ---
        'filmography': "출연작 (최신 개봉일 순)",
        'btnBack': "🔙 뒤로",
        'btnSave': "☆ 배우 저장",
        'btnSaved': "⭐ 저장됨(해제)",
        'audiTrend': "개봉 시기별 관객수 추이",
        'audiCnt': "관객수",
        'audiPerActor': "관객수/배우 수",
        'audiPerFilmo': "관객수/출연작",
        'audiPerCareer': "관객수/연기경력",
        'audiBonus': "500만+ 가중치",
        // --- saved.html ---
        'compareBtn2': "스타 파워 비교 (2명 선택)",
        'compareBtn3': "스타 파워 비교 (3명 선택)",
        // --- detail.html ---
        'titleLabel': "제목:",
        'openDtLabel': "개봉일:",
        'showTmLabel': "상영시간:",
        'genreLabel': "장르:",
        'directorLabel': "감독:",
        'companyLabel': "제작사:",
        'actorLabel': "배우:",
        'audiAccLabel': "관객수(누적):",
        'btnMore': "더보기"
    },
    'en': {
        'langToggle': "KR",
        // --- 공통 헤더 ---
        'home': "Home",
        'movieSearch': "Movie Search",
        'peopleSearch': "People Search",
        'costarSearch': "Co-star Search",
        'savedList': "Saved List",
        'compareTitle': "Star Power Compare",
        // --- 페이지 제목 ---
        'homeTitle': "K-Movie A Archive",
        'movieSearchTitle': "Movie Search (by Date)",
        'peopleSearchTitle': "Actor/Director Search",
        'costarSearchTitle': "Co-star Search",
        'savedListTitle': "Saved List",
        'movieDetailTitle': "Movie Details",
        'actorProfileTitle': "Actor Profile",
        // --- index.html ---
        'monthlyTimeline': "Monthly Timeline",
        'prevMonth': "◀ Prev Month",
        'nextMonth': "Next Month ▶",
        'panelDefault': "Select a date to see the movies released on that day.",
        'latestBoxOffice': "Latest Daily Box Office TOP 10",
        // --- search.html ---
        'dateStart': "From:",
        'dateEnd': "To:",
        'rating': "Rating:",
        'ratingAll': "All Audiences",
        'rating12': "12+",
        'rating15': "15+",
        'ratingR': "R-rated",
        'nation': "Nation:",
        'nationAll': "All",
        'nationK': "Domestic",
        'nationF': "Foreign",
        'searchBtn': "Search",
        // --- people-search.html ---
        'nameLabel': "Name:",
        'actorOnly': "Actors only",
        // --- costar-search.html ---
        'actorA': "Actor A:",
        'actorB': "Actor B:",
        'costarDefaultMsg': "Enter names for Actor A and Actor B (e.g., Hwang Jung-min, Yoo Hae-jin)",
        // --- person.html ---
        'filmography': "Filmography (Latest first)",
        'btnBack': "🔙 Back",
        'btnSave': "☆ Save Actor",
        'btnSaved': "⭐ Saved (Remove)",
        'audiTrend': "Audience Trend by Release Date",
        'audiCnt': "Audience Count",
        'audiPerActor': "Audi/Actors",
        'audiPerFilmo': "Audi/Filmography",
        'audiPerCareer': "Audi/Career",
        'audiBonus': "5M+ Bonus",
        // --- saved.html ---
        'compareBtn2': "Compare Star Power (Pick 2)",
        'compareBtn3': "Compare Star Power (Pick 3)",
        // --- detail.html ---
        'titleLabel': "Title:",
        'openDtLabel': "Release Date:",
        'showTmLabel': "Showtime:",
        'genreLabel': "Genre:",
        'directorLabel': "Director:",
        'companyLabel': "Production:",
        'actorLabel': "Actors:",
        'audiAccLabel': "Total Audience:",
        'btnMore': "More"
    }
};

/**
 * 2. 현재 언어 설정 (localStorage에서 로드, 기본 'ko')
 */
function getCurrentLang() {
    let lang = localStorage.getItem('kma-lang') || 'ko';
    if (lang !== 'ko' && lang !== 'en') lang = 'ko';
    return lang;
}

/**
 * 3. 페이지의 모든 텍스트를 번역하는 메인 함수
 */
function translatePage(lang) {
    if (!STRINGS[lang]) lang = 'ko';
    const strings = STRINGS[lang];

    // [data-i18n-key] 속성을 가진 모든 요소를 찾아 번역
    document.querySelectorAll('[data-i18n-key]').forEach(el => {
        const key = el.getAttribute('data-i18n-key');
        if (strings[key]) {
            const tag = el.tagName.toUpperCase();
            
            // [수정] BUTTON 태그는 textContent를 바꿔야 합니다 (value 아님)
            if (tag === 'INPUT') {
                if (el.type === 'submit' || el.type === 'button' || el.type === 'reset') {
                    el.value = strings[key]; // <input type="submit">
                } else {
                    el.placeholder = strings[key]; // <input type="text">
                }
            } else if (tag === 'BUTTON') {
                el.textContent = strings[key]; // <button>Search</button>
            } else if (el.dataset.i18nTarget === 'placeholder') {
                 el.placeholder = strings[key]; 
            } else {
                el.textContent = strings[key]; // <a>, <h1>, <div>, <span> 등
            }
        }
    });

    // <title> 태그 번역
    const titleEl = document.querySelector('title');
    const titleKey = titleEl ? titleEl.getAttribute('data-i18n-key') : null;
    if (titleKey && strings[titleKey]) {
        titleEl.textContent = strings[titleKey] + " – K-Movie A Archive";
    }
    
    // 언어 전환 버튼 자체의 텍스트도 변경 (EN <-> KR)
    const toggleBtn = document.getElementById('lang-toggle');
    if (toggleBtn) {
        toggleBtn.textContent = strings['langToggle'];
    }
}

/**
 * 4. 언어 전환 함수 (버튼 클릭 시)
 */
function toggleLanguage() {
    const currentLang = getCurrentLang();
    const newLang = currentLang === 'ko' ? 'en' : 'ko';
    localStorage.setItem('kma-lang', newLang);
    translatePage(newLang);
}

/**
 * 5. 페이지 헤더에 언어 전환 버튼 삽입
 */
function createLangButton() {
    const header = document.querySelector('.header, .ctrls');
    if (!header) return;

    // 이미 버튼이 있다면 생성하지 않음
    if (document.getElementById('lang-toggle')) return;

    const btn = document.createElement('button');
    btn.id = 'lang-toggle';
    btn.className = 'btn';
    btn.style.marginLeft = '4px';
    btn.style.fontWeight = 'bold';
    btn.onclick = toggleLanguage;

    // 헤더의 마지막 버튼 뒤에 추가
    const allButtons = header.querySelectorAll('a.btn, button.btn');
    if (allButtons.length > 0) {
        const lastButton = allButtons[allButtons.length - 1];
        lastButton.insertAdjacentElement('afterend', btn);
    } else {
        header.appendChild(btn); 
    }
}

/**
 * 6. 페이지 로드 시 실행
 */
document.addEventListener('DOMContentLoaded', () => {
    createLangButton(); // 1. 언어 전환 버튼 생성
    translatePage(getCurrentLang()); // 2. 현재 언어로 즉시 번역
});
