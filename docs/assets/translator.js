/**
 * K-Movie A Archive 다국어 번역 스크립트 (i18n)
 */

// 1. 번역 문자열 (KO/EN)
const STRINGS = {
    'ko': {
        'langToggle': "EN",
        // --- 공통 헤더 (네비게이션) ---
        'home': "홈",
        'movieTitleSearch': "영화 검색 (제목)", // [추가됨]
        'movieSearchPeriod': "영화 검색 (기간)", // [키 이름 변경: movieSearch -> movieSearchPeriod]
        'peopleSearch': "배우 검색",
        'costarSearch': "공동 출연 검색",
        'savedList': "저장 목록",
        
        // --- 페이지 제목 ---
        'homeTitle': "K-Movie A Archive",
        'peopleSearchTitle': "배우/감독 검색",
        'costarSearchTitle': "공동 출연 검색",
        'savedListTitle': "저장 목록",
        'compareTitle': "스타 파워 비교",
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
        'actorNamesLabel': "배우 이름들:", // [수정]

        // --- person.html ---
        'filmography': "출연작 (최신 개봉일 순)",
        'btnBack': "🔙 뒤로",
        'btnSave': "☆ 배우 저장",
        'btnSaved': "⭐ 저장됨",
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
        // --- Common Header ---
        'home': "Home",
        'movieTitleSearch': "Movie Search (Title)", // [추가됨]
        'movieSearchPeriod': "Movie Search (Date)", // [수정됨]
        'peopleSearch': "People Search",
        'costarSearch': "Co-star Search",
        'savedList': "Saved List",

        // --- Page Titles ---
        'homeTitle': "K-Movie A Archive",
        'peopleSearchTitle': "Actor/Director Search",
        'costarSearchTitle': "Co-star Search",
        'savedListTitle': "Saved List",
        'compareTitle': "Star Power Compare",
        'movieDetailTitle': "Movie Details",
        'actorProfileTitle': "Actor Profile",

        // --- index.html ---
        'monthlyTimeline': "Monthly Timeline",
        'prevMonth': "◀ Prev Month",
        'nextMonth': "Next Month ▶",
        'panelDefault': "Select a date to see movies released on that day.",
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
        'actorNamesLabel': "Actor Names:",

        // --- person.html ---
        'filmography': "Filmography (Latest first)",
        'btnBack': "🔙 Back",
        'btnSave': "☆ Save Actor",
        'btnSaved': "⭐ Saved",
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

// 2. 현재 언어 가져오기
function getCurrentLang() {
    let lang = localStorage.getItem('kma-lang') || 'ko';
    if (lang !== 'ko' && lang !== 'en') lang = 'ko';
    return lang;
}

// 3. 번역 실행
function translatePage(lang) {
    if (!STRINGS[lang]) lang = 'ko';
    const strings = STRINGS[lang];

    document.querySelectorAll('[data-i18n-key]').forEach(el => {
        const key = el.getAttribute('data-i18n-key');
        if (strings[key]) {
            const tag = el.tagName.toUpperCase();
            if (tag === 'INPUT') {
                if (['submit','button','reset'].includes(el.type)) el.value = strings[key];
                else el.placeholder = strings[key];
            } else if (tag === 'BUTTON') {
                el.textContent = strings[key];
            } else if (el.dataset.i18nTarget === 'placeholder') {
                el.placeholder = strings[key];
            } else {
                el.textContent = strings[key];
            }
        }
    });

    // Title 태그 처리
    const titleEl = document.querySelector('title');
    const titleKey = titleEl ? titleEl.getAttribute('data-i18n-key') : null;
    if (titleKey && strings[titleKey]) {
        titleEl.textContent = strings[titleKey] + " – K-Movie A Archive";
    }

    // 언어 버튼 텍스트 변경
    const toggleBtn = document.getElementById('lang-toggle');
    if (toggleBtn) toggleBtn.textContent = strings['langToggle'];
}

// 4. 언어 전환
function toggleLanguage() {
    const newLang = getCurrentLang() === 'ko' ? 'en' : 'ko';
    localStorage.setItem('kma-lang', newLang);
    translatePage(newLang);
}

// 5. 버튼 생성 및 초기화
document.addEventListener('DOMContentLoaded', () => {
    // 버튼 생성
    const header = document.querySelector('.ctrls') || document.querySelector('.header');
    if (header && !document.getElementById('lang-toggle')) {
        const btn = document.createElement('button');
        btn.id = 'lang-toggle';
        btn.className = 'btn';
        btn.style.marginLeft = '4px';
        btn.style.fontWeight = 'bold';
        btn.style.minWidth = '40px';
        btn.onclick = toggleLanguage;
        header.appendChild(btn);
    }
    // 번역 적용
    translatePage(getCurrentLang());
});
