/**
 * K-Movie A Archive 다국어 번역 스크립트 (i18n)
 * Smart Auto-Text Matching 엔진 탑재 (속성 없이도 전체 페이지 자동 번역)
 */

// 1. 기본 태그 속성(data-i18n-key) 기반 번역 딕셔너리
const STRINGS = {
    'ko': {
        'langToggle': "EN",
        'home': "홈",
        'movieTitleSearch': "영화 검색 (제목)",
        'movieSearchPeriod': "영화 검색 (기간)",
        'peopleSearch': "배우/감독 검색",
        'costarSearch': "공동 출연 검색",
        'savedList': "저장 목록",
        'annualRatio': "관객수 비율",
        'homeTitle': "K-Movie A Archive",
        'movieSearchTitle': "영화 검색(기간)",
        'peopleSearchTitle': "배우/감독 검색",
        'costarSearchTitle': "공동 출연 검색",
        'savedListTitle': "저장 목록",
        'compareTitle': "배우 관객수 비교",
        'movieDetailTitle': "영화 상세 정보",
        'actorProfileTitle': "인물 프로필",
        'annualRatioTitle': "연간 관객수 비율",
        'monthlyTimeline': "월간 타임라인",
        'prevMonth': "◀ 이전 달",
        'nextMonth': "다음 달 ▶",
        'panelDefault': "날짜를 선택하면 해당 날짜의 개봉작을 보여줍니다.",
        'latestBoxOffice': "최신 일일 박스오피스 TOP 10",
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
        'nameLabel': "이름:",
        'actorOnly': "배우만",
        'actorNamesLabel': "배우 이름들:",
        'filmography': "참여작 (최신 개봉일 순)",
        'btnBack': "🔙 뒤로",
        'btnSave': "☆ 인물 저장",
        'btnSaved': "⭐ 저장됨",
        'audiTrend': "개봉 시기별 관객수 추이",
        'audiCnt': "관객수",
        'audiPerActor': "관객수/배우 수",
        'audiPerFilmo': "관객수/참여작",
        'audiPerCareer': "관객수/경력",
        'audiBonus': "500만+ 가중치",
        'compareBtn2': "스타 파워 비교 (2명 선택)",
        'compareBtn3': "스타 파워 비교 (3명 선택)",
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
        'home': "Home",
        'movieTitleSearch': "Movie Search (Title)",
        'movieSearchPeriod': "Movie Search (Date)",
        'peopleSearch': "Actor/Director",
        'costarSearch': "Co-star Search",
        'savedList': "Saved List",
        'annualRatio': "Audience Ratio",
        'homeTitle': "K-Movie A Archive",
        'movieSearchTitle': "Movie Search (by Date)",
        'peopleSearchTitle': "Actor/Director Search",
        'costarSearchTitle': "Co-star Search",
        'savedListTitle': "Saved List",
        'compareTitle': "Actor Audience Comparison",
        'movieDetailTitle': "Movie Details",
        'actorProfileTitle': "Profile",
        'annualRatioTitle': "Annual Audience Ratio",
        'monthlyTimeline': "Monthly Timeline",
        'prevMonth': "◀ Prev Month",
        'nextMonth': "Next Month ▶",
        'panelDefault': "Select a date to see movies released on that day.",
        'latestBoxOffice': "Latest Daily Box Office TOP 10",
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
        'nameLabel': "Name:",
        'actorOnly': "Actors only",
        'actorNamesLabel': "Actor Names:",
        'filmography': "Filmography (Latest first)",
        'btnBack': "🔙 Back",
        'btnSave': "☆ Save",
        'btnSaved': "⭐ Saved",
        'audiTrend': "Audience Trend by Release Date",
        'audiCnt': "Audience Count",
        'audiPerActor': "Audi/Actors",
        'audiPerFilmo': "Audi/Filmography",
        'audiPerCareer': "Audi/Career",
        'audiBonus': "5M+ Bonus",
        'compareBtn2': "Compare Star Power (Pick 2)",
        'compareBtn3': "Compare Star Power (Pick 3)",
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

// 🌟 2. 스마트 텍스트 자동 매칭 사전 (data-i18n-key가 없는 소제목, 설명, 표 헤더 자동 번역)
const AUTO_TEXT_MAP = {
    // --- 공통 & index.html ---
    "영화 통합 데이터 통계": "Integrated Movie Database Statistics",
    "등록된 전체 영화 수:": "Total Registered Movies:",
    "국내 영화 수:": "Domestic Movies:",
    "국외 영화 수:": "Foreign Movies:",
    "누적 총 관객 수 (~현재):": "Total Cumulative Audience (~Present):",
    "연도별 영화 개봉 편수": "Annual Movie Releases Trend",
    "국내 영화": "Domestic Movies",
    "해외 영화": "Foreign Movies",

    // --- title-search.html ---
    "영화 제목을 입력하여 검색합니다.": "Search movies by entering a title keyword.",
    "부분 일치 검색을 지원합니다.": "Supports partial keyword matching.",
    "영화 제목 검색": "Movie Title Search",

    // --- people-search.html ---
    "배우/감독 검색": "Actor/Director Search",
    "데이터베이스 전체 통계": "Database Overall Statistics",
    "총 등록된 배우:": "Total Registered Actors:",
    "국내 배우:": "Domestic Actors:",
    "해외 배우:": "Foreign Actors:",
    "국내 감독:": "Domestic Directors:",
    "해외 감독:": "Foreign Directors:",
    "국내 배우 성비:": "Domestic Actor Gender Ratio:",
    "스타 파워 랭킹 총 인원:": "Total Star Power Ranked:",
    "스타 파워 평균:": "Average Star Power:",
    "스타 파워 최고점:": "Highest Star Power:",
    "스타 파워 최저점:": "Lowest Star Power:",
    "국내 스타 파워 구간별 인원 분포 (막대형)": "Domestic Star Power Distribution by Range (Bar)",
    "국내 배우 스타 파워 분포 (산점도)": "Domestic Actor Star Power Distribution (Scatter)",
    "배우 프로필 여론 필터링": "Actor Profile Sentiment Filtering",
    "유튜브 여론 심층 필터링": "In-depth YouTube Sentiment Filtering",
    "실시간 스타 파워 (SP_Final) TOP 10": "Real-time Star Power (SP_Final) TOP 10",
    "통합 랭킹": "Overall Ranking",
    "남자 배우 랭킹": "Male Actor Ranking",
    "여자 배우 랭킹": "Female Actor Ranking",
    "위에서부터": "From Top",
    "아래에서부터": "From Bottom",
    "위 조건으로 여론 기반 검색": "Search based on sentiment conditions",
    "Colab용 h1_data.csv 다운로드": "Download h1_data.csv for Colab",
    "배우 전체 여론 요약 다운로드": "Download Actor Sentiment Summary CSV",

    // --- costar-search.html ---
    "공동 출연 검색": "Co-star Search",
    "유튜브 여론 분석 (긍정/부정) 비교": "YouTube Sentiment Analysis (Positive/Negative) Comparison",
    "검색된 배우들의 긍정 댓글(+)과 부정 댓글(-) 추이를 개봉 시기에 맞춰 함께 비교합니다.": "Compare positive (+) and negative (-) comment trends of searched actors aligned with release dates.",

    // --- person.html ---
    "유튜브 여론 분석 (긍정/부정)": "YouTube Sentiment Analysis (Positive/Negative)",
    "여론 분석 데이터 수집 출처": "Sentiment Analysis Data Sources",
    "영상 업로드 시기": "Upload Date",
    "채널명": "Channel Name",
    "영상 제목": "Video Title",
    "추출 댓글": "Extracted Comments",
    "X축 (기간) 설정:": "X-Axis (Period):",
    "Y축 (댓글 수) 고정:": "Y-Axis (Max Comments):",
    "전체 기간 보기": "View All Periods",
    "최근 3개월 (12주)": "Last 3 Months (12 Weeks)",
    "최근 6개월 (24주)": "Last 6 Months (24 Weeks)",
    "최근 1년 (52주)": "Last 1 Year (52 Weeks)",
    "최근 2년 (104주)": "Last 2 Years (104 Weeks)",
    "최근 3년 (156주)": "Last 3 Years (156 Weeks)",
    "최근 5년 (260주)": "Last 5 Years (260 Weeks)",
    "그래프 적용": "Apply Chart",

    // --- annual-ratio.html ---
    "연간 관객수 비율": "Annual Audience Ratio",
    "Hawk 그룹 추적 및 데이터 추출": "Hawk Group Tracking & Data Extraction",
    "연도별 관객수 분포 및 편차 분석": "Annual Audience Distribution & Deviation",
    "설정 적용 및 검색": "Apply & Search",
    "해당 조건으로 검색": "Search with conditions",
    "기본 CSV 다운로드": "Download Base CSV",
    "배우들 여론 분석(댓글 분석용)": "Download AI Prompt (JSON)",
    "Hawk ➔ Dove로 바뀐 배우들": "Actors changed from Hawk ➔ Dove",
    "Dove ➔ Hawk로 바뀐 배우들": "Actors changed from Dove ➔ Hawk",
    "시점 필터링:": "Period Filter:",
    "Hawk 기준(상위 %):": "Hawk Criteria (Top %):",
    "최소 진입 횟수:": "Min Appearance:",
    "조회 연도:": "Target Years:",

    // --- starpower.html & starpower-candidates.html ---
    "스타 파워 (Star Power) 비교": "Star Power Comparison",
    "배우의 흥행력, 여론, 점유율, 다작 활동성이 개봉 시기별로 어떻게 변화하는지 꺾은선 그래프로 비교합니다.": "Compare changes in box office power, sentiment, market share, and productivity over time.",
    "스타 파워 (Star Power) 공식 후보 테스트": "Star Power Formula Candidate Test",
    "새롭게 고안된 스타 파워 공식 후보들을 실제 데이터에 적용하여 비교해 보는 테스트 페이지": "Test page to apply and compare newly designed Star Power formula candidates on real data.",
    "순위 (현재 기준)": "Rank (Current)",
    "배우 이름": "Actor Name",
    "분석된 영화 수": "Analyzed Movies",
    "누적 관객수 합계": "Total Cumulative Audience",
    "대표 점수": "Representative Score",
    "평균 점수 (Average)": "Average Score",
    "최종 점수 (Score)": "Final Score"
};

// 역방향 매칭 사전 (EN -> KO 복구용)
const REVERSE_TEXT_MAP = {};
Object.entries(AUTO_TEXT_MAP).forEach(([ko, en]) => {
    REVERSE_TEXT_MAP[en] = ko;
});

function getCurrentLang() {
    let lang = localStorage.getItem('kma-lang') || 'ko';
    if (lang !== 'ko' && lang !== 'en') lang = 'ko';
    return lang;
}

function translatePage(lang) {
    const strings = STRINGS[lang] || STRINGS['ko'];
    const autoMap = lang === 'en' ? AUTO_TEXT_MAP : REVERSE_TEXT_MAP;

    // 1단계: data-i18n-key 속성 기반 번역
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

    // 2단계: 스마트 자동 텍스트 매칭 (속성이 없는 h1~h4, th, label, option, p, div, span, button 번역)
    const targetSelectors = 'h1, h2, h3, h4, th, label, option, p, div, span, button, a';
    document.querySelectorAll(targetSelectors).forEach(el => {
        // 자식 HTML 태그(input, select 등)를 포함하지 않는 순수 텍스트 노드나 말단 요소만 변환
        if (el.children.length === 0 || el.tagName.toUpperCase() === 'LABEL' || el.tagName.toUpperCase() === 'TH') {
            const currentText = el.textContent.trim();
            if (autoMap[currentText]) {
                el.textContent = autoMap[currentText];
            } else {
                // 문장 일부가 포함된 경우(예: "데이터베이스 전체 통계 (2003년...") 부분 치환
                Object.keys(autoMap).forEach(keyText => {
                    if (currentText.includes(keyText)) {
                        el.innerHTML = el.innerHTML.replace(keyText, autoMap[keyText]);
                    }
                });
            }
        }
    });

    // 타이틀 및 토글 버튼 텍스트 변경
    const titleEl = document.querySelector('title');
    const titleKey = titleEl ? titleEl.getAttribute('data-i18n-key') : null;
    if (titleKey && strings[titleKey]) {
        titleEl.textContent = strings[titleKey] + " – K-Movie A Archive";
    }

    const toggleBtn = document.getElementById('lang-toggle');
    if (toggleBtn) toggleBtn.textContent = strings['langToggle'];
}

function toggleLanguage() {
    const newLang = getCurrentLang() === 'ko' ? 'en' : 'ko';
    localStorage.setItem('kma-lang', newLang);
    translatePage(newLang);
}

// 초기화 및 버튼 바인딩 (하드코딩된 EN 버튼도 100% 감지하여 바인딩)
document.addEventListener('DOMContentLoaded', () => {
    let toggleBtn = document.getElementById('lang-toggle');
    const header = document.querySelector('.ctrls') || document.querySelector('.header');

    // HTML에 EN 버튼이 없으면 자동 생성
    if (header && !toggleBtn) {
        toggleBtn = document.createElement('button');
        toggleBtn.id = 'lang-toggle';
        toggleBtn.className = 'btn';
        toggleBtn.style.marginLeft = '4px';
        toggleBtn.style.fontWeight = 'bold';
        toggleBtn.style.minWidth = '40px';
        
        const annualBtn = document.querySelector('[data-i18n-key="annualRatio"]');
        if (annualBtn) {
            header.insertBefore(toggleBtn, annualBtn);
        } else {
            header.appendChild(toggleBtn);
        }
    }

    // 🌟 확실한 이벤트 바인딩 (annual-ratio.html 등 하드코딩 버튼 먹통 문제 완전 해결)
    if (toggleBtn) {
        toggleBtn.onclick = toggleLanguage;
        toggleBtn.addEventListener('click', toggleLanguage);
    }

    translatePage(getCurrentLang());
});
