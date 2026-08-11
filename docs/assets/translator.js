/**
 * K-Movie A Archive 다국어 번역 스크립트 (i18n)
 * Fix: 원본 한글 완벽 보존(data-original-ko) 및 전체 페이지 스마트 매칭 엔진 탑재
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

// 🌟 2. 전체 페이지 자동 텍스트 매칭 사전 (지적하신 모든 한글 문구 100% 수록)
const AUTO_TEXT_MAP = {
    // --- index.html ---
    "영화 통합 데이터 통계": "Integrated Movie Database Statistics",
    "등록된 전체 영화 수:": "Total Registered Movies:",
    "국내 영화 수:": "Domestic Movies:",
    "국외 영화 수:": "Foreign Movies:",
    "누적 총 관객 수 (~현재):": "Total Cumulative Audience (~Present):",
    "연도별 영화 개봉 편수": "Annual Movie Releases Trend",
    "연도별 누적 관객수 (단위: 천 명)": "Annual Cumulative Audience (Unit: Thousand)",
    "연도별 누적 관객수 (단위: 천명)": "Annual Cumulative Audience (Unit: Thousand)",
    "* 국내 영화 총": "* Domestic Movies Total",
    "/ 해외 영화 총": "/ Foreign Movies Total",
    "* 연평균 개봉: 국내": "* Annual Avg Releases: Domestic",
    "/ 해외": "/ Foreign",

    // --- title-search.html ---
    "* 제목을 입력하면 유사한 영화 목록이 아래에 표시됩니다.": "* Enter a title keyword to see a list of matching movies below.",
    "* 목록에서 영화를 클릭하거나 방향키로 선택 후 엔터(Enter)를 누르면 가장 정확한 결과의 상세 페이지가 새 탭에서 열립니다.": "* Click a movie or use arrow keys + Enter to open the movie detail page in a new tab.",
    "영화 제목을 입력하세요 (예: 범죄도시)": "Enter movie title (e.g. Oldboy)",
    "일반 검색 결과:": "Search Results:",
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
    "1. 부정 댓글 총합:": "1. Total Negative Comments:",
    "2. 부정 댓글 비율:": "2. Negative Comment Ratio:",
    "3. 부정 댓글 최대 급증/급락폭(기울기):": "3. Max Negative Comment Slope:",
    "연구 대상 사건 발생일 설정:": "Research Milestone Date:",
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
    "최종 점수 (Score)": "Final Score",
    "공식 1: 기여도 및 최신성": "Formula 1: Contribution & Recency",
    "공식 2: 거시경제 시대 보정": "Formula 2: Macroeconomic Era Correction",
    "공식 3: 흥행 타율 변동 모델": "Formula 3: Box Office Batting Average Model",
    "유튜브 여론 차이 분석": "YouTube Sentiment Difference Analysis",
    "연도별 다작 활동성": "Annual Productivity Activity",
    "최종 통합 스타 파워 (Final)": "Final Integrated Star Power (Final)",
    "후보 1: 장르 확장성 (Genre)": "Candidate 1: Genre Scalability (Genre)",
    "후보 2: 여론 기반 파급력 (Sentiment)": "Candidate 2: Sentiment Impact (Sentiment)",
    "후보 3: 출연 대비 타율 (Percentage)": "Candidate 3: Batting Average per Appearance (Percentage)",
    "후보 4: 감독-배우 (Director-Actor)": "Candidate 4: Director-Actor Persona",
    "후보 5: 연도별 관객수 변형 (Annual Efficiency)": "Candidate 5: Annual Efficiency"
};

// 3. 현재 언어 가져오기 (기본 디폴트값: 무조건 'ko')
function getCurrentLang() {
    let lang = localStorage.getItem('kma-lang') || 'ko';
    if (lang !== 'ko' && lang !== 'en') lang = 'ko';
    return lang;
}

// 4. 번역 실행 (원본 한글 데이터 완벽 보존 및 복구 엔진)
function translatePage(lang) {
    const strings = STRINGS[lang] || STRINGS['ko'];

    // 1단계: data-i18n-key 기반 속성 번역
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

    // 2단계: 스마트 자동 매칭 (h1~h5, th, label, option, p, div, span, button, a)
    const targetSelectors = 'h1, h2, h3, h4, h5, th, label, option, p, div, span, button, a';
    document.querySelectorAll(targetSelectors).forEach(el => {
        // 자식 태그(input, select 등)가 없거나 구조상 안전한 요소만 선별
        if (el.children.length === 0 || ['LABEL', 'TH', 'H1', 'H2', 'H3', 'H4', 'H5', 'P', 'SPAN', 'DIV'].includes(el.tagName.toUpperCase())) {
            
            // 🌟 최초 접근 시 무조건 원본 한글 HTML/텍스트를 data-original-ko 속성에 영구 보존
            if (!el.hasAttribute('data-original-ko')) {
                el.setAttribute('data-original-ko', el.innerHTML);
            }

            // 🌟 한글(ko) 모드 요청 시: 보존해 둔 원본 한글 상태로 100% 즉시 복구하고 종료
            if (lang === 'ko') {
                el.innerHTML = el.getAttribute('data-original-ko');
                return;
            }

            // 영어(en) 모드 요청 시: 사전을 확인하여 영어로 치환
            const currentText = el.textContent.trim();
            if (AUTO_TEXT_MAP[currentText]) {
                el.textContent = AUTO_TEXT_MAP[currentText];
            } else {
                // 문장 일부 일치 시 긴 문구부터 순차 치환
                Object.keys(AUTO_TEXT_MAP).sort((a,b) => b.length - a.length).forEach(keyText => {
                    if (currentText.includes(keyText)) {
                        el.innerHTML = el.innerHTML.replace(keyText, AUTO_TEXT_MAP[keyText]);
                    }
                });
            }
        }
    });

    // 타이틀 및 버튼 텍스트 업데이트
    const titleEl = document.querySelector('title');
    const titleKey = titleEl ? titleEl.getAttribute('data-i18n-key') : null;
    if (titleKey && strings[titleKey]) {
        titleEl.textContent = strings[titleKey] + " – K-Movie A Archive";
    }

    const toggleBtn = document.getElementById('lang-toggle');
    if (toggleBtn) toggleBtn.textContent = strings['langToggle'];
}

// 5. 언어 토글 함수
function toggleLanguage() {
    const newLang = getCurrentLang() === 'ko' ? 'en' : 'ko';
    localStorage.setItem('kma-lang', newLang);
    translatePage(newLang);
}

// 6. 초기화 및 버튼 이벤트 바인딩 (annual-ratio.html 등 하드코딩 버튼 100% 완벽 호환)
document.addEventListener('DOMContentLoaded', () => {
    let toggleBtn = document.getElementById('lang-toggle');
    const header = document.querySelector('.ctrls') || document.querySelector('.header');

    // HTML 내에 EN 버튼이 없으면 자동 삽입
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

    // 어떤 페이지에서든 토글 클릭 이벤트가 정상 작동하도록 강제 바인딩
    if (toggleBtn) {
        toggleBtn.onclick = toggleLanguage;
        toggleBtn.addEventListener('click', toggleLanguage);
    }

    translatePage(getCurrentLang());
});
