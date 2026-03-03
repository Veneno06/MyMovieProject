# scripts/youtube_sentiment.py
import os
import json
import glob
import argparse
from datetime import datetime
from pathlib import Path
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from transformers import pipeline
import sys
import time

# 인코딩 설정
sys.stdout.reconfigure(encoding='utf-8')

HERE = Path(__file__).resolve()
ROOT = HERE.parents[1] if HERE.parents[1].name == "MyMovieProject" else HERE.parents[2]
PEOPLE_DIR = ROOT / "docs" / "data" / "people"
SEARCH_INDEX_PATH = ROOT / "docs" / "data" / "search_index.json"
SENTIMENT_DIR = ROOT / "docs" / "data" / "sentiment"
SENTIMENT_DIR.mkdir(parents=True, exist_ok=True)

API_KEYS = []
for k in [os.environ.get("YOUTUBE_API_KEY"), os.environ.get("YOUTUBE_API_KEY_2")]:
    if k: API_KEYS.append(k)

CURRENT_KEY_INDEX = 0
CLASSIFIER = None 

def load_ai_model():
    global CLASSIFIER
    if CLASSIFIER is None:
        print("🤖 AI 감성 분석 모델 로딩 중... (최초 1회만 실행)")
        CLASSIFIER = pipeline("sentiment-analysis", model="nlptown/bert-base-multilingual-uncased-sentiment")
    return CLASSIFIER

def get_week_string(date_str):
    try:
        # YouTube 날짜 형식 처리를 위한 안전 장치
        dt = datetime.strptime(date_str[:10], "%Y-%m-%d")
        year, week, _ = dt.isocalendar()
        return f"{year}-W{week:02d}"
    except:
        return None

def get_initial_sound(char):
    CHOSUNG_LIST = ['ㄱ', 'ㄲ', 'ㄴ', 'ㄷ', 'ㄸ', 'ㄹ', 'ㅁ', 'ㅂ', 'ㅃ', 'ㅅ', 'ㅆ', 'ㅇ', 'ㅈ', 'ㅉ', 'ㅊ', 'ㅋ', 'ㅌ', 'ㅍ', 'ㅎ']
    if not char: return ""
    if char in CHOSUNG_LIST: return char
    if '가' <= char <= '힣':
        return CHOSUNG_LIST[(ord(char) - 0xAC00) // 588]
    return char

def get_youtube_comments(actor_name):
    """
    개선된 로직:
    1. 2010년 1월 1일 이후 영상 검색
    2. 검색 후보 50개 확보
    3. 댓글 수 기준으로 정렬하여 상위 20개 영상 선정
    4. 각 영상에서 댓글 수집
    """
    global CURRENT_KEY_INDEX
    
    if not API_KEYS:
        print("❌ [오류] 등록된 YOUTUBE_API_KEY가 없습니다.")
        return []

    # [설정] 수집 파라미터
    SEARCH_CANDIDATES = 50       # 1차 검색 후보 수 (기존 10 -> 50)
    TARGET_VIDEO_COUNT = 20      # 최종 분석할 영상 수 (기존 3 -> 20)
    MAX_COMMENTS_PER_VIDEO = 100 # 영상당 최대 댓글 수
    MIN_VIEWS = 10000            # 최소 조회수 (기존 10만 -> 1만으로 완화하여 과거 영상 포함 확률 높임)
    DATE_AFTER = '2010-01-01T00:00:00Z' # 2010년 이후 영상만

    while CURRENT_KEY_INDEX < len(API_KEYS):
        current_key = API_KEYS[CURRENT_KEY_INDEX]
        youtube = build('youtube', 'v3', developerKey=current_key)
        
        try:
            print(f"🔍 '{actor_name}' 유튜브 검색 중... (Key {CURRENT_KEY_INDEX+1})")
            print(f"   조건: 2010년 이후, 댓글 많은 순 상위 {TARGET_VIDEO_COUNT}개 영상 분석")
            
            # 1. 영상 검색 (최신순보다는 관련도순이 배우 검색에 적합하나, 기간 필터 적용)
            search_response = youtube.search().list(
                q=actor_name,
                part='id,snippet',
                maxResults=SEARCH_CANDIDATES,
                type='video',
                order='relevance', 
                publishedAfter=DATE_AFTER 
            ).execute()

            candidate_ids = [item['id']['videoId'] for item in search_response.get('items', [])]
            if not candidate_ids: 
                print("   -> 검색 결과가 없습니다.")
                return []

            # 2. 영상 세부 정보 조회 (통계)
            # id를 콤마로 연결하여 한 번에 조회 (API 비용 절약)
            stats_response = youtube.videos().list(
                part='statistics,snippet', 
                id=','.join(candidate_ids)
            ).execute()

            video_items = stats_response.get('items', [])
            
            # 3. 필터링 및 정렬 (댓글 수 많은 순서)
            valid_videos = []
            for item in video_items:
                stats = item.get('statistics', {})
                view_count = int(stats.get('viewCount', 0))
                comment_count = int(stats.get('commentCount', 0))
                
                # 조회수 1만 이상이고 댓글이 하나라도 있는 경우
                if view_count >= MIN_VIEWS and comment_count > 0:
                    valid_videos.append({
                        'id': item['id'],
                        'title': item['snippet']['title'],
                        'date': item['snippet']['publishedAt'][:10],
                        'comment_count': comment_count
                    })

            # 댓글 수 기준으로 내림차순 정렬
            valid_videos.sort(key=lambda x: x['comment_count'], reverse=True)
            
            # 상위 N개 선정
            target_videos = valid_videos[:TARGET_VIDEO_COUNT]
            
            print(f"   -> 유효 후보 {len(valid_videos)}개 중 댓글 많은 상위 {len(target_videos)}개 선정 완료.")
            if not target_videos: return []

            # 4. 댓글 수집
            all_comments = []
            for i, video in enumerate(target_videos):
                print(f"   [{i+1}/{len(target_videos)}] 댓글 수집: {video['title'][:30]}... ({video['date']}, 댓글 {video['comment_count']}개)")
                try:
                    comment_response = youtube.commentThreads().list(
                        videoId=video['id'], 
                        part='snippet', 
                        maxResults=MAX_COMMENTS_PER_VIDEO, 
                        order='relevance' # 관련도순 (좋아요 많은 댓글 위주)
                    ).execute()

                    for item in comment_response.get('items', []):
                        comment_snippet = item['snippet']['topLevelComment']['snippet']
                        text = comment_snippet['textOriginal']
                        date = comment_snippet['publishedAt']
                        
                        # 너무 짧거나 링크가 포함된 광고성 댓글 제외
                        if len(text) > 3 and "http" not in text:
                            all_comments.append({"text": text, "date": date})
                            
                except HttpError as e:
                    # 댓글 사용 중지된 영상 등은 패스
                    print(f"      -> 댓글 수집 실패 (권한 없음 등): {e.resp.status}")
                    continue
                except Exception:
                    continue

            print(f"💬 총 {len(all_comments)}개 댓글 데이터 확보.")
            return all_comments

        except HttpError as e:
            if e.resp.status in [403, 429]:
                print(f"⚠️ API Key {CURRENT_KEY_INDEX+1} 할당량 초과!")
                CURRENT_KEY_INDEX += 1
                if CURRENT_KEY_INDEX < len(API_KEYS):
                    print(f"🔄 다음 키로 교체합니다...")
                    continue
                else:
                    return []
            else: 
                print(f"❌ API 오류: {e}")
                return []
        except Exception as e:
            print(f"❌ 알 수 없는 오류: {e}")
            return []
            
    return []

def analyze_sentiment(comments):
    if not comments: return {}
    classifier = load_ai_model()
    timeline_data = {}

    print(f"📊 수집된 댓글 {len(comments)}개 감성 분석 시작...")
    
    # 배치 처리가 빠르지만 메모리 문제 방지를 위해 순차 처리
    for i, comment in enumerate(comments):
        try:
            text = comment["text"][:500] # BERT 입력 길이 제한 고려
            label = classifier(text)[0]['label']
            
            # 별점 기준 감성 분류 (1~2점: 부정, 4~5점: 긍정, 3점: 중립-제외)
            if "1 star" in label or "2 stars" in label: sentiment = "negative"
            elif "4 stars" in label or "5 stars" in label: sentiment = "positive"
            else: continue 

            week_str = get_week_string(comment["date"])
            if not week_str: continue
            
            if week_str not in timeline_data: timeline_data[week_str] = {"positive": 0, "negative": 0}
            timeline_data[week_str][sentiment] += 1
            
            if (i+1) % 100 == 0:
                print(f"   -> {i+1}개 분석 완료...")
        except: continue

    return timeline_data

def run_single(actor_name):
    print(f"\n========================================")
    print(f"🎬 분석 시작: {actor_name}")
    print(f"========================================")
    
    # 기존 데이터 로드 (누적 업데이트를 위해)
    save_path = SENTIMENT_DIR / f"{actor_name}.json"
    existing_data = {}
    
    if save_path.exists():
        try:
            with open(save_path, 'r', encoding='utf-8') as f:
                existing_data = json.load(f).get("timeline", {})
        except: pass

    # 새로운 데이터 수집 및 분석
    comments = get_youtube_comments(actor_name)
    new_timeline_data = analyze_sentiment(comments)
    
    if not new_timeline_data and not existing_data:
        print(f"❌ 데이터가 없어 저장을 생략합니다.")
        return False

    # 데이터 병합 (기존 데이터 + 새 데이터)
    # 단순 덮어쓰기가 아니라, 기존 데이터에 누적 (선택 사항: 원하시면 완전 덮어쓰기로 변경 가능)
    # 여기서는 '새로 수집한 내용'으로 덮어씌우는게 깔끔함 (과거 데이터 중복 방지)
    # 사용자의 요청이 '범위를 확장해서 새로 수집'이므로 덮어씌우는 로직(병합 X)을 사용합니다.
    # 만약 병합을 원하시면 아래 로직을 주석 해제하세요.
    
    # for week, counts in new_timeline_data.items():
    #     if week not in existing_data: existing_data[week] = {"positive": 0, "negative": 0}
    #     existing_data[week]["positive"] += counts["positive"]
    #     existing_data[week]["negative"] += counts["negative"]
    # final_timeline = existing_data
    
    final_timeline = new_timeline_data # 새로 수집한 데이터로 갱신

    final_data = {
        "actor_name": actor_name,
        "last_updated": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "timeline": final_timeline
    }

    with open(save_path, 'w', encoding='utf-8') as f:
        json.dump(final_data, f, ensure_ascii=False, indent=2)
    print(f"✅ 저장 완료: {actor_name}.json")
    return True

def run_pattern(pattern):
    target_initial = get_initial_sound(pattern[0]) if pattern else None
    print(f"🎬 [자음 검색] 초성: '{target_initial or '전체'}'")

    MIN_AUDIENCE = 5000000 
    famous_actors = set()
    
    if SEARCH_INDEX_PATH.exists():
        with open(SEARCH_INDEX_PATH, 'r', encoding='utf-8') as f:
            movies = json.load(f)
            for m in movies:
                audi_str = str(m.get('audiAcc', '0')).replace(',', '')
                audi_num = int(audi_str) if audi_str.isdigit() else 0
                
                if audi_num >= MIN_AUDIENCE:
                    for actor in m.get('actors', []):
                        famous_actors.add(actor.get('name', '').strip())
    else:
        print("⚠️ search_index.json 파일이 없어 흥행 필터링을 적용할 수 없습니다.")

    candidate_actors = []
    for name in famous_actors:
        if not name: continue
        if target_initial:
            if get_initial_sound(name[0]).upper() == target_initial.upper():
                candidate_actors.append(name)
        else:
            candidate_actors.append(name)

    print(f"-> 대상 배우 수: {len(candidate_actors)}명")

    success_count = 0
    for idx, actor in enumerate(candidate_actors[:5]): # 자음 검색은 테스트용으로 5명만 제한 (API 보호)
        if run_single(actor): success_count += 1
        
        global CURRENT_KEY_INDEX
        if CURRENT_KEY_INDEX >= len(API_KEYS):
            print("🚨 모든 API 키가 소진되어 종료합니다.")
            break

    print(f"\n🎉 작업 완료! (성공: {success_count}명)")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--pattern", type=str, help="초성 검색")
    parser.add_argument("--actor", type=str, help="단일 또는 다중 배우 검색 (쉼표로 구분)")
    args = parser.parse_args()

    if args.pattern:
        run_pattern(args.pattern)
    elif args.actor:
        # 콤마로 구분된 배우 목록 처리
        actors = [x.strip() for x in args.actor.split(',') if x.strip()]
        print(f"📋 입력된 배우 목록: {actors}")
        
        success = 0
        for name in actors:
            if run_single(name): success += 1
            
            # 키 소진 체크
            if CURRENT_KEY_INDEX >= len(API_KEYS):
                print("🚨 모든 API 키 소진으로 작업을 중단합니다.")
                break
                
        print(f"\n🎉 전체 작업 종료 (처리: {success}/{len(actors)})")
