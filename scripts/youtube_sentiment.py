# scripts/youtube_sentiment.py
import os
import json
import argparse
from datetime import datetime
from pathlib import Path
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from transformers import pipeline
import sys
import time
import re

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
        print("🤖 AI 감성 분석 모델 로딩 중... (KoELECTRA-small-v3-nsmc 적용)")
        # [최종 채택 모델] 속도와 정확도 모두 가장 우수한 KoELECTRA 기반 모델
        CLASSIFIER = pipeline("sentiment-analysis", model="daekeun-ml/koelectra-small-v3-nsmc")
    return CLASSIFIER

def clean_text(text):
    text = re.sub(r'([ㅋㅎㅠㅜ]){3,}', r'\1\1', text)
    return text[:500]

def get_week_string(date_str):
    try:
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
    global CURRENT_KEY_INDEX
    
    if not API_KEYS:
        print("❌ [오류] 등록된 YOUTUBE_API_KEY가 없습니다.")
        return []

    all_comments = []
    current_year = datetime.now().year
    start_year = 2005 

    print(f"🔍 '{actor_name}' 연도별({start_year}~{current_year}) 유튜브 영상 분할 수집 시작...")

    for target_year in range(start_year, current_year + 1):
        if CURRENT_KEY_INDEX >= len(API_KEYS):
            print("🚨 모든 API 키가 소진되어 데이터 수집을 중단합니다.")
            break

        published_after = f"{target_year}-01-01T00:00:00Z"
        published_before = f"{target_year}-12-31T23:59:59Z"

        success_for_year = False
        while CURRENT_KEY_INDEX < len(API_KEYS) and not success_for_year:
            current_key = API_KEYS[CURRENT_KEY_INDEX]
            youtube = build('youtube', 'v3', developerKey=current_key)
            
            try:
                print(f" 📅 [{target_year}년] 영상 검색 중...")
                
                search_response = youtube.search().list(
                    q=actor_name,
                    part='id',
                    maxResults=10, 
                    type='video',
                    order='relevance', 
                    publishedAfter=published_after,
                    publishedBefore=published_before
                ).execute()

                candidate_ids = [item['id']['videoId'] for item in search_response.get('items', [])]
                
                if not candidate_ids:
                    print(f"   -> 검색 결과 없음.")
                    success_for_year = True 
                    continue

                stats_response = youtube.videos().list(
                    part='statistics,snippet', 
                    id=','.join(candidate_ids)
                ).execute()

                valid_videos = []
                for item in stats_response.get('items', []):
                    stats = item.get('statistics', {})
                    view_count = int(stats.get('viewCount', 0))
                    comment_count = int(stats.get('commentCount', 0))
                    
                    if view_count >= 5000 and comment_count > 0:
                        valid_videos.append({
                            'id': item['id'],
                            'title': item['snippet']['title'],
                            'comment_count': comment_count
                        })

                valid_videos.sort(key=lambda x: x['comment_count'], reverse=True)
                target_videos = valid_videos[:3] 

                if not target_videos:
                    print(f"   -> 유효한(댓글 있는) 영상 없음.")
                    success_for_year = True
                    continue

                year_comments = 0
                for video in target_videos:
                    try:
                        comment_response = youtube.commentThreads().list(
                            videoId=video['id'], 
                            part='snippet', 
                            maxResults=100, 
                            order='relevance'
                        ).execute()

                        for item in comment_response.get('items', []):
                            comment_snippet = item['snippet']['topLevelComment']['snippet']
                            text = comment_snippet['textOriginal']
                            date = comment_snippet['publishedAt'] 
                            
                            if len(text) > 3 and "http" not in text:
                                all_comments.append({"text": text, "date": date})
                                year_comments += 1
                                
                    except HttpError as e:
                        continue
                        
                print(f"   -> {len(target_videos)}개 영상에서 댓글 {year_comments}개 수집 완료.")
                success_for_year = True

            except HttpError as e:
                if e.resp.status in [403, 429]:
                    print(f"⚠️ API Key {CURRENT_KEY_INDEX+1} 할당량 초과!")
                    CURRENT_KEY_INDEX += 1
                    if CURRENT_KEY_INDEX < len(API_KEYS):
                        print(f"🔄 다음 키로 교체하여 {target_year}년 데이터부터 이어서 시도합니다...")
                    else:
                        print("🚨 모든 키가 소진되었습니다.")
                        return all_comments
                else:
                    print(f"❌ API 통신 오류: {e}")
                    success_for_year = True 
            except Exception as e:
                print(f"❌ 알 수 없는 오류: {e}")
                success_for_year = True

    print(f"💬 총 누적 {len(all_comments)}개 댓글 데이터 확보 완료.")
    return all_comments

def analyze_sentiment(comments):
    if not comments: return {}
    classifier = load_ai_model()
    timeline_data = {}

    print(f"📊 수집된 댓글 {len(comments)}개 감성 분석 시작 (KoELECTRA 적용)...")
    
    for i, comment in enumerate(comments):
        try:
            raw_text = comment["text"][:500] 
            clean_txt = clean_text(raw_text)
            
            result = classifier(clean_txt)[0]
            label = str(result['label']).lower()
            score = result['score'] 
            
            # 확신도 60% 미만은 중립으로 판단하여 과감히 버림
            if score < 0.6:
                continue

            # KoELECTRA 모델은 '0' (부정), '1' (긍정) 형식으로 결과값을 출력함
            if '1' in label or 'positive' in label:
                sentiment = "positive"
            elif '0' in label or 'negative' in label:
                sentiment = "negative"
            else:
                continue 

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
    
    save_path = SENTIMENT_DIR / f"{actor_name}.json"
    comments = get_youtube_comments(actor_name)
    new_timeline_data = analyze_sentiment(comments)
    
    if not new_timeline_data:
        print(f"❌ 유효한 데이터가 없어 저장을 생략합니다.")
        return False

    final_data = {
        "actor_name": actor_name,
        "last_updated": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "timeline": new_timeline_data
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
    for idx, actor in enumerate(candidate_actors[:5]): 
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
        actors = [x.strip() for x in args.actor.split(',') if x.strip()]
        print(f"📋 입력된 배우 목록: {actors}")
        
        success = 0
        for name in actors:
            if run_single(name): success += 1
            if CURRENT_KEY_INDEX >= len(API_KEYS):
                print("🚨 모든 API 키 소진으로 작업을 중단합니다.")
                break
                
        print(f"\n🎉 전체 작업 종료 (처리: {success}/{len(actors)})")
