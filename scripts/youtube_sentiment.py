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

# 한글 출력 세팅
sys.stdout.reconfigure(encoding='utf-8')

HERE = Path(__file__).resolve()
ROOT = HERE.parents[1] if HERE.parents[1].name == "MyMovieProject" else HERE.parents[2]
PEOPLE_DIR = ROOT / "docs" / "data" / "people"
SENTIMENT_DIR = ROOT / "docs" / "data" / "sentiment"
SENTIMENT_DIR.mkdir(parents=True, exist_ok=True)

# [핵심] 여러 개의 API 키를 리스트로 묶어 관리합니다.
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
    dt = datetime.strptime(date_str[:10], "%Y-%m-%d")
    year, week, _ = dt.isocalendar()
    return f"{year}-W{week:02d}"

def get_initial_sound(char):
    CHOSUNG_LIST = ['ㄱ', 'ㄲ', 'ㄴ', 'ㄷ', 'ㄸ', 'ㄹ', 'ㅁ', 'ㅂ', 'ㅃ', 'ㅅ', 'ㅆ', 'ㅇ', 'ㅈ', 'ㅉ', 'ㅊ', 'ㅋ', 'ㅌ', 'ㅍ', 'ㅎ']
    if not char: return ""
    if char in CHOSUNG_LIST: return char
    if '가' <= char <= '힣':
        return CHOSUNG_LIST[(ord(char) - 0xAC00) // 588]
    return char

def get_youtube_comments(actor_name, min_views=100000, max_videos_check=10, max_comments_per_video=100):
    global CURRENT_KEY_INDEX
    
    if not API_KEYS:
        print("❌ [오류] 등록된 YOUTUBE_API_KEY가 없습니다.")
        return []

    # API 키를 모두 소진할 때까지 반복
    while CURRENT_KEY_INDEX < len(API_KEYS):
        current_key = API_KEYS[CURRENT_KEY_INDEX]
        youtube = build('youtube', 'v3', developerKey=current_key)
        
        try:
            print(f"🔍 '{actor_name}' 유튜브 검색 중... (Key {CURRENT_KEY_INDEX+1} 사용 / 조회수 {min_views}+ 필터)")
            
            search_response = youtube.search().list(
                q=actor_name, part='id,snippet', maxResults=max_videos_check, type='video', order='relevance'
            ).execute()

            candidate_ids = [item['id']['videoId'] for item in search_response.get('items', [])]
            if not candidate_ids: return []

            stats_response = youtube.videos().list(part='statistics,snippet', id=','.join(candidate_ids)).execute()

            target_video_ids = []
            for item in stats_response.get('items', []):
                view_count = int(item['statistics'].get('viewCount', 0))
                if view_count >= min_views:
                    target_video_ids.append(item['id'])
                    if len(target_video_ids) >= 3: break

            if not target_video_ids: return []

            all_comments = []
            for video_id in target_video_ids:
                comment_response = youtube.commentThreads().list(
                    videoId=video_id, part='snippet', maxResults=max_comments_per_video, order='relevance'
                ).execute()

                for item in comment_response.get('items', []):
                    text = item['snippet']['topLevelComment']['snippet']['textOriginal']
                    date = item['snippet']['topLevelComment']['snippet']['publishedAt']
                    if len(text) > 5 and "http" not in text:
                        all_comments.append({"text": text, "date": date})

            print(f"💬 총 {len(all_comments)}개 댓글 수집 완료.")
            return all_comments

        except HttpError as e:
            # 403 (할당량 초과) 에러 발생 시 다음 키로 교체!
            if e.resp.status in [403, 429]:
                print(f"⚠️ API Key {CURRENT_KEY_INDEX+1} 할당량 초과!")
                CURRENT_KEY_INDEX += 1
                if CURRENT_KEY_INDEX < len(API_KEYS):
                    print(f"🔄 다음 키(Key {CURRENT_KEY_INDEX+1})로 교체하여 다시 시도합니다...")
                    continue # while 루프 처음으로 돌아가 다음 키로 재시도
                else:
                    print("🚨 모든 API 키의 할당량이 소진되었습니다.")
                    return []
            else:
                print(f"❌ 검색 API 기타 오류: {e}")
                return []
        except Exception as e:
            print(f"❌ 알 수 없는 오류: {e}")
            return []
            
    return []

def analyze_sentiment(comments):
    if not comments: return {}
    classifier = load_ai_model()
    timeline_data = {}

    for comment in comments:
        try:
            text = comment["text"][:500]
            label = classifier(text)[0]['label']
            
            if "1 star" in label or "2 stars" in label: sentiment = "negative"
            elif "4 stars" in label or "5 stars" in label: sentiment = "positive"
            else: continue 

            week_str = get_week_string(comment["date"])
            if week_str not in timeline_data: timeline_data[week_str] = {"positive": 0, "negative": 0}
            timeline_data[week_str][sentiment] += 1
        except: continue

    return timeline_data

def run_single(actor_name):
    print(f"\n========================================")
    print(f"🎬 분석 시작: {actor_name}")
    print(f"========================================")
    comments = get_youtube_comments(actor_name)
    timeline_data = analyze_sentiment(comments)
    
    if not timeline_data:
        print(f"❌ 유효한 데이터가 없어 '{actor_name}' 저장을 생략합니다.")
        return False

    save_path = SENTIMENT_DIR / f"{actor_name}.json"
    
    if save_path.exists():
        with open(save_path, 'r', encoding='utf-8') as f:
            existing_data = json.load(f).get("timeline", {})
        for week, counts in timeline_data.items():
            if week not in existing_data: existing_data[week] = {"positive": 0, "negative": 0}
            existing_data[week]["positive"] += counts["positive"]
            existing_data[week]["negative"] += counts["negative"]
        timeline_data = existing_data

    final_data = {
        "actor_name": actor_name,
        "last_updated": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "timeline": timeline_data
    }

    with open(save_path, 'w', encoding='utf-8') as f:
        json.dump(final_data, f, ensure_ascii=False, indent=2)
    print(f"✅ 저장 완료: {actor_name}.json")
    return True

def run_pattern(pattern):
    target_initial = get_initial_sound(pattern[0]) if pattern else None
    print(f"🎬 [자음 검색 모드] 패턴: '{pattern}' -> 초성: '{target_initial or '전체'}'")

    if not PEOPLE_DIR.exists():
        print(f"❌ 배우 데이터 폴더를 찾을 수 없습니다: {PEOPLE_DIR}")
        return

    files = glob.glob(str(PEOPLE_DIR / "*.json"))
    candidate_actors = []
    
    for p in files:
        try:
            with open(p, 'r', encoding='utf-8') as f:
                data = json.load(f)
            info = data.get("peopleInfoResult", {}).get("peopleInfo", data)
            name = info.get("peopleNm", "").strip()
            
            if not name: continue
            
            if target_initial:
                movie_initial = get_initial_sound(name[0])
                if movie_initial.upper() == target_initial.upper():
                    candidate_actors.append(name)
            else:
                candidate_actors.append(name)
        except: continue
        
    candidate_actors = list(set(candidate_actors))
    print(f"-> 대상 초성에 해당하는 총 배우 수: {len(candidate_actors)}명")

    # 최근 업데이트된 배우 스킵 (API 절약)
    target_actors = []
    for name in candidate_actors:
        save_path = SENTIMENT_DIR / f"{name}.json"
        if save_path.exists():
            try:
                with open(save_path, 'r', encoding='utf-8') as f:
                    last_updated_str = json.load(f).get("last_updated", "")
                if last_updated_str:
                    last_updated = datetime.strptime(last_updated_str, "%Y-%m-%d %H:%M:%S")
                    # 최근 6일 이내에 업데이트된 파일이면 스킵
                    if (datetime.now() - last_updated).days < 6:
                        continue
            except: pass
        target_actors.append(name)

    print(f"-> 최근 업데이트된 배우 제외 후 실제 분석할 배우 수: {len(target_actors)}명")

    # API 보호를 위해 1회 실행 시 최대 50명까지만 제한
    LIMIT = 50
    if len(target_actors) > LIMIT:
        print(f"⚠️ 안전을 위해 이번 실행에서는 최대 {LIMIT}명까지만 분석합니다.")
        target_actors = target_actors[:LIMIT]

    if not target_actors:
        print("✅ 이번 실행에서 업데이트가 필요한 배우가 없습니다.")
        return

    success_count = 0
    for idx, actor in enumerate(target_actors):
        print(f"\n전체 진행률: [{idx+1}/{len(target_actors)}]")
        try:
            if run_single(actor):
                success_count += 1
        except Exception as e:
            print(f"⚠️ {actor} 처리 중 예기치 않은 오류 발생: {e}")
            continue
            
        # 키가 2개 모두 소진되었다면 전체 루프 강제 종료
        global CURRENT_KEY_INDEX
        if CURRENT_KEY_INDEX >= len(API_KEYS):
            print("🚨 모든 API 키가 소진되어 오늘의 작업을 조기 종료합니다.")
            break

    print(f"\n🎉 작업 완료! (성공: {success_count}/{len(target_actors)}명)")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--pattern", type=str, help="초성 검색 (예: ㄱ, ㄴ, ㄷ)")
    parser.add_argument("--actor", type=str, help="단일 배우 이름 검색 (예: 마동석)")
    args = parser.parse_args()

    if args.pattern:
        run_pattern(args.pattern)
    elif args.actor:
        run_single(args.actor)
    else:
        print("사용법: python youtube_sentiment.py --pattern ㄱ (또는 --actor 배우이름)")
