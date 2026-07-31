# scripts/youtube_hawk_sentiment.py
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

sys.stdout.reconfigure(encoding='utf-8')

ROOT = Path(__file__).resolve().parents[1]
SENTIMENT_DIR = ROOT / "docs" / "data" / "sentiment"
SENTIMENT_DIR.mkdir(parents=True, exist_ok=True)

# 🌟 Hawk 그룹 전용 6개 API 키 로드
API_KEYS = []
for i in range(1, 7):
    val = os.environ.get(f"YOUTUBE_HAWK_API_KEY_{i}")
    if val and val.strip():
        API_KEYS.append(val.strip())

CURRENT_KEY_INDEX = 0
CLASSIFIER = None 

def load_ai_model():
    global CLASSIFIER
    if CLASSIFIER is None:
        print("🤖 AI 모델 로딩 중... (KoELECTRA-small-v3-nsmc 적용)")
        CLASSIFIER = pipeline("sentiment-analysis", model="daekeun-ml/koelectra-small-v3-nsmc")
    return CLASSIFIER

def clean_text(text):
    text = re.sub(r'([ㅋㅎㅠㅜ]){3,}', r'\1\1', text)
    return text[:500]

def search_videos_for_year(actor_name, target_year):
    global CURRENT_KEY_INDEX
    if not API_KEYS:
        print("❌ [오류] YOUTUBE_HAWK_API_KEY 가 없습니다.")
        return []

    published_after = f"{target_year}-01-01T00:00:00Z"
    published_before = f"{target_year}-12-31T23:59:59Z"
    search_query = f"{actor_name} 영화 | {actor_name} 예고편 | {actor_name} 인터뷰 | {actor_name} 무대인사 | {actor_name} 리뷰 | {actor_name} 예능 | {actor_name} 연기 | {actor_name} 직캠 | {actor_name} 출연 | {actor_name} 시상식"

    while CURRENT_KEY_INDEX < len(API_KEYS):
        current_key = API_KEYS[CURRENT_KEY_INDEX]
        youtube = build('youtube', 'v3', developerKey=current_key)
        
        try:
            print(f" 📅 [{target_year}년] 1단계: 검색 실행 중... (Key {CURRENT_KEY_INDEX + 1})")
            search_response = youtube.search().list(
                q=search_query, part='id', maxResults=15, type='video', order='relevance', 
                publishedAfter=published_after, publishedBefore=published_before
            ).execute()

            candidate_ids = [item['id']['videoId'] for item in search_response.get('items', []) if item['id'].get('videoId')]
            if not candidate_ids:
                return []

            stats_response = youtube.videos().list(part='statistics,snippet', id=','.join(candidate_ids)).execute()
            
            channel_ids = {item['snippet']['channelId'] for item in stats_response.get('items', [])}
            channel_subs = {}
            if channel_ids:
                channels_response = youtube.channels().list(part='statistics', id=','.join(list(channel_ids))).execute()
                for ch in channels_response.get('items', []):
                    channel_subs[ch['id']] = int(ch.get('statistics', {}).get('subscriberCount', 0))

            valid_videos = []
            for item in stats_response.get('items', []):
                stats = item.get('statistics', {})
                snippet = item.get('snippet', {})
                view_count = int(stats.get('viewCount', 0))
                comment_count = int(stats.get('commentCount', 0))
                subs_count = channel_subs.get(snippet.get('channelId', ''), 0)
                
                title = snippet.get('title', '')
                description = snippet.get('description', '')
                tags = snippet.get('tags', [])
                
                title_desc_tags = title + " " + description + " " + " ".join(tags)
                has_exact_name = (actor_name in title_desc_tags)
                
                if has_exact_name and (view_count >= 100000 or subs_count >= 100000) and comment_count >= 10:
                    valid_videos.append({
                        'id': item['id'],
                        'title': title,
                        'comment_count': comment_count,
                        'published_year': target_year
                    })

            valid_videos.sort(key=lambda x: x['comment_count'], reverse=True)
            return valid_videos[:3]

        except HttpError as e:
            if e.resp.status in [403, 429]:
                CURRENT_KEY_INDEX += 1
            else:
                break
        except Exception:
            break
            
    return []

def get_comments_from_videos(videos, target_year):
    global CURRENT_KEY_INDEX
    all_comments = []
    
    for video in videos:
        if CURRENT_KEY_INDEX >= len(API_KEYS): break
        youtube = build('youtube', 'v3', developerKey=API_KEYS[CURRENT_KEY_INDEX])
        
        try:
            comments_for_video = []
            next_page_token = None
            
            while len(comments_for_video) < 1000:
                request = youtube.commentThreads().list(
                    videoId=video['id'], part='snippet', maxResults=100, 
                    order='relevance', pageToken=next_page_token
                )
                response = request.execute()
                
                for item in response.get('items', []):
                    comment_snippet = item['snippet']['topLevelComment']['snippet']
                    text = comment_snippet['textOriginal']
                    date_str = comment_snippet['publishedAt']
                    comment_year = int(date_str[:4])
                    
                    if comment_year == target_year and len(text) > 3 and "http" not in text:
                        comments_for_video.append(clean_text(text))
                
                next_page_token = response.get('nextPageToken')
                if not next_page_token:
                    break
                    
            all_comments.extend(comments_for_video)
            
        except HttpError as e:
            if e.resp.status in [403, 429]:
                CURRENT_KEY_INDEX += 1
            continue
            
    return all_comments

def run_hawk_analysis(target_file_path):
    if not Path(target_file_path).exists():
        print(f"❌ 분석 지시서({target_file_path})를 찾을 수 없습니다.")
        return

    with open(target_file_path, 'r', encoding='utf-8') as f:
        job_data = json.load(f)

    targets = job_data.get("targets", [])
    if not targets:
        print("❌ 분석할 대상이 없습니다.")
        return

    classifier = load_ai_model()

    for idx, target in enumerate(targets):
        actor_name = target["actor_name"]
        actor_id = target.get("actor_id", "코드없음")
        transition_year = target["target_year"]
        
        print(f"\n========================================")
        print(f"🎬 [{idx+1}/{len(targets)}] {actor_name}({actor_id}) 여론 분석 (기준 연도: {transition_year})")
        print(f"========================================")

        years_to_search = [transition_year - 3, transition_year - 2, transition_year - 1,
                           transition_year + 1, transition_year + 2, transition_year + 3]
        
        yearly_results = {}
        
        for yr in years_to_search:
            videos = search_videos_for_year(actor_name, yr)
            if not videos:
                print(f"   -> {yr}년: 유효한 영상이 없습니다. 스킵.")
                continue
                
            print(f"   -> {yr}년: 유효 영상 {len(videos)}개 발견. 댓글 수집 시작...")
            comments = get_comments_from_videos(videos, yr)
            print(f"   -> {yr}년: 유효 댓글 {len(comments)}개 수집 완료. AI 분석 중...")
            
            pos_count = 0
            neg_count = 0
            
            for comment in comments:
                try:
                    res = classifier(comment[:500])[0]
                    if res['score'] < 0.8: continue
                    
                    label = str(res['label']).lower()
                    if '1' in label or 'positive' in label: pos_count += 1
                    elif '0' in label or 'negative' in label: neg_count += 1
                except: pass
            
            yearly_results[str(yr)] = {"positive": pos_count, "negative": neg_count, "total_scanned": len(comments)}

        # 🌟 식별 가능한 actor_id를 파일 이름에 포함하여 동명이인 데이터 덮어쓰기 방지
        safe_id = actor_id if actor_id and actor_id != "코드없음" else "unknown"
        save_path = SENTIMENT_DIR / f"hawk_analysis_{actor_name}_{safe_id}.json"
        
        final_data = {
            "actor_name": actor_name,
            "actor_id": actor_id,
            "transition_year": transition_year,
            "last_updated": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "yearly_sentiment": yearly_results
        }
        
        with open(save_path, 'w', encoding='utf-8') as f:
            json.dump(final_data, f, ensure_ascii=False, indent=2)
            
        print(f"✅ {actor_name}({actor_id}) 분석 완료 및 저장됨.")

    print("\n🎉 모든 분석 지시서 처리가 완료되었습니다!")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--file", type=str, required=True, help="분석 지시서 JSON 파일 경로 (예: hawk_sentiment_targets.json)")
    args = parser.parse_args()
    run_hawk_analysis(args.file)
