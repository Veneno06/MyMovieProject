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

# GitHub Actions 환경변수에서 HAWK 전용 6개 키 로드
API_KEYS = []
for i in range(1, 7):
    val = os.environ.get(f"YOUTUBE_HAWK_API_KEY_{i}")
    if val and val.strip():
        API_KEYS.append(val.strip())

CURRENT_KEY_INDEX = 0
CLASSIFIER = None 

REQUIRED_KEYWORDS = ['배우', '영화', '예고편', '인터뷰', '무대인사', '리뷰', '예능', '연기']

def load_ai_model():
    global CLASSIFIER
    if CLASSIFIER is None:
        print("🤖 AI 모델 로딩 중... (KoELECTRA-small-v3-nsmc 적용)")
        CLASSIFIER = pipeline("sentiment-analysis", model="daekeun-ml/koelectra-small-v3-nsmc")
    return CLASSIFIER

def clean_text(text):
    text = re.sub(r'([ㅋㅎㅠㅜ]){3,}', r'\1\1', text)
    return text[:500]

def search_and_collect_for_period(actor_name, start_date, end_date):
    global CURRENT_KEY_INDEX
    if not API_KEYS:
        return []

    search_query = f"{actor_name} 영화 | {actor_name} 예고편 | {actor_name} 인터뷰 | {actor_name} 무대인사 | {actor_name} 리뷰 | {actor_name} 예능 | {actor_name} 연기"
    valid_videos_comments = []

    while CURRENT_KEY_INDEX < len(API_KEYS):
        current_key = API_KEYS[CURRENT_KEY_INDEX]
        youtube = build('youtube', 'v3', developerKey=current_key)
        
        try:
            # 1단계: 분기별 검색
            search_response = youtube.search().list(
                q=search_query, part='id', maxResults=15, type='video', order='relevance', 
                publishedAfter=start_date, publishedBefore=end_date
            ).execute()

            candidate_ids = [item['id']['videoId'] for item in search_response.get('items', []) if item['id'].get('videoId')]
            if not candidate_ids:
                return []

            # 2단계: 메타데이터 및 통계 확인
            stats_response = youtube.videos().list(part='statistics,snippet', id=','.join(candidate_ids)).execute()
            videos_info = stats_response.get('items', [])

            channel_ids = {item['snippet']['channelId'] for item in videos_info}
            channel_subs = {}
            if channel_ids:
                channels_response = youtube.channels().list(part='statistics', id=','.join(list(channel_ids))).execute()
                for ch in channels_response.get('items', []):
                    channel_subs[ch['id']] = int(ch.get('statistics', {}).get('subscriberCount', 0))

            target_videos = []
            for item in videos_info:
                stats = item.get('statistics', {})
                snippet = item.get('snippet', {})
                view_count = int(stats.get('viewCount', 0))
                comment_count = int(stats.get('commentCount', 0))
                subs_count = channel_subs.get(snippet.get('channelId', ''), 0)
                
                title = snippet.get('title', '')
                description = snippet.get('description', '')
                tags = snippet.get('tags', [])
                
                title_desc_tags = title + " " + description + " " + " ".join(tags)
                has_req_keyword_meta = any(k in title_desc_tags for k in REQUIRED_KEYWORDS)
                has_name_in_meta = (actor_name in title_desc_tags)
                
                # 3단계: 조회수/구독자 필터 및 상위 영상 추출
                if (view_count >= 100000 or subs_count >= 100000) and comment_count > 0:
                    target_videos.append({
                        'id': item['id'],
                        'title': title,
                        'has_name_in_meta': has_name_in_meta,
                        'has_req_keyword_meta': has_req_keyword_meta,
                        'comment_count': comment_count
                    })

            target_videos.sort(key=lambda x: x['comment_count'], reverse=True)
            target_videos = target_videos[:3] # 분기당 가장 반응이 좋은 영상 3개만 추출

            if not target_videos:
                return []

            # 4단계: 댓글 수집 및 교차 검증
            for video in target_videos:
                has_name_in_comments = False
                has_req_keyword_comments = False
                video_comments_temp = []
                next_page_token = None

                while len(video_comments_temp) < 1000:
                    try:
                        comment_response = youtube.commentThreads().list(
                            videoId=video['id'], part='snippet', maxResults=100, 
                            order='relevance', pageToken=next_page_token
                        ).execute()

                        for c_item in comment_response.get('items', []):
                            text = c_item['snippet']['topLevelComment']['snippet']['textOriginal']
                            if len(text) > 3 and "http" not in text:
                                video_comments_temp.append(clean_text(text))
                                if actor_name in text: has_name_in_comments = True
                                if any(k in text for k in REQUIRED_KEYWORDS): has_req_keyword_comments = True
                        
                        next_page_token = comment_response.get('nextPageToken')
                        if not next_page_token: break
                    except HttpError:
                        break # 댓글 수집 중 에러 발생 시 현재까지 수집된 것만 유지

                # 동명이인/노이즈 차단 (메타 또는 댓글 중 한 곳에는 반드시 있어야 함)
                if not video['has_name_in_meta'] and not has_name_in_comments: continue
                if not video['has_req_keyword_meta'] and not has_req_keyword_comments: continue

                valid_videos_comments.extend(video_comments_temp)

            return valid_videos_comments

        except HttpError as e:
            if e.resp.status in [403, 429]:
                CURRENT_KEY_INDEX += 1
            else:
                break
        except Exception:
            break
            
    return []

def run_hawk_analysis(target_file_path):
    if not Path(target_file_path).exists():
        print(f"❌ 분석 지시서({target_file_path})를 찾을 수 없습니다.")
        return

    with open(target_file_path, 'r', encoding='utf-8') as f:
        job_data = json.load(f)

    targets = job_data.get("targets", [])
    if not targets: return

    classifier = load_ai_model()
    
    # 1년 4분기 기준 날짜
    quarters = [
        ("Q1", "01-01T00:00:00Z", "03-31T23:59:59Z"),
        ("Q2", "04-01T00:00:00Z", "06-30T23:59:59Z"),
        ("Q3", "07-01T00:00:00Z", "09-30T23:59:59Z"),
        ("Q4", "10-01T00:00:00Z", "12-31T23:59:59Z")
    ]

    for idx, target in enumerate(targets):
        actor_name = target["actor_name"]
        actor_id = target.get("actor_id", "코드없음")
        transition_year = target["target_year"]
        
        print(f"\n========================================")
        print(f"🎬 [{idx+1}/{len(targets)}] {actor_name} 여론 분석 (기준: {transition_year}년 6월 30일)")
        print(f"   -> 수집 기간: {transition_year-3}년 1월 ~ {transition_year+3}년 12월 (총 7년)")
        
        timeline_results = {}
        
        # 전후 3년, 총 7년의 궤적 추적
        for yr in range(transition_year - 3, transition_year + 4):
            for q_name, q_start, q_end in quarters:
                period_label = f"{yr}-{q_name}"
                start_dt = f"{yr}-{q_start}"
                end_dt = f"{yr}-{q_end}"
                
                print(f"   -> [{period_label}] 구간 탐색 중...")
                comments = search_and_collect_for_period(actor_name, start_dt, end_dt)
                
                pos_count, neg_count = 0, 0
                for comment in comments:
                    try:
                        res = classifier(comment[:500])[0]
                        if res['score'] < 0.6: continue 
                        label = str(res['label']).lower()
                        if '1' in label or 'positive' in label: pos_count += 1
                        elif '0' in label or 'negative' in label: neg_count += 1
                    except: pass
                
                timeline_results[period_label] = {
                    "positive": pos_count, 
                    "negative": neg_count, 
                    "total_scanned": len(comments)
                }

        safe_id = actor_id if actor_id and actor_id != "코드없음" else "unknown"
        save_path = SENTIMENT_DIR / f"hawk_analysis_{actor_name}_{safe_id}.json"
        
        final_data = {
            "actor_name": actor_name,
            "actor_id": actor_id,
            "transition_year": transition_year,
            "last_updated": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "timeline": timeline_results
        }
        
        with open(save_path, 'w', encoding='utf-8') as f:
            json.dump(final_data, f, ensure_ascii=False, indent=2)
            
        print(f"✅ {actor_name} 연속 7년 분기별 분석 완료 및 저장됨.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--file", type=str, required=True)
    args = parser.parse_args()
    run_hawk_analysis(args.file)
