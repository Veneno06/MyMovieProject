# scripts/youtube_sentiment.py (개선판: 조회수 필터 적용)
import os
import json
import argparse
from datetime import datetime
from pathlib import Path
from googleapiclient.discovery import build
from transformers import pipeline
import sys

# 한글 출력 세팅
sys.stdout.reconfigure(encoding='utf-8')

# 파일 경로
HERE = Path(__file__).resolve()
ROOT = HERE.parents[1] if HERE.parents[1].name == "MyMovieProject" else HERE.parents[2]
SENTIMENT_DIR = ROOT / "docs" / "data" / "sentiment"
SENTIMENT_DIR.mkdir(parents=True, exist_ok=True)

YOUTUBE_API_KEY = os.environ.get("YOUTUBE_API_KEY")

def get_week_string(date_str):
    dt = datetime.strptime(date_str[:10], "%Y-%m-%d")
    year, week, _ = dt.isocalendar()
    return f"{year}-W{week:02d}"

def get_youtube_comments(actor_name, min_views=100000, max_videos_check=10, max_comments_per_video=100):
    """
    actor_name: 검색할 배우 이름
    min_views: 최소 조회수 (기본 10만)
    max_videos_check: 조회수 확인을 위해 검사할 후보 영상 개수 (기본 10개)
    """
    if not YOUTUBE_API_KEY:
        print("❌ [오류] YOUTUBE_API_KEY 없음")
        return []

    youtube = build('youtube', 'v3', developerKey=YOUTUBE_API_KEY)
    
    print(f"🔍 '{actor_name}' 관련 영상 검색 중... (조회수 {min_views}회 이상 필터링)")
    
    # 1. 검색 (검색어 범위를 넓힘: 이름만 검색)
    # type='video'로 영상만 검색
    search_response = youtube.search().list(
        q=actor_name, 
        part='id,snippet',
        maxResults=max_videos_check, 
        type='video',
        order='relevance' # 관련성 순 (조회수 순으로 하면 너무 옛날 영상만 나올 수 있어서 관련성 추천)
    ).execute()

    # 검색된 영상들의 ID 목록
    candidate_ids = [item['id']['videoId'] for item in search_response.get('items', [])]
    
    if not candidate_ids:
        print(" -> 검색 결과가 없습니다.")
        return []

    # 2. 영상 세부 정보(조회수) 조회
    # search()에서는 조회수가 안 나와서 videos()를 한 번 더 호출해야 함
    stats_response = youtube.videos().list(
        part='statistics,snippet',
        id=','.join(candidate_ids)
    ).execute()

    target_video_ids = []
    
    print(f" -> 후보 영상 {len(candidate_ids)}개 중 조회수 {min_views}회 이상 선별 중...")

    for item in stats_response.get('items', []):
        view_count = int(item['statistics'].get('viewCount', 0))
        video_title = item['snippet']['title']
        
        # [조건 체크] 조회수 10만 이상인가?
        if view_count >= min_views:
            print(f"  [O] 선택됨: {video_title} (조회수: {view_count:,}회)")
            target_video_ids.append(item['id'])
            # 너무 많이 수집하면 시간 걸리니 상위 3개만 수집
            if len(target_video_ids) >= 3: 
                break
        else:
            print(f"  [X] 제외됨: {video_title} (조회수: {view_count:,}회 - 기준 미달)")

    if not target_video_ids:
        print("⚠️ 조건(조회수 10만 이상)을 만족하는 영상이 없습니다.")
        return []

    all_comments = []

    # 3. 선별된 영상에서 댓글 수집
    for video_id in target_video_ids:
        try:
            comment_response = youtube.commentThreads().list(
                videoId=video_id,
                part='snippet',
                maxResults=max_comments_per_video,
                order='relevance'
            ).execute()

            for item in comment_response.get('items', []):
                snippet = item['snippet']['topLevelComment']['snippet']
                text = snippet['textOriginal']
                published_at = snippet['publishedAt']
                
                if len(text) > 5 and "http" not in text:
                    all_comments.append({
                        "text": text,
                        "date": published_at
                    })
        except Exception as e:
            print(f"⚠️ 영상({video_id}) 댓글 수집 불가: {e}")

    print(f"💬 총 {len(all_comments)}개의 유효한 댓글 수집 완료.")
    return all_comments

def analyze_sentiment(comments):
    if not comments: return {}

    print("🤖 AI 감성 분석 모델 로딩 중...")
    classifier = pipeline("sentiment-analysis", model="nlptown/bert-base-multilingual-uncased-sentiment")
    
    timeline_data = {}

    print("🧠 댓글 분석 시작...")
    for idx, comment in enumerate(comments):
        if idx > 0 and idx % 50 == 0: print(f" ... {idx}/{len(comments)} 완료")
            
        try:
            text = comment["text"][:500]
            result = classifier(text)[0]
            label = result['label']
            
            if "1 star" in label or "2 stars" in label: sentiment = "negative"
            elif "4 stars" in label or "5 stars" in label: sentiment = "positive"
            else: continue 

            week_str = get_week_string(comment["date"])
            if week_str not in timeline_data: timeline_data[week_str] = {"positive": 0, "negative": 0}
            timeline_data[week_str][sentiment] += 1
            
        except: continue

    return timeline_data

def run(actor_name):
    print(f"=== [{actor_name}] 유튜브 여론 분석 (조회수 10만+ 필터) ===")
    
    comments = get_youtube_comments(actor_name)
    timeline_data = analyze_sentiment(comments)
    
    if not timeline_data:
        print(f"❌ 데이터 부족으로 저장 생략.")
        return

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
        
    print(f"✅ 저장 완료: {save_path.name}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--actor", type=str, required=True)
    args = parser.parse_args()
    run(args.actor)
