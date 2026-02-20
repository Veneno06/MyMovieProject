# scripts/youtube_sentiment.py
import os
import json
import argparse
from datetime import datetime
from pathlib import Path
from googleapiclient.discovery import build
from transformers import pipeline

# 한글 출력을 위한 세팅
import sys
sys.stdout.reconfigure(encoding='utf-8')

# 파일 저장 경로 설정
HERE = Path(__file__).resolve()
ROOT = HERE.parents[1] if HERE.parents[1].name == "MyMovieProject" else HERE.parents[2]
SENTIMENT_DIR = ROOT / "docs" / "data" / "sentiment"
SENTIMENT_DIR.mkdir(parents=True, exist_ok=True)

# 유튜브 API 키
YOUTUBE_API_KEY = os.environ.get("YOUTUBE_API_KEY")

# 날짜를 주차(Week)로 변환하는 함수 (예: 2023-W42)
def get_week_string(date_str):
    # 날짜 형식: 2023-10-24T08:00:00Z
    dt = datetime.strptime(date_str[:10], "%Y-%m-%d")
    year, week, _ = dt.isocalendar()
    return f"{year}-W{week:02d}"

def get_youtube_comments(actor_name, max_videos=3, max_comments_per_video=100):
    if not YOUTUBE_API_KEY:
        print("❌ [오류] YOUTUBE_API_KEY가 설정되지 않았습니다.")
        return []

    youtube = build('youtube', 'v3', developerKey=YOUTUBE_API_KEY)
    
    print(f"🔍 '{actor_name}' 유튜브 영상 검색 중...")
    
    # 1. 영상 검색 (배우명 + 인터뷰/무대인사/공식)
    search_query = f"{actor_name} 인터뷰 OR {actor_name} 무대인사"
    search_response = youtube.search().list(
        q=search_query,
        part='id,snippet',
        maxResults=max_videos,
        type='video',
        order='relevance'
    ).execute()

    video_ids = [item['id']['videoId'] for item in search_response.get('items', [])]
    print(f"🎬 {len(video_ids)}개의 관련 영상을 찾았습니다.")

    all_comments = []

    # 2. 각 영상에서 댓글 수집
    for video_id in video_ids:
        try:
            comment_response = youtube.commentThreads().list(
                videoId=video_id,
                part='snippet',
                maxResults=max_comments_per_video,
                order='relevance' # 인기 댓글 위주로 수집
            ).execute()

            for item in comment_response.get('items', []):
                snippet = item['snippet']['topLevelComment']['snippet']
                text = snippet['textOriginal']
                published_at = snippet['publishedAt']
                
                # 너무 짧은 댓글이나 링크 제외
                if len(text) > 5 and "http" not in text:
                    all_comments.append({
                        "text": text,
                        "date": published_at
                    })
        except Exception as e:
            print(f"⚠️ 영상({video_id}) 댓글 수집 불가 (댓글 사용 중지 등): {e}")

    print(f"💬 총 {len(all_comments)}개의 유효한 댓글을 수집했습니다.")
    return all_comments

def analyze_sentiment(comments, actor_name):
    if not comments:
        return {}

    print("🤖 AI 감성 분석 모델 로딩 중... (약 1~2분 소요될 수 있습니다)")
    # 가볍고 성능 좋은 다국어(한국어 포함) 감성 분석 모델 사용
    # 1~2점: 부정, 4~5점: 긍정, 3점: 중립으로 처리
    classifier = pipeline("sentiment-analysis", model="nlptown/bert-base-multilingual-uncased-sentiment")
    
    timeline_data = {}

    print("🧠 수집된 댓글 분석 중...")
    for idx, comment in enumerate(comments):
        if idx > 0 and idx % 50 == 0:
            print(f" ... {idx}/{len(comments)}개 분석 완료")
            
        try:
            # 텍스트 길이 제한 (AI 모델 최대 입력 길이 방지)
            text = comment["text"][:500]
            result = classifier(text)[0]
            label = result['label'] # 예: '1 star', '5 stars'
            
            # 별점 기반 긍정/부정 판단
            if "1 star" in label or "2 stars" in label:
                sentiment = "negative"
            elif "4 stars" in label or "5 stars" in label:
                sentiment = "positive"
            else:
                continue # 3별(중립)은 차트 대비가 모호해지므로 패스 (선택사항)

            week_str = get_week_string(comment["date"])
            
            if week_str not in timeline_data:
                timeline_data[week_str] = {"positive": 0, "negative": 0}
            
            timeline_data[week_str][sentiment] += 1
            
        except Exception as e:
            continue

    return timeline_data

def run(actor_name):
    print(f"=== [{actor_name}] 유튜브 여론 AI 분석 시작 ===")
    
    # 1. 댓글 수집
    comments = get_youtube_comments(actor_name)
    
    # 2. AI 감성 분석
    timeline_data = analyze_sentiment(comments, actor_name)
    
    if not timeline_data:
        print(f"❌ 분석할 데이터가 부족하여 저장하지 않습니다.")
        return

    # 3. JSON 파일로 저장
    save_path = SENTIMENT_DIR / f"{actor_name}.json"
    
    # 기존 데이터가 있으면 불러와서 병합 (데이터 누적용)
    if save_path.exists():
        with open(save_path, 'r', encoding='utf-8') as f:
            existing_data = json.load(f).get("timeline", {})
            
        for week, counts in timeline_data.items():
            if week not in existing_data:
                existing_data[week] = {"positive": 0, "negative": 0}
            existing_data[week]["positive"] += counts["positive"]
            existing_data[week]["negative"] += counts["negative"]
        
        timeline_data = existing_data

    # 최종 저장할 데이터 포맷
    final_data = {
        "actor_name": actor_name,
        "last_updated": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "timeline": timeline_data
    }

    with open(save_path, 'w', encoding='utf-8') as f:
        json.dump(final_data, f, ensure_ascii=False, indent=2)
        
    print(f"✅ 분석 완료! 파일 저장됨: {save_path.name}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--actor", type=str, required=True, help="분석할 배우 이름 (예: 마동석)")
    args = parser.parse_args()
    run(args.actor)
