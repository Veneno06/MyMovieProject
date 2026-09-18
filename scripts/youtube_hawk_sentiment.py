# scripts/youtube_hawk_sentiment.py
import os
import json
import argparse
from datetime import datetime, timezone
from pathlib import Path
import sys
import time
import re

if hasattr(sys.stdout, 'reconfigure'): sys.stdout.reconfigure(encoding='utf-8')

ROOT = Path(__file__).resolve().parents[1]
SENTIMENT_DIR = ROOT / "docs" / "data" / "sentiment"
SENTIMENT_DIR.mkdir(parents=True, exist_ok=True)

API_KEYS = []
for i in range(1, 7):
    val = os.environ.get(f"YOUTUBE_HAWK_API_KEY_{i}")
    if val and val.strip():
        API_KEYS.append(val.strip())

CURRENT_KEY_INDEX = 0
CLASSIFIER = None 

# 🌟 영화 배우 관련 키워드 10개로 확장 및 고정
REQUIRED_KEYWORDS = ['배우', '영화', '예고편', '인터뷰', '무대인사', '리뷰', '예능', '연기', '작품', '캐스팅']

def load_ai_model():
    global CLASSIFIER
    if CLASSIFIER is None:
        print("🤖 AI 모델 로딩 중... (KoELECTRA-small-v3-nsmc 적용)")
        from transformers import pipeline
        CLASSIFIER = pipeline("sentiment-analysis", model="daekeun-ml/koelectra-small-v3-nsmc")
    return CLASSIFIER

def clean_text(text):
    text = re.sub(r'([ㅋㅎㅠㅜ]){3,}', r'\1\1', text)
    return text[:500]

def search_and_collect_for_period(actor_name, start_date, end_date):
    from googleapiclient.discovery import build
    from googleapiclient.errors import HttpError
    global CURRENT_KEY_INDEX
    if not API_KEYS:
        return [], {}

    search_query = f"{actor_name} 영화 | {actor_name} 예고편 | {actor_name} 인터뷰 | {actor_name} 무대인사 | {actor_name} 리뷰 | {actor_name} 예능 | {actor_name} 연기"
    valid_videos_comments = []
    sources_dict = {}

    while CURRENT_KEY_INDEX < len(API_KEYS):
        current_key = API_KEYS[CURRENT_KEY_INDEX]
        youtube = build('youtube', 'v3', developerKey=current_key)
        
        try:
            search_response = youtube.search().list(
                q=search_query, part='id', maxResults=15, type='video', order='relevance', 
                publishedAfter=start_date, publishedBefore=end_date
            ).execute()

            candidate_ids = [item['id']['videoId'] for item in search_response.get('items', []) if item['id'].get('videoId')]
            if not candidate_ids:
                return [], {}

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
                
                # 제목과 태그만 따로 결합하여 배우 이름 존재 여부 엄격 검사
                title_and_tags = title + " " + " ".join(tags)
                has_name_in_title_or_tags = (actor_name in title_and_tags)
                
                title_desc_tags = title + " " + description + " " + " ".join(tags)
                has_req_keyword_meta = any(k in title_desc_tags for k in REQUIRED_KEYWORDS)
                
                # 구독자 또는 조회수 5만 이상으로 조건 하향, 제목/태그에 이름 필수 포함
                if (view_count >= 50000 or subs_count >= 50000) and comment_count > 0:
                    if not has_name_in_title_or_tags:
                        continue 
                        
                    target_videos.append({
                        'id': item['id'],
                        'title': title,
                        'channelTitle': snippet.get('channelTitle', '채널명 없음'),
                        'publishedAt': snippet.get('publishedAt', ''),
                        'has_req_keyword_meta': has_req_keyword_meta,
                        'comment_count': comment_count
                    })

            target_videos.sort(key=lambda x: x['comment_count'], reverse=True)
            target_videos = target_videos[:3] 

            if not target_videos:
                return [], {}

            for video in target_videos:
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
                                video_comments_temp.append({
                                    "text": clean_text(text),
                                    "videoId": video['id'],
                                    "date": c_item["snippet"]["topLevelComment"]["snippet"].get("publishedAt"),
                                    "comment_id": c_item["snippet"]["topLevelComment"].get("id", c_item.get("id"))
                                })
                                if any(k in text for k in REQUIRED_KEYWORDS): has_req_keyword_comments = True
                        
                        next_page_token = comment_response.get('nextPageToken')
                        if not next_page_token: break
                    except HttpError as error:
                        if error.resp.status in (403, 429): raise
                        break 

                if not video['has_req_keyword_meta'] and not has_req_keyword_comments: continue

                print(f"      🎯 [수집 확정] '{video['title'][:30]}...' (댓글 {len(video_comments_temp)}개 확보)")

                sources_dict[video['id']] = {
                    "videoId": video['id'],
                    "title": video['title'],
                    "channel": video['channelTitle'],
                    "publishedAt": video['publishedAt']
                }

                valid_videos_comments.extend(video_comments_temp)

            return valid_videos_comments, sources_dict

        except HttpError as e:
            if e.resp.status in [403, 429]:
                print(f"   ⚠️ API Key {CURRENT_KEY_INDEX + 1} 할당량 초과! 🔄 다음 키({CURRENT_KEY_INDEX + 2}번)로 교체합니다...")
                CURRENT_KEY_INDEX += 1
            else:
                break
        except Exception:
            break
            
    return [], {}

def parse_timestamp(value):
    dt = datetime.fromisoformat(str(value).replace('Z', '+00:00'))
    return dt.replace(tzinfo=timezone.utc) if dt.tzinfo is None else dt.astimezone(timezone.utc)


def aggregate_comments(comments, classifier, window_start, window_end):
    """Actual comment timestamps; half-open UTC window, with retained audit rows."""
    start, end = parse_timestamp(window_start), parse_timestamp(window_end)
    timeline, videos, observations, seen = {}, {}, [], set()
    for comment in comments:
        try:
            dt = parse_timestamp(comment.get('date'))
            if not start <= dt < end: continue
            key = comment.get('comment_id') or (comment.get('videoId'), comment.get('date'), comment['text'])
            if key in seen: continue
            seen.add(key)
            res = classifier(clean_text(comment['text']))[0]
            if res['score'] < .6: continue
            label = str(res['label']).lower()
            sentiment = 'positive' if ('1' in label or 'positive' in label) else ('negative' if ('0' in label or 'negative' in label) else None)
            if sentiment is None: continue
            year, week, _ = dt.isocalendar()
            bucket = timeline.setdefault(f'{year}-W{week:02d}', {'positive':0, 'negative':0})
            bucket[sentiment] += 1
            vid = comment.get('videoId')
            videos.setdefault(vid, {'positive':0, 'negative':0})[sentiment] += 1
            observations.append({'comment_id': comment.get('comment_id'), 'videoId':vid,
                                 'publishedAt':comment['date'], 'sentiment':sentiment})
        except (ValueError, TypeError, KeyError):
            continue
    return timeline, videos, observations


def run_hawk_analysis(target_file_path):
    from youtube_sentiment import resolve_actor_id
    job = json.loads(Path(target_file_path).read_text(encoding='utf-8'))
    for target in job.get('targets', []):
        name = target['actor_name']
        actor_id = resolve_actor_id(name, actor_id=target.get('actor_id'))
        if not actor_id:
            print(f'⚠️ ID 미확정/동명이인: {name}; 생략')
            continue
        year = int(target['target_year'])
        start = target.get('window_start', f'{year-3}-01-01')
        end = target.get('window_end', f'{year+4}-01-01')
        if parse_timestamp(start) >= parse_timestamp(end): raise ValueError('Invalid half-open window')
        event = target.get('event_date')
        if event: parse_timestamp(event)
        path = SENTIMENT_DIR / f'hawk_analysis_{actor_id}.json'
        if path.exists():
            existing = json.loads(path.read_text(encoding='utf-8'))
            if (existing.get('schema_version') == 2 and existing.get('date_basis') == 'comment_published_at'
                and existing.get('window') == {'start':start,'end_exclusive':end}
                and existing.get('event_date') == event and existing.get('collection_status') == 'sampled'):
                continue
        comments, sources = [], {}
        # Search video cohorts through the comment-window end. Earlier videos can have in-window comments.
        for video_year in range(2005, parse_timestamp(end).year + 1):
            for month in (1,4,7,10):
                begin = datetime(video_year, month, 1, tzinfo=timezone.utc)
                finish = datetime(video_year + (month == 10), 1 if month == 10 else month+3, 1, tzinfo=timezone.utc)
                if begin >= parse_timestamp(end): continue
                if CURRENT_KEY_INDEX >= len(API_KEYS):
                    print('API 키 없음/소진; 불완전한 결과 저장 생략')
                    return
                rows, src = search_and_collect_for_period(name, begin.isoformat().replace('+00:00','Z'), min(finish,parse_timestamp(end)).isoformat().replace('+00:00','Z'))
                comments.extend(rows); sources.update(src)
        if CURRENT_KEY_INDEX >= len(API_KEYS): return
        timeline, videos, observations = aggregate_comments(comments, load_ai_model(), start, end)
        final_sources = [{**sources[vid], 'pos_count':counts['positive'], 'neg_count':counts['negative']}
                         for vid, counts in videos.items() if vid in sources]
        data = {'schema_version':2, 'actor_name':name, 'actor_id':actor_id,
                'date_basis':'comment_published_at', 'time_unit':'iso_week',
                'transition_year':year, 'event_date':event,
                'event_date_status':'exact' if event else 'unknown_year_only',
                'window':{'start':start,'end_exclusive':end},
                'collection_status':'sampled',
                'sampling_note':'Relevance-ranked videos/comments; top-level comments only; missing weeks are unobserved, not zero. Not historical as-of availability.',
                'last_updated':datetime.now(timezone.utc).isoformat(),
                'timeline':timeline,'observations':observations,'sources':final_sources}
        path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding='utf-8')

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--file', required=True)
    run_hawk_analysis(parser.parse_args().file)
