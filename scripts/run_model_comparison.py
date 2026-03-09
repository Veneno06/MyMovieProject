# scripts/run_model_comparison.py
import os
import json
import time
import argparse
from datetime import datetime
from pathlib import Path
from googleapiclient.discovery import build
from transformers import pipeline
import re

ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT / "docs" / "data"
DATA_DIR.mkdir(parents=True, exist_ok=True)
OUT_FILE = DATA_DIR / "model_comparison.json"

API_KEYS = [k for k in [os.environ.get("YOUTUBE_API_KEY"), os.environ.get("YOUTUBE_API_KEY_2")] if k]
CURRENT_KEY_INDEX = 0

# 비교할 5가지 모델 리스트
MODELS_TO_TEST = [
    {"id": "nlptown/bert-base-multilingual-uncased-sentiment", "name": "기존 다국어 모델 (NLPTown)"},
    {"id": "sangrimlee/bert-base-multilingual-cased-nsmc", "name": "네이버 영화 리뷰 모델 (현재 사용)"},
    {"id": "daigo/kcbert-base-nsmc", "name": "구어체 특화 (KcBERT)"},
    {"id": "monologg/koelectra-small-v2-nsmc", "name": "경량화/고속 특화 (KoELECTRA)"},
    {"id": "Whitezza/korean-sentiment-analysis", "name": "한국어 특화 (RoBERTa)"}
]

def clean_text(text):
    text = re.sub(r'([ㅋㅎㅠㅜ]){3,}', r'\1\1', text)
    return text[:500]

def get_youtube_comments(actor_name):
    global CURRENT_KEY_INDEX
    if not API_KEYS: return []
    
    all_comments = []
    print(f"\n🎥 '{actor_name}' 댓글 수집 시작 (최근 5년간 상위 3개 영상 기준)...")
    
    for year in range(datetime.now().year - 4, datetime.now().year + 1):
        if CURRENT_KEY_INDEX >= len(API_KEYS): break
        
        try:
            youtube = build('youtube', 'v3', developerKey=API_KEYS[CURRENT_KEY_INDEX])
            search_res = youtube.search().list(
                q=actor_name, part='id', maxResults=5, type='video', order='relevance',
                publishedAfter=f"{year}-01-01T00:00:00Z", publishedBefore=f"{year}-12-31T23:59:59Z"
            ).execute()
            
            c_ids = [item['id']['videoId'] for item in search_res.get('items', [])]
            if not c_ids: continue
            
            stats_res = youtube.videos().list(part='statistics,snippet', id=','.join(c_ids)).execute()
            videos = [i for i in stats_res.get('items', []) if int(i.get('statistics', {}).get('viewCount', 0)) >= 5000]
            videos.sort(key=lambda x: int(x['statistics'].get('commentCount', 0)), reverse=True)
            
            for video in videos[:3]:
                try:
                    c_res = youtube.commentThreads().list(videoId=video['id'], part='snippet', maxResults=100, order='relevance').execute()
                    for item in c_res.get('items', []):
                        txt = item['snippet']['topLevelComment']['snippet']['textOriginal']
                        if len(txt) > 3 and "http" not in txt:
                            all_comments.append(clean_text(txt))
                except: pass
        except Exception as e:
            if "quotaExceeded" in str(e): CURRENT_KEY_INDEX += 1
            pass
            
    print(f"✅ 총 {len(all_comments)}개의 테스트용 댓글 확보 완료.")
    return all_comments

def parse_label(label, score):
    # 60% 미만 확신은 무조건 탈락 (중립)
    if score < 0.6: return "dropped"
    
    lbl = str(label).lower()
    if 'star' in lbl:
        if '4' in lbl or '5' in lbl: return "positive"
        if '1' in lbl or '2' in lbl: return "negative"
        return "dropped"
    else:
        if 'positive' in lbl or 'label_1' in lbl: return "positive"
        if 'negative' in lbl or 'label_0' in lbl: return "negative"
        return "dropped"

def run_comparison(actors):
    results = {}
    
    for actor in actors:
        comments = get_youtube_comments(actor)
        total_input = len(comments)
        if total_input == 0: continue
        
        actor_result = {"total_input": total_input, "models": []}
        
        # 5개 모델 순차적 로드 및 테스트
        for m_info in MODELS_TO_TEST:
            print(f"\n🤖 [{actor}] 모델 테스트 중: {m_info['name']}")
            try:
                start_time = time.time()
                classifier = pipeline("sentiment-analysis", model=m_info["id"])
                
                pos_count = 0
                neg_count = 0
                dropped_count = 0
                
                for c in comments:
                    res = classifier(c)[0]
                    parsed = parse_label(res['label'], res['score'])
                    if parsed == "positive": pos_count += 1
                    elif parsed == "negative": neg_count += 1
                    else: dropped_count += 1
                        
                end_time = time.time()
                elapsed = round(end_time - start_time, 2)
                
                actor_result["models"].append({
                    "model_id": m_info["id"],
                    "model_name": m_info["name"],
                    "positive": pos_count,
                    "negative": neg_count,
                    "dropped": dropped_count,
                    "yield_rate": round(((pos_count + neg_count) / total_input) * 100, 1),
                    "time_sec": elapsed
                })
                print(f"  -> 결과: 긍정 {pos_count} / 부정 {neg_count} / 탈락 {dropped_count} (소요시간: {elapsed}초)")
            except Exception as e:
                print(f"  -> 모델 로드/실행 실패: {e}")
                
        results[actor] = actor_result

    # 저장
    with open(OUT_FILE, 'w', encoding='utf-8') as f:
        json.dump(results, f, ensure_ascii=False, indent=2)
    print("\n🎉 모든 모델 비교 및 JSON 저장 완료!")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--actors", type=str, required=True, help="테스트할 배우 이름들 (쉼표 구분)")
    args = parser.parse_args()
    
    actor_list = [x.strip() for x in args.actors.split(',') if x.strip()]
    run_comparison(actor_list)
