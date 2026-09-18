import sys
import tempfile
import json
import unittest
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'scripts'))
from build_db_summary import period_start, select_sentiment_datasets, summarize_timeline
from youtube_hawk_sentiment import aggregate_comments
from youtube_sentiment import resolve_actor_id

class SentimentIntegrity(unittest.TestCase):
    def test_comment_time_window_and_iso_year(self):
        rows = [{'date': '2021-01-01T12:00:00Z', 'text': 'ok', 'videoId': 'v'},
                {'date': '2026-01-01T00:00:00Z', 'text': 'future', 'videoId': 'v'}]
        timeline, videos, observations = aggregate_comments(rows, lambda _: [{'label':'negative','score':.9}], '2020-01-01', '2022-01-01')
        self.assertEqual(timeline, {'2020-W53': {'positive':0,'negative':1}})
        self.assertEqual(len(observations), 1)
        self.assertEqual(observations[0]['publishedAt'], rows[0]['date'])
    def test_name_collision(self):
        movies = [{'actors':[{'id':'1','name':'Same'},{'id':'2','name':'Same'}]}]
        self.assertIsNone(resolve_actor_id('Same', movies))
        self.assertEqual(resolve_actor_id('Same', movies, '2'), '2')
    def test_duplicate_and_legacy_quarantine(self):
        rank = [{'id':'1','name':'A','score':1}]
        with tempfile.TemporaryDirectory() as td:
            p = Path(td)
            for name, data in [('A', {'actor_name':'A','timeline':{'2020-W01':{'positive':1,'negative':2}}}),
                ('1', {'actor_name':'A','actor_id':'1','date_basis':'comment_published_at','schema_version':2,'timeline':{'2020-W01':{'positive':2,'negative':3}}}),
                ('hawk_analysis_A_1', {'actor_id':'1','actor_name':'A','timeline':{'2020-Q1':{'negative':100}}})]:
                (p / (name+'.json')).write_text(json.dumps(data))
            selected, report = select_sentiment_datasets(p, rank)
            self.assertEqual(len(selected),1)
            self.assertEqual(selected[0][1]['timeline']['2020-W01']['negative'],3)
            self.assertEqual(report['legacy_video_quarter'],1)
            self.assertEqual(report['duplicate_actor_dataset'],1)
    def test_collision_outside_rankings_and_hawk_window_separation(self):
        ranks = [{'id':'1','name':'Same','score':1}]
        movies = [{'actors':[{'id':'1','name':'Same'},{'id':'2','name':'Same'}]}]
        with tempfile.TemporaryDirectory() as td:
            p = Path(td)
            (p/'Same.json').write_text(json.dumps({'actor_name':'Same','timeline':{'2020-W01':{'negative':3}}}))
            (p/'hawk_analysis_1.json').write_text(json.dumps({'actor_name':'Same','actor_id':'1','schema_version':2,'date_basis':'comment_published_at','timeline':{'2020-W01':{'negative':3}}}))
            selected, report = select_sentiment_datasets(p, ranks, movies)
            self.assertEqual(selected, [])
            self.assertEqual(report['ambiguous_or_unknown_name'],1)
            self.assertEqual(report['hawk_event_sample_separate'],1)

    def test_signed_slope_and_real_week_date(self):
        result = summarize_timeline({'2020-W01':{'negative':10},'2020-W03':{'negative':2}})
        self.assertEqual(result['maxSlope'],4)
        self.assertEqual(result['peakSignedSlope'],-4)
        self.assertEqual(result['peakDate'],'20200113')
        self.assertEqual(period_start('2020-Q2').isoformat(),'2020-04-01')
        self.assertIsNone(period_start('2021-W53'))

if __name__ == '__main__': unittest.main()
