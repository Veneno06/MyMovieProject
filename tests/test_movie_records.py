import json
import tempfile
import unittest
from pathlib import Path
from scripts.movie_records import merge_sources, load_movie_records, preserve_enhanced_fields, update_audience_files

class MovieRecordsTests(unittest.TestCase):
    def test_nested_flat_audience_and_cast(self):
        a = {'movieCd': '1', 'actors': [{'peopleCd': 'b'}, {'peopleCd': 'a'}], 'audiAcc': 0, 'movieNm': 'A'}
        b = {'audiAcc': '1,250', 'movieInfoResult': {'movieInfo': {'movieCd': '1', 'actors': [{'peopleCd': 'c'}], 'movieNm': 'B'}}}
        result, audit = merge_sources([('z.json', b), ('a.json', a)])
        self.assertEqual(result['audiAcc'], 1250)
        self.assertEqual(result['actors'], a['actors'])
        self.assertEqual(audit['fields']['actors'], 'a.json')
        self.assertIn('movieNm', audit['conflicts'])
        self.assertEqual((result, audit), merge_sources([('a.json', a), ('z.json', b)]))

    def test_unknown_not_observed_zero(self):
        result, _ = merge_sources([('a', {'movieCd': '1', 'audiAcc': 'bad'})])
        self.assertIsNone(result['audiAcc'])

    def test_refetch_keeps_enrichment(self):
        old = {'movieCd': '1', 'audiAcc': 42, 'customNote': 'local', 'actors': [{'peopleCd':'a'}, {'peopleCd':'b'}]}
        fresh = {'movieInfoResult': {'movieInfo': {'movieCd': '1', 'movieNm': 'fresh'}}}
        result = preserve_enhanced_fields(fresh, old)
        self.assertEqual(result['movieInfoResult']['movieInfo']['audiAcc'], 42)
        self.assertEqual(result['movieInfoResult']['movieInfo']['customNote'], 'local')
        self.assertEqual(result['movieInfoResult']['movieInfo']['movieNm'], 'fresh')

    def test_loader_and_update_all_duplicates(self):
        with tempfile.TemporaryDirectory() as tmp:
            p = Path(tmp); (p/'2023').mkdir()
            (p/'1.json').write_text(json.dumps({'movieCd':'1','audiAcc':10}))
            (p/'2023'/'1.json').write_text(json.dumps({'movieInfoResult':{'movieInfo':{'movieCd':'1','audiAcc':20}}}))
            records, audit = load_movie_records(p)
            self.assertEqual(records[0]['audiAcc'],20)
            self.assertEqual(audit['duplicate_movie_count'],1)
            update_audience_files([p/'1.json',p/'2023'/'1.json'],15)
            records, _ = load_movie_records(p)
            self.assertEqual(records[0]['audiAcc'],20)
            for file in p.rglob('*.json'):
                raw=json.loads(file.read_text()); node=raw.get('movieInfoResult',{}).get('movieInfo',raw)
                self.assertEqual(node['audiAcc'],20)

    def test_both_indexes_share_merged_audience_and_cast(self):
        from unittest.mock import patch
        from scripts import reindex_search, build_indices, sync_people_stats
        with tempfile.TemporaryDirectory(prefix='renamed-project-') as tmp:
            root = Path(tmp); movies = root/'docs/data/movies'; movies.mkdir(parents=True)
            (movies/'2023').mkdir(); people = root/'docs/data/people'; people.mkdir()
            search = root/'docs/data/search'; search.mkdir()
            sparse = {'movieCd':'1','movieNm':'Test','openDt':'20230101','audiAcc':0,'actors':[{'peopleCd':'a','peopleNm':'A'}]}
            rich = dict(sparse, audiAcc=55, actors=[{'peopleCd':'b','peopleNm':'B'},{'peopleCd':'a','peopleNm':'A'}])
            (movies/'1.json').write_text(json.dumps(sparse))
            (movies/'2023/1.json').write_text(json.dumps({'movieInfoResult':{'movieInfo':rich}}))
            person={'peopleInfoResult':{'peopleInfo':{'filmos':[{'movieCd':'1','audiAcc':0}]}}}
            (people/'a.json').write_text(json.dumps(person))
            with patch.multiple(reindex_search, ROOT=root, DATA_DIR=movies, PEOPLE_DIR=people, OUTPUT_FILE=root/'docs/data/search_index.json'):
                reindex_search.reindex()
            with patch.multiple(build_indices, MOVIE_DIR=str(movies), SEARCH_DIR=str(search)):
                build_indices.main()
            with patch.multiple(sync_people_stats, MOVIE_DIR=movies, PEOPLE_DIR=people):
                sync_people_stats.sync_stats()
            flat=json.loads((root/'docs/data/search_index.json').read_text())[0]
            indexed=json.loads((search/'movies.json').read_text())['movies'][0]
            self.assertEqual(flat['audiAcc'],55)
            self.assertEqual(indexed['audiAcc'],55)
            self.assertEqual([a['name'] for a in flat['actors']],indexed['cast'])
            self.assertEqual(indexed['cast'],['B','A'])
            self.assertEqual(json.loads((people/'a.json').read_text())['peopleInfoResult']['peopleInfo']['filmos'][0]['audiAcc'],55)

if __name__ == '__main__': unittest.main()
