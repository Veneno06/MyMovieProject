"""Behavioral checks for the fixed, score-independent second-release endpoint."""
import copy
import importlib.util
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest


SCRIPT = Path(__file__).resolve().parents[1] / 'scripts/analyze_recovery_followup.py'
MODULE = None
if SCRIPT.exists():
    SPEC = importlib.util.spec_from_file_location('recovery_followup', SCRIPT)
    MODULE = importlib.util.module_from_spec(SPEC)
    SPEC.loader.exec_module(MODULE)


def row(actor, movie, day, score):
    return dict(id=actor, name=actor, movie=movie, title=movie, date=day, score=score)


def fixture():
    return {
        'config': {'cutoffDate': '20201231', 'percent': 8.329547800949825, 'lambda': 12},
        'observations': [
            row('A', 'solo', '20180101', 100),
            row('A', 'joint', '20190101', 100), row('B', 'joint', '20190101', 20),
            row('B', 'without', '20190201', 10),
            row('A', 'event', '20200215', 1000), row('B', 'event', '20200215', 1000),
            row('A', 'firsta', '20200301', 40), row('B', 'firsta', '20200301', 10),
            row('A', 'firstb', '20200301', 60), row('C', 'firstb', '20200301', 10),
            row('A', 'second', '20200401', 75), row('B', 'second', '20200401', 10),
            row('A', 'later', '20200501', 150),
            row('A', 'future', '20210101', 300),
        ],
        'annual': [dict(id='A', year=y, usable=True, high=True) for y in (2018, 2019, 2020)],
        'events': [dict(id='A', name='A', eventMonth='2020-02', pauseStatus='documented', sources=[])],
    }


class RecoveryFollowupTests(unittest.TestCase):
    def setUp(self):
        self.assertIsNotNone(MODULE, 'The fixed second-release analysis is not implemented')

    def test_distinct_days_same_day_mean_and_union_without_recovery_search(self):
        result = MODULE.analyze(fixture())
        case = result['cases'][0]
        self.assertEqual(case['preN'], 2)
        self.assertEqual(case['preMean'], 100)
        self.assertEqual(case['first']['date'], '20200301')
        self.assertEqual(case['first']['score'], 50)
        self.assertEqual(case['first']['coactorCount'], 2)
        self.assertEqual(len(case['first']['movies']), 2)
        self.assertEqual(case['second']['date'], '20200401')
        self.assertEqual([p['score'] for p in case['trajectory']], [50, 75, 150])

    def test_partial_rise_does_not_imply_above_baseline(self):
        case = MODULE.analyze(fixture())['cases'][0]
        self.assertEqual(case['delta'], 25)
        self.assertEqual(case['normalizedDelta'], .25)
        self.assertEqual(case['ratioToPre'], .75)
        self.assertEqual(case['ratioToFirst'], 1.5)
        self.assertTrue(case['improved'])
        self.assertFalse(case['aboveBaseline'])
        self.assertTrue(case['partialRise'])

    def test_pre_event_pool_stays_frozen_through_later_releases(self):
        case = MODULE.analyze(fixture())['cases'][0]
        for point in case['trajectory'][:2]:
            forward = point['synergy']['forward']
            self.assertEqual((forward['M'], forward['K'], forward['n'], forward['x']), (1, 1, 1, 1))
            self.assertEqual(forward['matches'][0]['value'], 100)
            self.assertEqual(forward['matches'][0]['joint'], 1)
        changed = fixture()
        changed['observations'][4]['score'] = 99999
        changed['observations'][5]['score'] = 99999
        changed_case = MODULE.analyze(changed)['cases'][0]
        self.assertEqual(case['second']['synergy'], changed_case['second']['synergy'])

    def test_reverse_computability_does_not_require_partner_without_actor(self):
        data = fixture()
        data['observations'] = [o for o in data['observations'] if o['movie'] != 'without']
        data['observations'][1]['score'] = 200
        first = MODULE.analyze(data)['cases'][0]['first']
        self.assertEqual(first['synergy']['forward']['M'], 0)
        reverse = first['synergy']['reverse']
        self.assertEqual((reverse['M'], reverse['K'], reverse['x']), (1, 1, 1))
        self.assertEqual(reverse['matches'][0]['value'], 100)

    def test_top_quartile_ceiling_includes_cutoff_ties_and_only_positive(self):
        pool = [dict(id=str(i), name=str(i), value=v, joint=1, without=1)
                for i, v in enumerate([100, 80, 80, 20, -10])]
        met = MODULE.metrics(pool, {'0', '1', '2', '4'}, 25)
        self.assertEqual((met['M'], met['K'], met['n'], met['x']), (5, 3, 4, 3))
        nonpositive = [dict(id='z', name='z', value=0, joint=1, without=1)]
        self.assertEqual(MODULE.metrics(nonpositive, {'z'}, 25)['K'], 0)

    def test_missing_second_and_missing_first_are_censored_not_zero(self):
        data = fixture()
        data['observations'] = [o for o in data['observations'] if o['date'] <= '20200301']
        case = MODULE.analyze(data)['cases'][0]
        self.assertIsNone(case['second'])
        for field in ('delta', 'normalizedDelta', 'ratioToPre', 'ratioToFirst', 'improved', 'aboveBaseline', 'partialRise'):
            self.assertIsNone(case[field], field)
        data['observations'] = [o for o in data['observations'] if o['date'] < '20200301']
        result = MODULE.analyze(data)
        self.assertEqual(len(result['cases']), 1)
        self.assertIsNone(result['cases'][0]['first'])
        self.assertEqual(result['counts']['censoredBeforeFirst'], 1)
        self.assertEqual(result['counts']['censoredBeforeSecond'], 1)

    def test_event_year_high_and_unusable_year_do_not_establish_history(self):
        data = fixture()
        data['annual'][1]['usable'] = False
        result = MODULE.analyze(data)
        self.assertEqual(result['cases'], [])
        self.assertFalse(result['registry'][0]['eligible'])
        self.assertEqual(result['registry'][0]['preHighYears'], [2018])
        data = fixture()
        data['events'][0]['pauseStatus'] = 'unresolved'
        self.assertFalse(MODULE.analyze(data)['registry'][0]['eligible'])

    def test_duplicate_actor_film_records_fail_audit(self):
        data = fixture()
        data['observations'].append(copy.deepcopy(data['observations'][0]))
        self.assertFalse(MODULE.analyze(data)['audit']['passed'])

    def test_snapshot_film_denominator_includes_films_without_identified_observations(self):
        data = fixture()
        data['counts'] = {'eligibleFilms': 12}
        counts = MODULE.analyze(data)['counts']
        self.assertEqual(counts['eligibleFilms'], 12)
        self.assertEqual(counts['filmsWithIdentifiedObservations'], 8)

    def test_cli_writes_readable_result_and_does_not_mutate_input(self):
        with tempfile.TemporaryDirectory() as tmp:
            source = Path(tmp) / 'input.json'
            output = Path(tmp) / 'output.json'
            content = json.dumps(fixture())
            source.write_text(content)
            completed = subprocess.run([sys.executable, str(SCRIPT), str(source), str(output)],
                                       capture_output=True, text=True)
            self.assertEqual(completed.returncode, 0, completed.stderr)
            self.assertTrue(json.loads(output.read_text())['audit']['passed'])
            self.assertEqual(source.read_text(), content)


if __name__ == '__main__':
    unittest.main()
