"""Known Kneedle reference, no-knee behavior, and exact JS lambda reproduction."""
import importlib.util
import math
import tempfile
import unittest
import json
from pathlib import Path

ROOT=Path(__file__).resolve().parents[1]
spec=importlib.util.spec_from_file_location('validate_research',ROOT/'scripts/validate_research.py')
audit=importlib.util.module_from_spec(spec);spec.loader.exec_module(audit)

class ResearchAuditTests(unittest.TestCase):
    def test_official_convex_decreasing_fixture(self):
        # kneed.DataGenerator.convex_decreasing(): known zero-based knee x=2.
        # Our ranked API is one-based and applies log1p, so undo that input transform.
        from kneed import DataGenerator, KneeLocator
        x,y=DataGenerator.convex_decreasing()
        self.assertEqual(KneeLocator(x,y,curve='convex',direction='decreasing').knee,2)
        result=audit.kneedle([math.expm1(float(v)) for v in y],1)
        self.assertEqual(result['rank'],3)
        self.assertEqual(result['percent'],30)

    def test_missing_knee_is_not_forced(self):
        self.assertIsNone(audit.kneedle([3,3,3,3]))
        self.assertIsNone(audit.kneedle([5,2]))
        self.assertIsNone(audit.kneedle([math.expm1(y) for y in (5,4,3,2,1)],5))

    def test_exact_page_and_core_prediction_fixture(self):
        movies=[]
        for cd,date,audience,ids in [('1','20170101',100,['a','b']),('2','20180101',200,['a','c']),('3','20180101',300,['a','b']),('4','20190101',400,['b','c'])]:
            movies.append({'movieCd':cd,'movieNm':cd,'openDt':date,'audiAcc':audience,'nation':'한국','actors':[{'id':i,'name':i} for i in ids]})
        with tempfile.TemporaryDirectory() as tmp:
            data=Path(tmp)/'fixture.json';data.write_text(json.dumps(movies),encoding='utf-8')
            output=audit.run_js(ROOT,data)
        self.assertEqual(output['crossCheck']['sampleCount'],5)
        self.assertLess(output['crossCheck']['maxRowDifference'],1e-12)
        # Same-day two movies cannot count as each other's prior history.
        same_day=[r for r in output['datedValidation'] if r['date']=='20180101']
        self.assertEqual([r['n'] for r in same_day],[1,1,1])
        independently=audit.metric_rows(output['datedValidation'])
        for actual,expected in zip(output['lambda']['results'],independently):
            self.assertAlmostEqual(actual['mae'],expected['mae'],places=14)
            self.assertAlmostEqual(actual['rmse'],expected['rmse'],places=14)
        self.assertEqual(len(independently),50)

    def test_loyo_and_valid_year_recurrence(self):
        self.assertEqual(audit.number_summary([1,2,3,100])['loyoMedianRange'],[2,3])
        years={'2020':[{'actorKey':str(i)} for i in range(11)],'2021':[{'actorKey':str(i)} for i in range(11)],'2026':[{'actorKey':'excluded'}]}
        result=audit.recurrence(years,['2020','2021'],20)
        self.assertEqual(result['persistentCounts']['2'],2)
        self.assertAlmostEqual(result['annualActualFractions'][0]['actualPercent'],200/11)
        self.assertNotIn('excluded',result['persistentActors']['2'])

if __name__=='__main__':unittest.main()
