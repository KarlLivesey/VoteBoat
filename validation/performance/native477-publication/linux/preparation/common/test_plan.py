import ast, collections, importlib.util, unittest
from pathlib import Path
from common import check_readbacks
from plan import cases

class PlanTests(unittest.TestCase):
    def setUp(self):
        self.cases = cases('/binary',Path('/wal'),Path('/metadata'),'/python',Path('/resources.py'))
    def test_exact_matrix_and_adjacent_matched_pairs(self):
        self.assertEqual(len(self.cases),48)
        self.assertEqual(sum(c['horizon_seconds'] for c in self.cases),1440)
        self.assertEqual(sum(c['offers'] for c in self.cases),297984)
        for a,b in zip(self.cases[::2],self.cases[1::2]):
            self.assertEqual(a['pair'],b['pair']); self.assertEqual({a['publication'],b['publication']},{'atomic','witness'})
            for field in ['protocol','offers','rate','window','trial','horizon_seconds']: self.assertEqual(a[field],b[field])
            self.assertEqual(a['command'][:1]+a['command'][2:-1],b['command'][:1]+b['command'][2:-1])
            self.assertEqual(a['command'][8:],['--maintenance','2','--ready-items','16','--offered-capacity','4096','--publication',a['publication']])
    def test_every_cell_protocol_rotates_first_arm_between_trials(self):
        pairs = self.cases[::2]
        for cell in ['singleton','moderate','bulk','overload']:
            for protocol in ['tcp','quic']:
                arms = [c['publication'] for c in pairs if c['cell']==cell and c['protocol']==protocol]
                self.assertEqual(len(arms),3); self.assertNotEqual(arms[0],arms[1]); self.assertEqual(arms[0],arms[2])
        self.assertEqual(collections.Counter(c['publication'] for c in pairs),{'atomic':12,'witness':12})
    def test_scripts_parse_without_starting_children(self):
        for p in Path(__file__).parent.glob('*.py'): ast.parse(p.read_text())
    def test_actual_readiness_lines_and_missing_mismatched_negative_controls(self):
        import json
        evidence = Path(__file__).resolve().parents[2]/'native476-linux/controls/RESULTS.json'
        actual = json.loads(evidence.read_text())
        for case in actual:
            lines = case['capacity_readbacks']+case['publication_readbacks']
            self.assertEqual(len(check_readbacks(lines,case['publication'])['publication_readback']),6)
            with self.assertRaises(AssertionError): check_readbacks(lines[:-1],case['publication'])
            wrong = ['witness' if case['publication']=='atomic' else 'atomic'][0]
            with self.assertRaises(AssertionError): check_readbacks(lines,wrong)
        case = actual[0]; lines = case['capacity_readbacks']+case['publication_readbacks']
        for original,replacement in [('shard_group_items=4112','shard_group_items=4096'),('storage_error_scope=overlapping_observations','storage_error_scope=unique'),('mode=Recover','mode=Create')]:
            with self.assertRaises(AssertionError): check_readbacks([line.replace(original,replacement) for line in lines],case['publication'])

if __name__ == '__main__': unittest.main()
