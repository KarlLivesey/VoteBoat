import ast,collections,unittest
from pathlib import Path
from plan import cases,validate_bound,retained_bound

class AdmissionPlanTests(unittest.TestCase):
    def test_fixed_matrix_and_adjacent_quantum_pairs(self):
        matrix=cases({'baseline':'/binary476','candidate':'/binary480'},Path('/wal'),Path('/meta'),'/python',Path('/resources.py'))
        self.assertEqual(len(matrix),48)
        self.assertEqual(sum(c['horizon_seconds']for c in matrix),1344)
        self.assertEqual(sum(c['offers']for c in matrix),391680)
        for a,b in zip(matrix[::2],matrix[1::2]):
            self.assertEqual(a['pair'],b['pair'])
            self.assertEqual({a['arm'],b['arm']},{'baseline','candidate'})
            self.assertEqual(a['publication'],b['publication'])
            for field in ['cell','protocol','ready_items','offers','rate','window','groups','weights','trial']:
                self.assertEqual(a[field],b[field])
            left=a['command'][:];right=b['command'][:]
            left[0]=right[0]='/binary';left[1]=right[1]='/fresh-root'
            left[left.index('--ready-items')+1]=right[right.index('--ready-items')+1]='QUANTUM'
            self.assertEqual(left,right)
        self.assertEqual(collections.Counter(c['arm']for c in matrix[::2]),{'baseline':12,'candidate':12})
    def test_skew_is_original_fixture_bound_and_exact_cold_demand(self):
        matrix=cases({'baseline':'/binary476','candidate':'/binary480'},Path('/wal'),Path('/meta'),'/python',Path('/resources.py'))
        skew=[c for c in matrix if c['cell']=='skew']
        self.assertEqual(len(skew),12)
        for case in skew:
            self.assertEqual((case['groups'],case['weights'],case['group_pending_ceiling']),(2,'31,1',2048))
            cold=sum((operation-1)%32==31 for operation in range(65,65+case['offers']))
            self.assertEqual(cold,256)
            self.assertEqual(cold/case['horizon_seconds'],32)
    def test_existing_retained_log_bound_rejects_initial_proposal(self):
        with self.assertRaises(AssertionError):validate_bound(16384,1)
        validate_bound(16000,1)
        self.assertEqual(retained_bound(16000,1),16128)
        self.assertEqual(16384-retained_bound(16000,1),256)
        for case in cases({'baseline':'/binary476','candidate':'/binary480'},Path('/wal'),Path('/meta'),'/python',Path('/resources.py')):validate_bound(case['offers'],case['groups'],case['weights'])
    def test_all_scripts_parse_without_workloads(self):
        for p in Path(__file__).parent.glob('*.py'):ast.parse(p.read_text(),filename=str(p))

if __name__=='__main__':unittest.main()
