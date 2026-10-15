import ast,collections,unittest
from pathlib import Path
from plan import cases

class ReadyPlanTests(unittest.TestCase):
    def test_fixed_matrix_and_adjacent_quantum_pairs(self):
        matrix=cases('/binary',Path('/wal'),Path('/meta'),'/python',Path('/resources.py'))
        self.assertEqual(len(matrix),48)
        self.assertEqual(sum(c['horizon_seconds']for c in matrix),1344)
        self.assertEqual(sum(c['offers']for c in matrix),297984)
        for a,b in zip(matrix[::2],matrix[1::2]):
            self.assertEqual(a['pair'],b['pair'])
            self.assertEqual({a['ready_items'],b['ready_items']},{16,64})
            self.assertEqual(a['publication'],b['publication'])
            for field in ['cell','protocol','offers','rate','window','groups','weights','trial']:
                self.assertEqual(a[field],b[field])
            left=a['command'][:];right=b['command'][:]
            left[1]=right[1]='/fresh-root'
            left[left.index('--ready-items')+1]=right[right.index('--ready-items')+1]='QUANTUM'
            self.assertEqual(left,right)
        self.assertEqual(collections.Counter(c['ready_items']for c in matrix[::2]),{16:12,64:12})
    def test_skew_is_original_fixture_bound_and_exact_cold_demand(self):
        matrix=cases('/binary',Path('/wal'),Path('/meta'),'/python',Path('/resources.py'))
        skew=[c for c in matrix if c['cell']=='skew']
        self.assertEqual(len(skew),12)
        for case in skew:
            self.assertEqual((case['groups'],case['weights'],case['group_pending_ceiling']),(2,'31,1',2048))
            cold=sum((operation-1)%32==31 for operation in range(65,65+case['offers']))
            self.assertEqual(cold,256)
            self.assertEqual(cold/case['horizon_seconds'],32)
    def test_all_scripts_parse_without_workloads(self):
        for p in Path(__file__).parent.glob('*.py'):ast.parse(p.read_text(),filename=str(p))

if __name__=='__main__':unittest.main()
