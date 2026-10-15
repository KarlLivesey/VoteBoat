"""Harmless owned Python child probes only: no VoteBoat workload or build."""
import contextlib, json, os, platform, signal, subprocess, sys, tempfile, time, unittest
from pathlib import Path

@contextlib.contextmanager
def probe(label):
    # Retain deliberate negative controls and their raw errors for review.
    yield tempfile.mkdtemp(prefix=label,dir=Path(__file__).resolve().parents[1])

class ResourceTests(unittest.TestCase):
    def run_helper(self, path, child):
        return subprocess.Popen([sys.executable,str(Path(__file__).parent/'resources.py'),str(path),sys.executable,'-c',child],stdout=subprocess.PIPE,stderr=subprocess.PIPE)
    def assert_gone(self, pid):
        with self.assertRaises(ProcessLookupError): os.kill(pid,0)
    def test_success_records_actual_host_rss_units(self):
        with probe('resource-success-') as directory:
            helper = self.run_helper(Path(directory),'pass'); stdout,stderr = helper.communicate(timeout=5)
            self.assertEqual(helper.returncode,0,stderr)
            (Path(directory)/'helper.stdout').write_bytes(stdout); (Path(directory)/'helper.stderr').write_bytes(stderr)
            (Path(directory)/'helper.exit').write_text(str(helper.returncode)+'\n')
            result = json.loads((Path(directory)/'RESOURCES.json').read_text())
            self.assertEqual(result['exit'],0); self.assertIsNone(result['helper_error'])
            unit = 'bytes' if platform.system() == 'Darwin' else 'KiB'
            self.assertEqual(result['max_rss_unit'],unit)
            self.assertEqual(result['max_rss_bytes'],result['max_rss_raw']*(1 if unit=='bytes' else 1024))
    def test_pid_metadata_failure_terminates_and_waits_original_child(self):
        with probe('resource-write-error-') as directory:
            root = Path(directory); (root/'NATIVE_PID').mkdir()
            helper = self.run_helper(root,'import time; time.sleep(30)'); stdout,stderr = helper.communicate(timeout=5)
            self.assertEqual(helper.returncode,1)
            (Path(directory)/'helper.stdout').write_bytes(stdout); (Path(directory)/'helper.stderr').write_bytes(stderr)
            (Path(directory)/'helper.exit').write_text(str(helper.returncode)+'\n')
            result = json.loads((root/'RESOURCES.json').read_text())
            self.assertEqual(result['exit'],-signal.SIGTERM)
            self.assertIn('IsADirectoryError',result['helper_error']); self.assert_gone(result['pid'])
    def test_signal_to_resource_helper_alone_reaches_and_waits_original_child(self):
        with probe('resource-signal-') as directory:
            root = Path(directory); helper = self.run_helper(root,'import time; time.sleep(30)')
            deadline = time.monotonic()+5
            while not (root/'NATIVE_PID').is_file() and time.monotonic()<deadline: time.sleep(.01)
            self.assertTrue((root/'NATIVE_PID').is_file())
            helper.send_signal(signal.SIGTERM); stdout,stderr = helper.communicate(timeout=5)
            self.assertEqual(helper.returncode,128+signal.SIGTERM,stderr)
            (Path(directory)/'helper.stdout').write_bytes(stdout); (Path(directory)/'helper.stderr').write_bytes(stderr)
            (Path(directory)/'helper.exit').write_text(str(helper.returncode)+'\n')
            result = json.loads((root/'RESOURCES.json').read_text())
            self.assertEqual(result['exit'],-signal.SIGTERM); self.assert_gone(result['pid'])

if __name__ == '__main__': unittest.main()
