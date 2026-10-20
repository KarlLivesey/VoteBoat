#!/usr/bin/env python3
# SPDX-License-Identifier: RPL-1.5
# Copyright (c) 2026 Karl Livesey
"""Self-test for cluster.py; `cargo test` runs it from the native_benchmark
example (offered::cluster::tests). Standard library only.

  python3 -I benchmarks/tools/cluster_test.py

CLUSTER_PY selects another copy of the script (to show a test failing on an
older revision). Pairing and validity checks use synthetic wait4 results; the
process checks run small Python fixture nodes that speak the node protocol.
No assertion depends on elapsed time: the one guard (--timeout) is only ever
expected to trip for a child that never exits.
"""
import argparse
import contextlib
import importlib.util
import io
import os
import re
import shutil
import signal
import subprocess
import sys
import tempfile
import time
import unittest
from types import SimpleNamespace

sys.dont_write_bytecode = True  # leave no __pycache__ in the source tree
HERE = os.path.dirname(os.path.abspath(__file__))
SCRIPT = os.environ.get("CLUSTER_PY", os.path.join(HERE, "cluster.py"))
spec = importlib.util.spec_from_file_location("cluster", SCRIPT)
cluster = importlib.util.module_from_spec(spec)
spec.loader.exec_module(cluster)

COUNTERS = {k: "1" for k in cluster.COUNTERS}


def fake_result(offers, n1_cpu, invalid):
    """A wait4-shaped result: node 1 burns n1_cpu seconds, followers none."""
    node = {}
    for n in (1, 2, 3):
        cpu = n1_cpu if n == 1 else 0.0
        node[n] = {"cpu": cpu, "user": cpu, "sys": 0.0, "exit": 0, "exit_s": 1.0,
                   "wall": 1.0, "counters": dict(COUNTERS)}
    summary = {"applied": str(offers), "window_refused": "0", "leader_term_start": "1",
               "leader_term_end": "1", "applied_total_ops_s": "1", "p99_us": "1"}
    return {"base": 1, "summary": summary, "node": node, "errors": [], "wall": 1.0,
            "children_cpu": n1_cpu, "invalid": "exit" if invalid else ""}


def run_main(argv):
    """cluster.main with sys.argv set (older revisions read only sys.argv)."""
    out, saved = io.StringIO(), sys.argv
    sys.argv = ["cluster.py"] + argv
    try:
        with contextlib.redirect_stdout(out):
            code = cluster.main()
    finally:
        sys.argv = saved
    return code, out.getvalue()


class Checks(unittest.TestCase):
    """Review round 2 (Opus) P3 findings 3 and 4: the CPU cross-check is
    enforced, and duplicate offer counts are a typed error."""

    def test_a_cpu_crosscheck_mismatch_invalidates_the_run(self):
        r = fake_result(100, 3.0, False)
        self.assertEqual(cluster.check(r, 100), [])
        r["children_cpu"] = 99.0  # RUSAGE_CHILDREN saw 99 s; wait4 saw 3 s
        self.assertEqual(cluster.check(r, 100), ["cpu_crosscheck=3.0000:99.0000"])
        # Within the stated tolerance the run stays valid.
        r["children_cpu"] = 3.0 + cluster.CROSSCHECK_TOLERANCE_S / 2
        self.assertEqual(cluster.check(r, 100), [])

    def test_duplicate_offers_are_a_typed_error_not_a_division_by_zero(self):
        with self.assertRaises(cluster.OffersError):
            cluster.parse_offers("2000,2000")
        for bad in ("2000,0", "x", "2000,-1"):
            with self.assertRaises(cluster.OffersError):
                cluster.parse_offers(bad)
        self.assertEqual(cluster.parse_offers("16000,2000"), [2000, 16000])
        with self.assertRaises(cluster.OffersError):
            cluster.report([(0, "A", 2000, fake_result(2000, 1.0, False))], ["A"],
                           [2000, 2000], io.StringIO())
        err = io.StringIO()
        with contextlib.redirect_stderr(err), self.assertRaises(SystemExit) as stop:
            cluster.main(["--wal", "/nonexistent", "--offers", "2000,2000", "A"])
        self.assertEqual(stop.exception.code, 2)
        self.assertIn("--offers values must be distinct", err.getvalue())


class Order(unittest.TestCase):
    """Review round 2 (Opus) P3 finding 5: the order alternates (ABBA)."""

    def test_odd_rounds_run_the_sequence_reversed(self):
        calls = []

        def fake_run_once(binary, args, offers, root):
            calls.append((binary, offers))
            return fake_result(offers, 1.0, False)

        saved = cluster.run_once
        cluster.run_once = fake_run_once
        try:
            code, out = run_main(["--wal", "/nonexistent", "--offers", "1000,2000",
                                  "--pairs", "2", "binA", "binB"])
        finally:
            cluster.run_once = saved
        self.assertEqual(code, 0)
        forward = [("binA", 1000), ("binA", 2000), ("binB", 1000), ("binB", 2000)]
        self.assertEqual(calls, forward + forward[::-1])
        runs = [l.split()[1:4] for l in out.splitlines() if l.startswith("run round=")]
        self.assertEqual(runs[4], ["round=1", "bin=B", "offers=2000"])
        self.assertIn("ABBA", out)


class Pairing(unittest.TestCase):
    """Review finding 1: A/B marginal pairs must come from the same round."""

    def test_marginal_ratio_pairs_only_rounds_where_every_run_is_valid(self):
        small, big = 1000, 2000
        # Marginal µs/write = node-1 CPU delta (s) * 1e6 / 1000.
        delta = {("A", 0): 1, ("A", 1): 2, ("A", 2): 3,
                 ("B", 0): 10, ("B", 1): 20, ("B", 2): 30}
        bad = {("A", 1), ("B", 0)}
        calls = []

        def fake_run_once(binary, args, offers, root):
            i = sum(1 for (b, o) in calls if (b, o) == (binary, offers))
            calls.append((binary, offers))
            cpu = 0.0 if offers == small else delta[(binary, i)] * (big - small) / 1e6
            return fake_result(offers, cpu, (binary, i) in bad and offers == big)

        saved = cluster.run_once
        cluster.run_once = fake_run_once
        try:
            code, out = run_main(["--wal", "/nonexistent", "--offers", "1000,2000",
                                  "--pairs", "3", "A", "B"])
        finally:
            cluster.run_once = saved
        self.assertEqual(code, 0)
        ratio = [l for l in out.splitlines() if l.startswith("B/A marginal ratio")]
        self.assertEqual(len(ratio), 1, out)
        pairs = re.search(r"pairs=\[([^\]]*)\]", ratio[0]).group(1).split()
        # Round 2 is the only round in which every run is valid: 30 / 3.
        self.assertEqual([float(x) for x in pairs], [10.0], out)
        self.assertIn("median=10.000", ratio[0])
        marg = {l.split()[0]: l for l in out.splitlines() if "marginal_us_per_write" in l}
        self.assertIn("total=3.00", marg["A"], out)
        self.assertIn("total=30.00", marg["B"], out)
        self.assertIn("rounds=1", marg["A"], out)
        # The report says which rounds were dropped and why.
        self.assertIn("# dropped round=0: B offers=2000 invalid(exit)", out)
        self.assertIn("# dropped round=1: A offers=2000 invalid(exit)", out)

    def test_one_binary_may_be_both_a_and_b(self):
        cpus = iter([1.0, 3.0])  # A then B within round 0

        def fake_run_once(binary, args, offers, root):
            return fake_result(offers, next(cpus), False)

        saved = cluster.run_once
        cluster.run_once = fake_run_once
        try:
            code, out = run_main(["--wal", "/nonexistent", "--offers", "10", "X", "X"])
        finally:
            cluster.run_once = saved
        self.assertEqual(code, 0)
        self.assertIn("A offers=10 valid=1 cpu_us_per_write=100000.0", out)
        self.assertIn("B offers=10 valid=1 cpu_us_per_write=300000.0", out)
        self.assertIn("B/A total CPU ratio median=3.000 pairs=[3.000] rounds=[0]", out)

    def test_single_offer_ratio_drops_a_round_with_any_invalid_run(self):
        runs = [(0, "A", 5, fake_result(5, 1.0, False)), (0, "B", 5, fake_result(5, 2.0, True)),
                (1, "A", 5, fake_result(5, 1.0, False)), (1, "B", 5, fake_result(5, 4.0, False))]
        valid, dropped = cluster.rounds(runs, ["A", "B"], [5])
        self.assertEqual(valid, [1])
        self.assertEqual(dropped, {0: ["B offers=5 invalid(exit)"]})
        out = io.StringIO()
        cluster.report(runs, ["A", "B"], [5], out)
        self.assertIn("B/A total CPU ratio median=4.000 pairs=[4.000] rounds=[1]", out.getvalue())


FIXTURE = r'''#!{python} -I
import os, sys, time
_, mode, n, root, proto, base, offers, window, rate = sys.argv[:9]
n = int(n)
os.makedirs(root)
print("ready node=%d" % n, flush=True)
counters = "node_counters node=%d poll_rounds=2 host_waits=1 host_waits_immediate=1 host_waits_ready=0 host_waits_timeout=0 sends=1 received=1 persistence_batches=1" % n
fail = os.environ["FIXTURE"]
wal = os.path.dirname(os.path.dirname(root))
marker = os.path.join(wal, "failed-once")
if fail == "linger":
    # Held until this process ends: the test reads EOF from this FIFO only
    # once every fixture is gone, without naming or signalling a PID.
    alive = os.open(os.path.join(wal, "alive"), os.O_WRONLY | os.O_NONBLOCK)
    open(os.path.join(wal, "up%d" % n), "w").close()
if n == 1:
    for line in sys.stdin:
        if line.strip() == "start":
            break
    else:
        sys.exit(4)
    if fail == "linger":  # ignores EOF: a kill, or the test closing the leash, ends it
        leash = os.open(os.path.join(wal, "leash"), os.O_RDONLY | os.O_NONBLOCK)
        os.set_blocking(leash, True)
        os.read(leash, 1)
        sys.exit(9)
    if fail == "leader-once" and not os.path.exists(marker):
        open(marker, "w").close()
        sys.exit(7)
    print("mode=cluster node=1 applied=%s window_refused=0 leader_term_start=1 "
          "leader_term_end=1 applied_total_ops_s=1 p99_us=1" % offers, flush=True)
    print(counters, flush=True)
else:
    for line in sys.stdin:
        pass
    if fail == "hang" and n == 2:
        time.sleep(3600)
    print(counters, flush=True)
'''


class Processes(unittest.TestCase):
    """Review findings 2 and 3, with fixture nodes in place of native_benchmark."""

    def setUp(self):
        self.dir = tempfile.mkdtemp(prefix="voteboat-cluster-py-%d-" % os.getpid())
        self.bin = os.path.join(self.dir, "node")
        with open(self.bin, "w") as f:
            f.write(FIXTURE.replace("{python}", sys.executable))
        os.chmod(self.bin, 0o755)
        self.wal = os.path.join(self.dir, "wal")
        os.makedirs(self.wal)

    def tearDown(self):
        os.environ.pop("FIXTURE", None)
        shutil.rmtree(self.dir)

    def args(self, timeout=600.0):
        return argparse.Namespace(ready=None, proto="tcp", window=1024, rate=50, base=24000,
                                  timeout=timeout, keep=False)

    def assert_no_children(self):
        with self.assertRaises(ChildProcessError):
            os.waitpid(-1, os.WNOHANG)

    def wrapper(self, mode):
        os.environ["FIXTURE"] = mode
        # The fixture takes the mode first; native_benchmark's argv follows.
        wrapper = os.path.join(self.dir, "wrapper-" + mode)
        with open(wrapper, "w") as f:
            f.write("#!/bin/sh\nexec %s \"$FIXTURE\" \"$2\" \"$3\" \"$4\" \"$5\" \"$6\" \"$7\" \"$8\"\n"
                    % self.bin)
        os.chmod(wrapper, 0o755)
        return wrapper

    def run_fixture(self, mode, **kw):
        wrapper = self.wrapper(mode)
        return wrapper, cluster.run_once(wrapper, self.args(**kw), 100,
                                         os.path.join(self.wal, mode))

    def test_a_valid_run_uses_the_common_interval_for_every_process(self):
        _, r = self.run_fixture("ok")
        self.assertEqual(r["invalid"], "", r)
        nodes = r["node"]
        self.assertEqual(sorted(nodes), [1, 2, 3])
        # wall_s is first spawn to last exit; each exit is stamped separately.
        self.assertEqual(r["wall"], max(v["exit_s"] for v in nodes.values()))
        for v in nodes.values():
            self.assertTrue(0 < v["exit_s"] <= r["wall"])
        total = cluster.busy(cluster.total_cpu(r), r["wall"])
        per = [cluster.busy(v["cpu"], r["wall"]) for v in nodes.values()]
        self.assertAlmostEqual(sum(per), total, places=12)
        line = cluster.describe("run round=0", "A", 100, r)
        for n in (1, 2, 3):
            self.assertIn("n%d_exit_s=" % n, line)
        self.assert_no_children()

    def test_a_leader_failure_after_start_is_a_retained_invalid_run(self):
        wrapper, r = self.run_fixture("leader-once")
        self.assertIn("n1_exit=7", r["invalid"].split(","))
        self.assertIn("no_summary", r["invalid"].split(","))
        self.assert_no_children()
        # The same failure inside an experiment: round 0 fails, round 1 runs.
        os.remove(os.path.join(self.wal, "failed-once"))
        code, out = run_main(["--wal", self.wal, "--offers", "100", "--rate", "50",
                              "--pairs", "2", wrapper])
        self.assertEqual(code, 0)
        runs = [l for l in out.splitlines() if l.startswith("run round=")]
        self.assertEqual(len(runs), 2, out)
        self.assertIn("valid=no(", runs[0])
        self.assertIn("n1_exit=7", runs[0])
        self.assertIn("no_summary", runs[0])
        # The followers were released by EOF and exited 0.
        self.assertNotIn("n2_exit=", runs[0])
        self.assertNotIn("n3_exit=", runs[0])
        self.assertIn("valid=yes", runs[1])
        self.assertIn("A offers=100 valid=1", out)
        self.assert_no_children()

    def test_a_child_that_never_exits_is_killed_reaped_and_reported(self):
        _, r = self.run_fixture("hang", timeout=1.0)
        reasons = r["invalid"].split(",")
        self.assertIn("timeout", reasons)
        self.assertIn("n2_exit=-9", reasons)
        self.assert_no_children()

    def test_a_child_is_marked_exited_before_it_is_reaped_and_never_signalled_after(self):
        """Review round 2 (Opus) P3 finding 2: kill must not signal a freed PID."""
        c = cluster.Cluster()
        seen, signalled = [], []
        real_wait4, real_kill = cluster.os.wait4, cluster.os.kill

        def wait4(pid, options):
            with c.lock:  # the PID is still ours here: marked exited, unreaped
                seen.append(any(c.procs[n].pid == pid for n in getattr(c, "exited", ())))
            return real_wait4(pid, options)

        def kill(pid, sig):
            signalled.append(pid)
            return real_kill(pid, sig)

        cluster.os.wait4, cluster.os.kill = wait4, kill
        try:
            c.spawn(1, [sys.executable, "-I", "-c", "pass"], subprocess.DEVNULL)
            kinds = set()
            while "exit" not in kinds:
                kinds.add(c.events.get(timeout=600)[0])
            c.close_stdin()
            c.kill()
            c.wait_all()
        finally:
            cluster.os.wait4, cluster.os.kill = real_wait4, real_kill
        self.assertEqual(seen, [True])
        self.assertEqual(signalled, [])
        self.assert_no_children()

    def test_a_signal_during_a_spawn_is_deferred_until_every_child_is_reaped(self):
        """Review round 3 (Opus) P3 finding 4: a signal between Popen returning
        and the child's registration (the handler is called at that point)
        must not leave that child out of the kill-and-reap. The children are
        this test process's own; it signals none of them."""
        wrapper = self.wrapper("ok")
        started, real_popen = [], cluster.subprocess.Popen

        def popen(*a, **kw):
            p = real_popen(*a, **kw)
            started.append(p)
            if len(started) == 1:
                cluster.raise_on_signal(signal.SIGTERM, None)
            return p

        cluster.subprocess.Popen = popen
        stop, left = None, None
        try:
            try:
                cluster.run_once(wrapper, self.args(), 100, os.path.join(self.wal, "signal"))
            except SystemExit as e:
                stop = e.code
            try:
                os.waitpid(-1, os.WNOHANG)
                left = True
            except ChildProcessError:
                left = False
        finally:
            cluster.subprocess.Popen = real_popen
            for p in started:  # only on failure: EOF ends a fixture, then reap it
                if p.returncode is None:
                    p.stdin.close()
                    p.wait()
            cluster._cleanup.update(depth=0, pending=None)
        self.assertEqual(stop, 128 + signal.SIGTERM)
        self.assertFalse(left, "a child was left unreaped")

    def test_sigterm_to_the_script_kills_and_reaps_every_child(self):
        """Review round 2 (Opus) P2 finding 1, parent side: SIGTERM runs the
        cleanup. Node 1 here ignores EOF, so only the script's kill ends it.
        Round 3 P3 finding 2: the test never signals a PID it did not spawn.
        Every fixture holds the write end of the FIFO `alive` open, so EOF on
        it means every fixture has exited; a node 1 that survives a failure
        is released by closing the FIFO `leash` it blocks on."""
        wrapper = self.wrapper("linger")
        alive_path, leash_path = (os.path.join(self.wal, x) for x in ("alive", "leash"))
        os.mkfifo(alive_path)
        os.mkfifo(leash_path)
        alive = os.open(alive_path, os.O_RDONLY | os.O_NONBLOCK)
        # A write end needs a reader to open without blocking; keep the writer only.
        reader = os.open(leash_path, os.O_RDONLY | os.O_NONBLOCK)
        leash = os.open(leash_path, os.O_WRONLY | os.O_NONBLOCK)
        os.close(reader)
        script = subprocess.Popen([sys.executable, "-I", SCRIPT, "--wal", self.wal, "--offers",
                                   "100", "--rate", "50", wrapper],
                                  stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        code, survivors = None, None
        try:
            ups = [os.path.join(self.wal, "up%d" % n) for n in (1, 2, 3)]
            # A failure guard only: the fixtures write these at once.
            limit = time.monotonic() + 600
            while not all(os.path.exists(p) for p in ups):
                self.assertIsNone(script.poll(), "cluster.py ended early")
                self.assertLess(time.monotonic(), limit, "fixtures never started")
                time.sleep(0.01)
            script.send_signal(signal.SIGTERM)
            code = script.wait(timeout=600)
            # Nothing is ever written: EOF (b"") means no fixture holds it now.
            try:
                survivors = os.read(alive, 1) != b""
            except BlockingIOError:
                survivors = True
        finally:
            if script.poll() is None:  # this test's own child
                script.kill()
                script.wait()
            os.close(leash)  # a surviving node 1 reads EOF and exits
            os.close(alive)
        self.assertEqual(code, 128 + signal.SIGTERM)
        self.assertFalse(survivors, "children outlived cluster.py")


if __name__ == "__main__":
    unittest.main(verbosity=2)
