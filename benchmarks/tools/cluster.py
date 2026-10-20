#!/usr/bin/env python3
# SPDX-License-Identifier: RPL-1.5
# Copyright (c) 2026 Karl Livesey
"""Three-process native cluster benchmark: per-process CPU per completed write.

Runs `native_benchmark node N ...` as three child processes on loopback, starts
node 1's offered load once all three print `ready`, closes every child's stdin
once node 1 prints its summary, and reaps every child with os.wait4 for its own
user+sys CPU. The sum must equal the RUSAGE_CHILDREN delta within
CROSSCHECK_TOLERANCE_S (0.01 s), or the run is invalid (cpu_crosscheck=SUM:DELTA):
a child of a child or a reaping fault cannot pass silently.

  cluster.py [options] BIN [BIN ...]

Wall and busy cores. A child's CPU covers its whole life (startup, campaign,
warmup, offered phase, drain, exit), and every child's life lies inside the
run's common interval: from the first spawn to the last child's exit. wall_s is
that interval; busy (total) and nN_busy (per process) are CPU / wall_s, so the
per-process figures add up to the total. Each child's exit is stamped when it
happens (a watcher thread per child waits for it), not when the parent gets
round to it; nN_exit_s is that stamp, measured from the first spawn. Busy cores
is an average. It hides peak concurrency (work moved onto extra threads at the
same total CPU looks the same), it includes fixed startup, campaign, warmup and
drain time (which dilutes small offer counts), and it excludes kernel work done
outside the node processes (softirq, kworker and writeback threads), which
wait4 never sees.

Validity. A run is invalid unless every child exits 0, node 1 prints its
summary, applied == offers, window_refused == 0, the leader term is unchanged,
every node prints node_counters, the CPU cross-check holds and the run ends
within --timeout. An invalid run is retained and printed with its reasons; the
experiment continues.

No child outlives the run. While this script runs, every path (a failure, the
--timeout guard, SIGINT, SIGTERM or SIGHUP: their handlers raise so that
cleanup runs, and one arriving during cleanup waits for it to finish) closes every stdin, kills whatever has not exited and reaps every
child; a child is signalled only while it is known not to have been reaped, so
a freed PID is never signalled. If this script is killed with SIGKILL, the
kernel closes the children's stdin: followers exit on that EOF as usual, and
node 1 stops at the first poll round that sees it in any phase before its
summary, aborts its node (a drain cannot finish without the followers),
prints node_counters and exits with status 4 (n1_exit=4).

Rounds and pairs. Round i runs every binary at every --offers value. Even
rounds run A small, A big, B small, B big ...; odd rounds run that sequence
reversed, so over each pair of rounds (ABBA) a linear drift (heat, caches,
background load) falls on every binary and offer count alike. Use an even
--pairs for that balance. Per-cell summary lines are medians over that cell's
valid runs. Marginal CPU per write between the smallest and largest offers,
(CPU(big) - CPU(small)) / (big - small), and B/A ratios are formed within a
round only; one invalid run drops its whole round from them (every binary and
offer count) and the report names each dropped round and why. Their medians
are over the remaining valid rounds. --offers values must be distinct positive
integers.

Builds. The node mode is in the native_benchmark example, which needs the tls
feature (default features include it). A native-only build
(--no-default-features --features native) cannot build the example, so it has
no cluster mode. A build without quic runs tcp clusters and refuses quic before
creating any file; quic needs --features quic.

Example (the N0 marginal cell):
  cluster.py --wal /Volumes/vbram/n0 --rate 15000 --offers 2000,16000 \\
      --pairs 5 target/release/examples/native_benchmark
"""
import argparse
import os
import queue
import errno
import resource
import select
import shutil
import signal
import socket
import statistics
import subprocess
import sys
import threading
import time

COUNTERS = ("poll_rounds", "host_waits", "host_waits_immediate", "host_waits_ready",
            "host_waits_timeout", "sends", "received", "persistence_batches")
SHORT = {"poll_rounds": "rounds", "host_waits": "waits", "host_waits_immediate": "imm",
         "host_waits_ready": "rdy", "host_waits_timeout": "tmo", "sends": "sends",
         "received": "recv", "persistence_batches": "pb"}
NODES = (1, 2, 3)
# |sum of per-child wait4 CPU - RUSAGE_CHILDREN delta| above this invalidates a
# run. Both come from the same kernel accounting at microsecond resolution.
CROSSCHECK_TOLERANCE_S = 0.01
# A node 1 that stopped because its stdin closed before its summary.
STDIN_CLOSED_STATUS = 4


class OffersError(ValueError):
    """--offers is not a list of distinct positive integers."""


def parse_offers(text):
    """Sorted distinct positive offer counts, or OffersError."""
    try:
        offers = [int(x) for x in text.split(",")]
    except ValueError:
        raise OffersError("--offers must be a comma list of integers, got %r" % text)
    if any(o <= 0 for o in offers):
        raise OffersError("--offers values must be positive, got %r" % text)
    if len(set(offers)) != len(offers):
        raise OffersError("--offers values must be distinct (a marginal needs two "
                          "different offer counts), got %r" % text)
    return sorted(offers)


def schedule(round_index, labels, offers):
    """The (label, offers) order of one round: forward in even rounds and
    reversed in odd ones (ABBA over consecutive rounds)."""
    order = [(label, o) for label in labels for o in offers]
    return order if round_index % 2 == 0 else order[::-1]


def kv(line):
    out = {}
    for word in line.split():
        if "=" in word:
            k, v = word.split("=", 1)
            out.setdefault(k, v)
    return out


def free_base(start):
    """A BASE whose BASE+1..3 are free for TCP and UDP right now."""
    for base in range(start, 32000, 8):
        held = []
        try:
            for port in (base + 1, base + 2, base + 3):
                for kind in (socket.SOCK_STREAM, socket.SOCK_DGRAM):
                    s = socket.socket(socket.AF_INET, kind)
                    held.append(s)
                    s.bind(("127.0.0.1", port))
            return base
        except OSError:
            continue
        finally:
            for s in held:
                s.close()
    raise RuntimeError("no free contiguous port block")


def children_cpu():
    r = resource.getrusage(resource.RUSAGE_CHILDREN)
    return r.ru_utime + r.ru_stime


def exit_code(status):
    return os.WEXITSTATUS(status) if os.WIFEXITED(status) else -os.WTERMSIG(status)


def wait_exited(pid):
    """Block until child `pid` has exited, without reaping it: its PID stays
    allocated (a zombie) until wait4. waitid(WNOWAIT) where Python has it
    (Linux); kqueue NOTE_EXIT otherwise (macOS, whose Python has no waitid)."""
    if hasattr(os, "waitid"):
        os.waitid(os.P_PID, pid, os.WEXITED | os.WNOWAIT)
        return
    kq = select.kqueue()
    try:
        ev = select.kevent(pid, filter=select.KQ_FILTER_PROC,
                           flags=select.KQ_EV_ADD | select.KQ_EV_ONESHOT,
                           fflags=select.KQ_NOTE_EXIT)
        for got in kq.control([ev], 1, None):
            # ESRCH: the child had already exited (it is our unreaped zombie).
            if got.flags & select.KQ_EV_ERROR and got.data not in (0, errno.ESRCH):
                raise OSError(got.data, os.strerror(got.data))
    except ProcessLookupError:
        pass
    finally:
        kq.close()


CLEANUP_SIGNALS = (signal.SIGTERM, signal.SIGHUP, signal.SIGINT)
# While a run's cleanup is in progress, a signal is recorded here and raised
# once cleanup ends, so it cannot cut the kill-and-reap short.
_cleanup = {"depth": 0, "pending": None}


def raise_on_signal(signum, _frame):
    """Handler for CLEANUP_SIGNALS (installed when run as a script): raise
    SystemExit so every `finally` runs (close stdin, kill, reap) instead of
    dying with children still running; deferred while cleanup runs."""
    if _cleanup["depth"]:
        _cleanup["pending"] = signum
        return
    raise SystemExit(128 + signum)


class Cluster:
    """Three children, each with a stdout reader and an exit watcher thread.

    The watcher is the only reaper of its child. It waits for the exit without
    reaping (waitid with WNOWAIT, so the PID stays the child's), stamps the
    time, marks the child exited under the lock, and only then reaps it with
    wait4 for its status and rusage. `kill` signals a child only under that
    lock and only if it is not marked exited, so it never signals a PID that
    has been freed (a zombie still owns its PID; signalling it is harmless).
    """

    def __init__(self):
        self.events = queue.Queue()
        self.procs, self.spawned, self.watchers = {}, {}, {}
        self.reaped, self.exited, self.lock = {}, set(), threading.Lock()

    def spawn(self, n, cmd, err):
        self.spawned[n] = time.monotonic()
        p = subprocess.Popen(cmd, stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                             stderr=err, text=True)
        self.procs[n] = p

        def reader():
            for line in p.stdout:
                self.events.put(("line", n, line.rstrip("\n")))
            p.stdout.close()
            self.events.put(("eof", n, None))

        def watcher():
            try:
                wait_exited(p.pid)
            except ChildProcessError:  # reaped elsewhere
                pass
            at = time.monotonic()
            with self.lock:
                self.exited.add(n)
            try:
                _, status, usage = os.wait4(p.pid, 0)
            except ChildProcessError:  # reaped elsewhere: no status or rusage
                status = usage = None
            with self.lock:
                self.reaped[n] = (at, status, usage)
                p.returncode = -255 if status is None else exit_code(status)
            self.events.put(("exit", n, None))

        threading.Thread(target=reader, daemon=True).start()
        self.watchers[n] = threading.Thread(target=watcher, daemon=True)
        self.watchers[n].start()

    def write(self, n, text):
        try:
            self.procs[n].stdin.write(text)
            self.procs[n].stdin.flush()
            return True
        except (BrokenPipeError, ValueError, OSError):
            return False

    def close_stdin(self):
        for p in self.procs.values():
            try:
                p.stdin.close()
            except (BrokenPipeError, OSError):
                pass

    def kill(self):
        for n, p in self.procs.items():
            with self.lock:
                if n not in self.exited:
                    try:
                        os.kill(p.pid, signal.SIGKILL)
                    except ProcessLookupError:
                        pass

    def wait_all(self):
        for t in self.watchers.values():
            t.join()


def check(result, offers):
    """Validity reasons for one run; empty when the run is valid."""
    s, reasons = result["summary"], list(result["errors"])
    for n in NODES:
        v = result["node"].get(n)
        if v is None:
            reasons.append("n%d_not_spawned" % n)
            continue
        if v["exit"] != 0:
            reasons.append("n%d_exit=%d" % (n, v["exit"]))
        if not v["counters"]:
            reasons.append("n%d_no_counters" % n)
    nodes = result["node"].values()
    if len(result["node"]) == len(NODES) and "children_cpu" in result:
        cpu = sum(v["cpu"] for v in nodes)
        if abs(cpu - result["children_cpu"]) > CROSSCHECK_TOLERANCE_S:
            reasons.append("cpu_crosscheck=%.4f:%.4f" % (cpu, result["children_cpu"]))
    if not s:
        reasons.append("no_summary")
    else:
        if s.get("applied") != str(offers):
            reasons.append("applied=%s" % s.get("applied"))
        if s.get("window_refused") != "0":
            reasons.append("window_refused=%s" % s.get("window_refused"))
        if not s.get("leader_term_start") or s.get("leader_term_start") != s.get("leader_term_end"):
            reasons.append("term=%s->%s" % (s.get("leader_term_start"), s.get("leader_term_end")))
    return reasons


def run_once(binary, args, offers, root):
    """One cluster run. Always returns a result (invalid ones say why) and
    never leaves a child alive or unreaped."""
    if os.path.exists(root):
        shutil.rmtree(root)
    os.makedirs(root)
    before = children_cpu()
    cluster, errs = Cluster(), {}
    lines = {n: [] for n in NODES}
    errors, summary, base = [], None, None
    extra = ["--ready-items", args.ready] if args.ready else []
    try:
        base = free_base(args.base)
        for n in NODES:
            errs[n] = open(os.path.join(root, "node%d.err" % n), "w")
            cmd = [binary, "node", str(n), os.path.join(root, "node%d" % n), args.proto,
                   str(base), str(offers), str(args.window), str(args.rate)] + extra
            cluster.spawn(n, cmd, errs[n])
        ready, exited, open_streams = set(), set(), set(NODES)
        released = False
        deadline = time.monotonic() + args.timeout
        while len(exited) < len(NODES) or open_streams:
            left = deadline - time.monotonic()
            try:
                # At most 0.5 s per wait: on macOS a lock wait is not woken by
                # signals, and a SIGTERM handler must run promptly.
                kind, n, line = cluster.events.get(timeout=min(0.5, max(0.01, left)))
            except queue.Empty:
                if time.monotonic() < deadline:
                    continue
                if "timeout" not in errors:
                    errors.append("timeout")
                    cluster.close_stdin()
                    cluster.kill()
                    # Killed children still exit; wait for every watcher and
                    # give the pipes a bounded drain below.
                    cluster.wait_all()
                    deadline = time.monotonic() + 10.0
                    continue
                errors.append("stdout_open=%s" % ",".join(str(x) for x in sorted(open_streams)))
                break
            if kind == "eof":
                open_streams.discard(n)
            elif kind == "exit":
                exited.add(n)
                if summary is None and not released:
                    # A child ended before node 1 reported: release the others
                    # so the run ends (followers exit on EOF; node 1 stops on
                    # EOF in any phase before its summary, status 4).
                    cluster.close_stdin()
                    released = True
            else:
                lines[n].append(line)
                if line.startswith("ready node="):
                    ready.add(n)
                    if len(ready) == len(NODES) and not released:
                        if not cluster.write(1, "start\n"):
                            errors.append("start_not_delivered")
                elif line.startswith("mode=cluster") and n == 1:
                    summary = kv(line)
                    if not released:
                        cluster.close_stdin()
                        released = True
    except Exception as e:  # retained as an invalid run
        errors.append("error=%s" % type(e).__name__)
        print("# run error: %r" % (e,), file=sys.stderr, flush=True)
    finally:
        # A signal arriving now waits until every child is reaped.
        _cleanup["depth"] += 1
        try:
            cluster.close_stdin()
            cluster.kill()
            cluster.wait_all()
            for f in errs.values():
                f.close()
        finally:
            _cleanup["depth"] -= 1
        if _cleanup["pending"] is not None and not _cleanup["depth"]:
            signum, _cleanup["pending"] = _cleanup["pending"], None
            raise SystemExit(128 + signum)
    first = min(cluster.spawned.values()) if cluster.spawned else 0.0
    result = {"base": base, "summary": summary or {}, "node": {}, "errors": errors,
              "children_cpu": children_cpu() - before}
    for n, (at, status, usage) in sorted(cluster.reaped.items()):
        if status is None:
            errors.append("n%d_not_reaped" % n)
            continue
        result["node"][n] = {
            "cpu": usage.ru_utime + usage.ru_stime, "user": usage.ru_utime,
            "sys": usage.ru_stime, "exit": exit_code(status), "exit_s": at - first,
            "counters": next((kv(l) for l in lines[n] if l.startswith("node_counters")), {}),
        }
    result["wall"] = max((v["exit_s"] for v in result["node"].values()), default=0.0)
    reasons = check(result, offers)
    result["invalid"] = ",".join(reasons)
    if not reasons and not args.keep:
        shutil.rmtree(root, ignore_errors=True)
    return result


def total_cpu(r):
    return sum(v["cpu"] for v in r["node"].values())


def busy(cpu, wall):
    return cpu / wall if wall else 0.0


def describe(tag, binary, offers, r):
    s, nodes, wall = r["summary"], r["node"], r["wall"]
    cpu = total_cpu(r)
    applied = int(s.get("applied", "0")) or 1
    parts = [tag, "bin=%s" % binary, "offers=%d" % offers,
             "valid=%s" % ("yes" if not r["invalid"] else "no(%s)" % r["invalid"]),
             "applied=%s" % s.get("applied"), "refused=%s" % s.get("window_refused"),
             "term=%s->%s" % (s.get("leader_term_start"), s.get("leader_term_end")),
             "rate=%s" % s.get("applied_total_ops_s"), "p99_us=%s" % s.get("p99_us"),
             "cpu_s=%.4f" % cpu, "children_cpu_s=%.4f" % r["children_cpu"],
             "cpu_us_per_write=%.1f" % (cpu * 1e6 / applied), "wall_s=%.3f" % wall,
             "busy=%.3f" % busy(cpu, wall)]
    for n in sorted(nodes):
        v = nodes[n]
        parts.append("n%d_cpu=%.4f n%d_user=%.4f n%d_sys=%.4f n%d_exit_s=%.3f n%d_busy=%.3f" % (
            n, v["cpu"], n, v["user"], n, v["sys"], n, v["exit_s"], n, busy(v["cpu"], wall)))
        for k in COUNTERS:
            parts.append("n%d_%s=%s" % (n, SHORT[k], v["counters"].get(k, "NA")))
    return " ".join(parts)


def median(xs):
    return statistics.median(xs) if xs else float("nan")


def rounds(runs, bins, offers):
    """Split round indices into valid ones and dropped ones with reasons.

    `runs` is a list of (round, label, offers, result). A round is valid only
    when every binary at every offer count has a valid run in it.
    """
    valid, dropped = [], {}
    for i in sorted({ii for (ii, _, _, _) in runs}):
        reasons = []
        for b in bins:
            for o in offers:
                r = next((r for (ii, bb, oo, r) in runs if (ii, bb, oo) == (i, b, o)), None)
                if r is None:
                    reasons.append("%s offers=%d missing" % (b, o))
                elif r["invalid"]:
                    reasons.append("%s offers=%d invalid(%s)" % (b, o, r["invalid"]))
        if reasons:
            dropped[i] = reasons
        else:
            valid.append(i)
    return valid, dropped


def marginal_rows(runs, binary, small, big, valid):
    """Per valid round: marginal CPU per write in µs, per node and in total."""
    out = {}
    for i in valid:
        rs = {o: r for (ii, bb, o, r) in runs if ii == i and bb == binary}
        row = {n: (rs[big]["node"][n]["cpu"] - rs[small]["node"][n]["cpu"]) * 1e6 / (big - small)
               for n in NODES}
        row["total"] = sum(row[n] for n in NODES)
        out[i] = row
    return out


def report(runs, bins, offers, out=None):
    out = out or sys.stdout
    if len(set(offers)) != len(offers):
        raise OffersError("--offers values must be distinct, got %r" % (offers,))
    print("# summary (medians over each cell's valid runs)", file=out)
    for b in bins:
        for o in offers:
            ok = [r for (_, bb, oo, r) in runs if bb == b and oo == o and not r["invalid"]]
            if not ok:
                print("%s offers=%d: no valid runs" % (b, o), file=out)
                continue
            line = "%s offers=%d valid=%d cpu_us_per_write=%.1f busy=%.3f" % (
                b, o, len(ok), median([total_cpu(r) * 1e6 / o for r in ok]),
                median([busy(total_cpu(r), r["wall"]) for r in ok]))
            for n in NODES:
                line += " n%d_cpu_us_per_write=%.1f n%d_busy=%.3f" % (
                    n, median([r["node"][n]["cpu"] * 1e6 / o for r in ok]),
                    n, median([busy(r["node"][n]["cpu"], r["wall"]) for r in ok]))
                for k in ("poll_rounds", "sends", "received", "persistence_batches"):
                    line += " n%d_%s_per_write=%.3f" % (n, SHORT[k], median(
                        [int(r["node"][n]["counters"][k]) / o for r in ok]))
            line += " rate=%.1f p99_us=%.1f" % (
                median([float(r["summary"]["applied_total_ops_s"]) for r in ok]),
                median([float(r["summary"]["p99_us"]) for r in ok]))
            print(line, file=out)
    if len(offers) < 2 and len(bins) < 2:
        return
    valid, dropped = rounds(runs, bins, offers)
    print("# paired figures use valid rounds only: valid=[%s] dropped=[%s]" % (
        " ".join(str(i) for i in valid), " ".join(str(i) for i in sorted(dropped))), file=out)
    for i in sorted(dropped):
        print("# dropped round=%d: %s" % (i, "; ".join(dropped[i])), file=out)
    if len(offers) >= 2:
        small, big = offers[0], offers[-1]
        print("# marginal CPU per write, (CPU at %d - CPU at %d) / %d, per valid round" % (
            big, small, big - small), file=out)
        marg = {}
        for b in bins:
            rows = marginal_rows(runs, b, small, big, valid)
            marg[b] = rows
            if rows:
                print("%s marginal_us_per_write total=%.2f n1=%.2f n2=%.2f n3=%.2f rounds=%d all=[%s]" % (
                    b, median([r["total"] for r in rows.values()]),
                    *[median([r[n] for r in rows.values()]) for n in NODES], len(rows),
                    " ".join("r%d:%.2f" % (i, rows[i]["total"]) for i in sorted(rows))), file=out)
            else:
                print("%s marginal_us_per_write: no valid rounds" % b, file=out)
        if len(bins) == 2:
            a, c = bins
            pairs = [(i, marg[c][i]["total"] / marg[a][i]["total"]) for i in valid
                     if marg[a][i]["total"]]
            print_ratio("B/A marginal ratio", pairs, out)
    else:
        a, c = bins[0], bins[1]
        o = offers[0]
        pairs = []
        for i in valid:
            ra = next(r for (ii, bb, oo, r) in runs if (ii, bb, oo) == (i, a, o))
            rc = next(r for (ii, bb, oo, r) in runs if (ii, bb, oo) == (i, c, o))
            if total_cpu(ra):
                pairs.append((i, total_cpu(rc) / total_cpu(ra)))
        print_ratio("B/A total CPU ratio", pairs, out)


def print_ratio(label, pairs, out):
    if pairs:
        print("%s median=%.3f pairs=[%s] rounds=[%s]" % (
            label, median([x for _, x in pairs]), " ".join("%.3f" % x for _, x in pairs),
            " ".join(str(i) for i, _ in pairs)), file=out)
    else:
        print("%s: no valid rounds" % label, file=out)


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("bins", nargs="+", help="native_benchmark binaries (A [B ...])")
    ap.add_argument("--wal", required=True, help="scratch root for node directories")
    ap.add_argument("--proto", default="tcp", choices=("tcp", "quic"))
    ap.add_argument("--rate", type=int, default=15000)
    ap.add_argument("--window", type=int, default=1024)
    ap.add_argument("--offers", default="16000", help="comma list, e.g. 2000,16000")
    ap.add_argument("--pairs", type=int, default=1, help="rounds over all binaries")
    ap.add_argument("--ready", default=None, help="--ready-items value (default: the binary's)")
    ap.add_argument("--base", type=int, default=24000, help="first BASE_PORT to probe")
    ap.add_argument("--timeout", type=float, default=900.0, help="per-run guard, seconds")
    ap.add_argument("--keep", action="store_true", help="keep node directories")
    args = ap.parse_args(sys.argv[1:] if argv is None else argv)
    try:
        offers = parse_offers(args.offers)
    except OffersError as e:
        ap.error(str(e))
    # Runs are keyed by label, so one binary may be both A and B (A/A noise).
    labels = [chr(ord("A") + i) for i in range(len(args.bins))]
    print("# cluster.py proto=%s rate=%d window=%d offers=%s pairs=%d load=%s" % (
        args.proto, args.rate, args.window, offers, args.pairs, os.getloadavg()), flush=True)
    print("# wall_s = first spawn to last child exit; busy = CPU / wall_s (per process and total)",
          flush=True)
    print("# order: even rounds A..Z small..big, odd rounds reversed (ABBA)", flush=True)
    for label, b in zip(labels, args.bins):
        print("# %s=%s" % (label, b), flush=True)
    runs = []  # (round, label, offers, result)
    binary = dict(zip(labels, args.bins))
    for i in range(args.pairs):
        for label, o in schedule(i, labels, offers):
            root = os.path.join(args.wal, "cluster-%d-%d-%s-%d" % (os.getpid(), i, label, o))
            r = run_once(binary[label], args, o, root)
            runs.append((i, label, o, r))
            print(describe("run round=%d" % i, label, o, r), flush=True)
    report(runs, labels, offers)
    return 0


if __name__ == "__main__":
    for sig in CLEANUP_SIGNALS:
        signal.signal(sig, raise_on_signal)
    sys.exit(main())
