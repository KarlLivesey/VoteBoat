#!/usr/bin/env python3
# SPDX-License-Identifier: RPL-1.5
# Copyright (c) 2026 Karl Livesey
"""Three-process native cluster benchmark: per-process CPU per completed write.

Runs `native_benchmark node N ...` as three child processes on loopback, starts
node 1's offered load once all three print `ready`, and reaps every child with
os.wait4 for its own user+sys CPU. The sum is cross-checked against the
RUSAGE_CHILDREN delta. Busy cores = CPU / wall, per process and in total.

  cluster.py [options] BIN [BIN ...]

With several binaries the runs interleave A B A B ... for --pairs rounds. With
two or more --offers values the marginal CPU per write between the smallest and
largest is computed per process and in total, as (CPU(big) - CPU(small)) /
(big - small) within each round. A run is invalid unless every child exits 0,
applied == offers, window_refused == 0 and the leader term is unchanged;
invalid runs are printed and retained but left out of the medians.

Example (the N0 marginal cell):
  cluster.py --wal /Volumes/vbram/n0 --rate 15000 --offers 2000,16000 \\
      --pairs 5 target/release/examples/native_benchmark
"""
import argparse
import os
import queue
import resource
import shutil
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
    raise SystemExit("no free contiguous port block")


def children_cpu():
    r = resource.getrusage(resource.RUSAGE_CHILDREN)
    return r.ru_utime + r.ru_stime


def run_once(binary, args, offers, root):
    """One cluster run. Returns a dict of results; never leaves a child alive."""
    if os.path.exists(root):
        shutil.rmtree(root)
    os.makedirs(root)
    base = free_base(args.base)
    before = children_cpu()
    events = queue.Queue()
    procs, spawned, errs = {}, {}, {}
    extra = ["--ready-items", args.ready] if args.ready else []
    try:
        for n in (1, 2, 3):
            errs[n] = open(os.path.join(root, "node%d.err" % n), "w")
            cmd = [binary, "node", str(n), os.path.join(root, "node%d" % n), args.proto,
                   str(base), str(offers), str(args.window), str(args.rate)] + extra
            spawned[n] = time.monotonic()
            procs[n] = subprocess.Popen(cmd, stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                                        stderr=errs[n], text=True)

            def reader(n=n, p=procs[n]):
                for line in p.stdout:
                    events.put((n, line.rstrip("\n")))
                events.put((n, None))
            threading.Thread(target=reader, daemon=True).start()
        lines = {1: [], 2: [], 3: []}
        ready, open_streams, summary = set(), {1, 2, 3}, None
        deadline = time.monotonic() + args.timeout
        while open_streams:
            try:
                n, line = events.get(timeout=max(0.1, deadline - time.monotonic()))
            except queue.Empty:
                raise RuntimeError("timed out; streams still open: %s" % sorted(open_streams))
            if line is None:
                open_streams.discard(n)
                if summary is None and len(ready) < 3:
                    break  # a child ended early; the run is invalid
                continue
            lines[n].append(line)
            if line.startswith("ready node="):
                ready.add(n)
                if len(ready) == 3:
                    procs[1].stdin.write("start\n")
                    procs[1].stdin.flush()
            elif line.startswith("mode=cluster"):
                summary = kv(line)
                for p in procs.values():
                    p.stdin.close()
        for p in procs.values():
            if not p.stdin.closed:
                p.stdin.close()
        # Reap in exit order, each with its own rusage and exit time.
        result = {"base": base, "summary": summary or {}, "node": {}}
        pending = {p.pid: n for n, p in procs.items()}
        while pending:
            pid, status, usage = os.wait4(-1, 0)
            if pid not in pending:
                continue
            n = pending.pop(pid)
            procs[n].returncode = (os.WEXITSTATUS(status) if os.WIFEXITED(status)
                                  else -os.WTERMSIG(status))
            counters = next((kv(l) for l in lines[n] if l.startswith("node_counters")), {})
            result["node"][n] = {
                "cpu": usage.ru_utime + usage.ru_stime, "user": usage.ru_utime,
                "sys": usage.ru_stime, "wall": time.monotonic() - spawned[n],
                "exit": procs[n].returncode, "counters": counters,
            }
    finally:
        for p in procs.values():
            if p.returncode is None:
                p.kill()
                try:
                    os.wait4(p.pid, 0)
                except ChildProcessError:
                    pass
                p.returncode = -9
        for f in errs.values():
            f.close()
    result["children_cpu"] = children_cpu() - before
    s = result["summary"]
    reasons = []
    if any(v["exit"] != 0 for v in result["node"].values()):
        reasons.append("exit")
    if s.get("applied") != str(offers):
        reasons.append("applied")
    if s.get("window_refused") != "0":
        reasons.append("refused")
    if not s.get("leader_term_start") or s.get("leader_term_start") != s.get("leader_term_end"):
        reasons.append("term")
    if any(not v["counters"] for v in result["node"].values()):
        reasons.append("counters")
    result["invalid"] = ",".join(reasons)
    if not reasons and not args.keep:
        shutil.rmtree(root, ignore_errors=True)
    return result


def describe(tag, binary, offers, r):
    s, nodes = r["summary"], r["node"]
    cpu = sum(v["cpu"] for v in nodes.values())
    wall = max(v["wall"] for v in nodes.values())
    applied = int(s.get("applied", "0")) or 1
    parts = [tag, "bin=%s" % binary, "offers=%d" % offers,
             "valid=%s" % ("yes" if not r["invalid"] else "no(%s)" % r["invalid"]),
             "applied=%s" % s.get("applied"), "refused=%s" % s.get("window_refused"),
             "term=%s->%s" % (s.get("leader_term_start"), s.get("leader_term_end")),
             "rate=%s" % s.get("applied_total_ops_s"), "p99_us=%s" % s.get("p99_us"),
             "cpu_s=%.4f" % cpu, "children_cpu_s=%.4f" % r["children_cpu"],
             "cpu_us_per_write=%.1f" % (cpu * 1e6 / applied), "wall_s=%.3f" % wall,
             "busy=%.3f" % (cpu / wall if wall else 0)]
    for n in sorted(nodes):
        v = nodes[n]
        parts.append("n%d_cpu=%.4f n%d_user=%.4f n%d_sys=%.4f n%d_busy=%.3f" % (
            n, v["cpu"], n, v["user"], n, v["sys"], n, v["cpu"] / v["wall"] if v["wall"] else 0))
        for k in COUNTERS:
            parts.append("n%d_%s=%s" % (n, SHORT[k], v["counters"].get(k, "NA")))
    return " ".join(parts)


def median(xs):
    return statistics.median(xs) if xs else float("nan")


def main():
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
    args = ap.parse_args()
    offers = sorted(int(x) for x in args.offers.split(","))
    names = {b: chr(ord("A") + i) for i, b in enumerate(args.bins)}
    print("# cluster.py proto=%s rate=%d window=%d offers=%s pairs=%d load=%s" % (
        args.proto, args.rate, args.window, offers, args.pairs, os.getloadavg()), flush=True)
    for b in args.bins:
        print("# %s=%s" % (names[b], b), flush=True)
    runs = []  # (round, bin, offers, result)
    for i in range(args.pairs):
        for b in args.bins:
            for o in offers:
                root = os.path.join(args.wal, "cluster-%d-%d-%s-%d" % (os.getpid(), i, names[b], o))
                r = run_once(b, args, o, root)
                runs.append((i, b, o, r))
                print(describe("run round=%d" % i, names[b], o, r), flush=True)
    print("# summary (medians over valid runs)")
    for b in args.bins:
        for o in offers:
            ok = [r for (_, bb, oo, r) in runs if bb == b and oo == o and not r["invalid"]]
            if not ok:
                print("%s offers=%d: no valid runs" % (names[b], o))
                continue
            cpu = [sum(v["cpu"] for v in r["node"].values()) for r in ok]
            per = [c * 1e6 / o for c in cpu]
            busy = [sum(v["cpu"] for v in r["node"].values()) / max(v["wall"] for v in r["node"].values()) for r in ok]
            line = "%s offers=%d valid=%d cpu_us_per_write=%.1f busy=%.3f" % (
                names[b], o, len(ok), median(per), median(busy))
            for n in (1, 2, 3):
                line += " n%d_cpu_us_per_write=%.1f n%d_busy=%.3f" % (
                    n, median([r["node"][n]["cpu"] * 1e6 / o for r in ok]),
                    n, median([r["node"][n]["cpu"] / r["node"][n]["wall"] for r in ok]))
                for k in ("poll_rounds", "sends", "received", "persistence_batches"):
                    line += " n%d_%s_per_write=%.3f" % (n, SHORT[k], median(
                        [int(r["node"][n]["counters"][k]) / o for r in ok]))
            line += " rate=%.1f p99_us=%.1f" % (
                median([float(r["summary"]["applied_total_ops_s"]) for r in ok]),
                median([float(r["summary"]["p99_us"]) for r in ok]))
            print(line)
    if len(offers) >= 2:
        small, big = offers[0], offers[-1]
        print("# marginal CPU per write, (CPU at %d - CPU at %d) / %d, per round" % (big, small, big - small))
        marg = {}
        for b in args.bins:
            rows = []
            for i in range(args.pairs):
                rs = {o: r for (ii, bb, o, r) in runs if ii == i and bb == b}
                if any(rs[o]["invalid"] for o in (small, big)):
                    continue
                row = {n: (rs[big]["node"][n]["cpu"] - rs[small]["node"][n]["cpu"]) * 1e6 / (big - small)
                       for n in (1, 2, 3)}
                row["total"] = sum(row[n] for n in (1, 2, 3))
                rows.append(row)
            marg[b] = rows
            if rows:
                print("%s marginal_us_per_write total=%.2f n1=%.2f n2=%.2f n3=%.2f rounds=%d all=[%s]" % (
                    names[b], median([r["total"] for r in rows]),
                    *[median([r[n] for r in rows]) for n in (1, 2, 3)], len(rows),
                    " ".join("%.2f" % r["total"] for r in rows)))
        if len(args.bins) == 2:
            a, c = args.bins
            ratios = [rc["total"] / ra["total"] for ra, rc in zip(marg[a], marg[c]) if ra["total"]]
            if ratios:
                print("B/A marginal ratio median=%.3f pairs=[%s]" % (
                    median(ratios), " ".join("%.3f" % x for x in ratios)))
    elif len(args.bins) == 2:
        a, c = args.bins
        o = offers[0]
        ratios = []
        for i in range(args.pairs):
            ra = next(r for (ii, bb, oo, r) in runs if ii == i and bb == a and oo == o)
            rc = next(r for (ii, bb, oo, r) in runs if ii == i and bb == c and oo == o)
            if not ra["invalid"] and not rc["invalid"]:
                ratios.append(sum(v["cpu"] for v in rc["node"].values()) / sum(v["cpu"] for v in ra["node"].values()))
        if ratios:
            print("B/A total CPU ratio median=%.3f pairs=[%s]" % (median(ratios), " ".join("%.3f" % x for x in ratios)))
    return 0


if __name__ == "__main__":
    sys.exit(main())
