"""Independent raw-ledger readout; run only after both actual timing terminals."""
import collections
import csv
import hashlib
import json
import math
from pathlib import Path

BASE = Path(__file__).resolve().parent
inputs = {}
records = []


def read(path):
    raw = path.read_bytes()
    inputs[str(path)] = {"sha256": hashlib.sha256(raw).hexdigest(), "bytes": len(raw)}
    return raw.decode()


def quantiles(values):
    ordered = sorted(values)
    if not ordered:
        return {key: None for key in ("p50_ms", "p95_ms", "p99_ms", "max_ms")}
    result = {f"p{p}_ms": ordered[(len(ordered) * p + 99) // 100 - 1] / 1e6
              for p in (50, 95, 99)}
    result["max_ms"] = ordered[-1] / 1e6
    return result


for host, root in [("linux", BASE / "linux"), ("mac", BASE / "mac-collection/raw")]:
    terminal = json.loads(read(root / "TERMINAL.json"))
    assert terminal["exit"] == 0 and terminal["all_timing_ended"]
    campaign = json.loads(read(root / "campaign/CAMPAIGN.json"))
    assert len(campaign["cases"]) == 48 and not campaign["post_timing_errors"]
    for case in campaign["cases"]:
        assert case["exit"] == case["validator_exit"] == case["readbacks_exit"] == 0
        wal = Path(case["wal_root"]) if host == "linux" else root / "results" / case["name"]
        summary = dict(value.split("=", 1) for value in read(wal / "summary.txt").split())
        rows = list(csv.DictReader(read(wal / "offers.csv").splitlines()))
        statuses = collections.Counter(row["status"] for row in rows)
        assert len(rows) == case["offers"] and len({r["operation"] for r in rows}) == len(rows)
        assert set(statuses) <= {"applied", "window_refused"}
        applied = [row for row in rows if row["status"] == "applied"]
        assert len(applied) == int(summary["applied"]) == int(summary["admitted"])
        assert int(summary["recovered_value"]) == 64 + len(applied)
        assert summary["retry_verified"] == summary["workers_joined"] == "true"
        assert int(summary["ready_visit_items_requested"]) == int(summary["ready_visit_items_effective"]) == case["ready_items"]
        elapsed, horizon = int(summary["elapsed_ns"]), int(summary["horizon_ns"])
        during = sum(int(row["completed_ns"]) < horizon for row in applied)
        backlog = sum(int(row["dispatched_ns"]) < horizon <= int(row["completed_ns"]) for row in applied)
        assert during == int(summary["applied_during"])
        assert backlog == int(summary["backlog_at_horizon"])
        groups = {}
        for group in sorted({row["group"] for row in rows}):
            offered = [row for row in rows if row["group"] == group]
            done = [row for row in applied if row["group"] == group]
            groups[group] = {
                "offers": len(offered), "applied": len(done),
                "statuses": dict(collections.Counter(row["status"] for row in offered)),
                "during": sum(int(row["completed_ns"]) < horizon for row in done),
                "backlog": sum(int(row["dispatched_ns"]) < horizon <= int(row["completed_ns"]) for row in done),
                "quantiles": quantiles([int(row["completed_ns"]) - int(row["intended_ns"]) for row in done]),
            }
        latency = quantiles([int(row["completed_ns"]) - int(row["intended_ns"]) for row in applied])
        assert math.isclose(latency["p99_ms"], float(summary["p99_us"]) / 1000, abs_tol=.0000011)
        storage = list(csv.DictReader(read(wal / "storage.csv").splitlines()))
        by_stage = {(row["stage"], row["replica"]): row for row in storage}
        physical = {}
        for replica in ("1", "2", "3"):
            before = by_stage[("before_measurement", replica)]
            after = by_stage[("after_measurement", replica)]
            fields = ("append_commands", "append_units", "barrier_calls", "barrier_ns", "sync_calls", "sync_ns", "publish_calls", "publish_ns", "errors")
            delta = {field: int(after[field]) - int(before[field]) for field in fields}
            assert all(value >= 0 for value in delta.values()) and delta["errors"] == 0
            physical[replica] = delta
        records.append({
            "host": host, "case": case["name"], "pair": case["pair"],
            "arm": case["arm"], "cell": case["cell"], "protocol": case["protocol"],
            "trial": case["trial"], "ready_items": case["ready_items"],
            "offers": len(rows), "applied": len(applied), "statuses": dict(statuses),
            "completed_ops_s": len(applied) * 1e9 / elapsed,
            "during_ops_s": during * 1e9 / horizon, "backlog": backlog,
            "quantiles": latency, "groups": groups, "physical": physical,
            "checkpoints": int(summary["checkpoints"]), "reclaims": int(summary["reclaims"]),
        })

output = {"scope": "Independent recomputation of all96 original ledgers after both actual terminals. No workload repeats. Native elapsed and sampled CPU scopes remain distinct.", "inputs": inputs, "records": records}
(BASE / "ROOT_DATA_REVIEW.json").write_text(json.dumps(output, indent=2) + "\n")
for host in ("linux", "mac"):
    selected = [record for record in records if record["host"] == host]
    print(host, "cases", len(selected), "offers", sum(r["offers"] for r in selected),
          "applied", sum(r["applied"] for r in selected))
