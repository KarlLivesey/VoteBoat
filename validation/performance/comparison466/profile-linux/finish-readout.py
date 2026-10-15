"""Assemble existing466 numeric results and provenance; does not run tools."""
from pathlib import Path
from collections import Counter
import hashlib
import json

ROOT = Path(__file__).resolve().parents[4]
JOB = Path(__file__).resolve().parent
RAW = JOB / "perf-quiet"
SYM = JOB / "symbolication"


def digest(path):
    with path.open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


record = json.loads((RAW / "RECORDING.json").read_text())
assert all(digest(Path(record[key])) == record[key + "_sha256"] for key in ["binary", "dwp", "perf"])
policies = {name: int((Path("/proc/sys/kernel") / name).read_text()) for name in ["perf_event_paranoid", "kptr_restrict", "perf_event_mlock_kb"]}
assert policies == record["settings_before"] == record["settings_after"] == {"perf_event_paranoid": 2, "kptr_restrict": 2, "perf_event_mlock_kb": 516}
result = {
    "source_commit": "92454c26cfe35aa65034eb3c306e990d42d88a80",
    "ELF_sha256": record["binary_sha256"], "DWP_sha256": record["dwp_sha256"],
    "scope": "User cycles:u1000Hz/DWARF16384/mmap128 whole launch including warmup/verification/sorting/shutdown. Sample counts are not cycles/CPUus/throughput-timer CPU or paired capacity evidence.",
    "capture_original": 31284, "capture_actual_exit": 0,
    "current_symbol_queries_original": 3736, "current_symbol_queries_actual_exit": 0,
    "headless_server_original": 74816, "headless_server_actual_exit": 0,
    "policy_unchanged": policies, "cases": {}, "baseline455": {},
    "conclusion": "Old slice-to-Vec symbol shrinks but actual completion iterator materialization and singleton history binary lookup remain visible replacement work. No percentage is claimed as removable; paired regressions remain authoritative. No optimization or allocator cause selected."
}
for case, reported in [("bulk", 6241), ("singleton", 23187)]:
    directory = SYM / (case + "-symbolicated")
    summary = json.loads((directory / "SUMMARY.json").read_text())
    ownership = json.loads((directory / "OWNERSHIP_CONTEXTS.json").read_text())
    diagnostic = json.loads((RAW / (case + ".stdout")).read_text())
    threads = Counter()
    for row in summary["threads"]:
        threads[row["name"]] += row["sample_weight"]
    result["cases"][case] = {
        "reported_perf_samples": reported, "selected_sample_weight": summary["denominator"],
        "null_samples": summary["totals"]["null_stack_sample_weight"],
        "unresolved_leaf_sample_weight": summary["totals"]["unresolved_leaf_sample_weight"],
        "unresolved_leaf_percent": 100 * summary["totals"]["unresolved_leaf_sample_weight"] / summary["denominator"],
        "synthetic_FDE_leaf_sample_weight": ownership["synthetic_FDE_leaf_sample_weight"],
        "synthetic_FDE_note": "Raw fun_ names are non-source-resolved FDE labels, separately disclosed; no allocator identity guessed.",
        "shallow_depth_lte2_weight": ownership["physical_depth_lte2_sample_weight"],
        "reported_lost_chunks": 1 if case == "bulk" else 0,
        "warnings": {name: (RAW / (case + "." + name)).read_text() for name in ["stderr", "import.stderr", "perf-script.stderr"]},
        "stack_quality_limit": "Finite DWARF16KiB stacks. Exact truncation count unavailable; no lost samples reconstructed. Bulk import zero-timestamp ordering warning retained.",
        "thread_group_weights": dict(threads), "top15_physical_self": summary["self_native"][:15],
        "inclusive_ownership_contexts": ownership["contexts_inclusive_unions"],
        "history_get_self_callers": ownership["history_get_physical_self_direct_project_callers"],
        "history_get_self_source_bindings": ownership["history_get_physical_leaf_source_bindings"],
        "iterator_materialization_callers": ownership["iterator_materialization_callchains"],
        "recording_diagnostic_only": {"completed_originals": diagnostic["completed_originals"], "completed_requests": diagnostic["completed_requests"], "applied_frontiers": diagnostic["applied_frontiers"], "requested": diagnostic["settings"]["operations"], "warmup": diagnostic["settings"]["warmup"], "profiled_originals_per_second": diagnostic["operations_per_second"], "request_p99_ns": diagnostic["request_p99_ns"]},
        "raw_perf_local": {"path": str(RAW / (case + ".data")), "bytes": (RAW / (case + ".data")).stat().st_size, "sha256": digest(RAW / (case + ".data"))},
        "numeric_profile": {"path": str(RAW / (case + ".json.gz")), "sha256": digest(RAW / (case + ".json.gz"))}
    }
    base = ROOT / "target/p9/profile455/linux/symbolication" / (case + "-symbolicated") / "SUMMARY.json"
    baseline = json.loads(base.read_text())
    result["baseline455"][case] = {
        "summary_path": str(base), "summary_sha256": digest(base),
        "selected_sample_weight": baseline["denominator"], "own_profile_sha256": baseline["raw_profile_sha256"],
        "slice_entry_to_Vec_physical_self": [row for row in baseline["self_native"] if "LogEntry as <[_]>::to_vec_in::ConvertVec" in row["function"]],
        "scope": "Separate455 capture and own IDs/RVAs. No455 response reused for466 or sample percentages treated as CPU-rate/speedup evidence."
    }
artifacts = json.loads((ROOT / "target/p9/matched455-linux/ARTIFACTS.json").read_text())
result["baseline455"]["ELF_sha256"] = next(row["sha256"] for row in artifacts if row["target"] == "voteboat-matched-framework")
result["baseline455"]["DWP_sha256"] = json.loads((ROOT / "target/p9/matched455-linux/SYMBOLS.json").read_text())["sha256"]
(JOB / "READOUT.json").write_text(json.dumps(result, indent=2) + "\n")
(JOB / "RAW_PERF_LOCATIONS.json").write_text(json.dumps({name: row["raw_perf_local"] for name, row in result["cases"].items()}, indent=2) + "\n")
(SYM / "FINAL_PROCESSING_STATUS.json").write_text(json.dumps({"capture": 0, "imports": [0, 0], "current_symbol_queries": [0, 0], "saved_perf_script": [0, 0], "subsets": 0, "client_contexts": 0, "ownership_contexts": 0, "server_original": 74816, "server_actual_exit": 0, "policies": policies, "original_failures_preserved": "../processing-attempt1 and PROCESSING_FAILURES.json", "no_browser": True, "no_new_capture_or_build": True}, indent=2) + "\n")
print("READOUT_READY", digest(JOB / "READOUT.json"))
