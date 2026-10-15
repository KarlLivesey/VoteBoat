"""Focused466 ownership predicates over already saved current-address responses.

Same physical/inline resolution as subsets.py; no server, capture or code change.
"""
from pathlib import Path
from collections import Counter
import gzip
import json

ROOT = Path(__file__).resolve().parent
output = {}
for cell in ["bulk", "singleton"]:
    directory = ROOT / (cell + "-symbolicated")
    summary = json.loads((directory / "SUMMARY.json").read_text())
    raw = json.load(gzip.open(summary["raw_profile"], "rt"))
    symbols = {(r["library"], r["address"]): r for r in json.loads((directory / "ADDRESS_SYMBOLS.json").read_text())}
    counts, parents, getlines, materialization = Counter(), Counter(), Counter(), Counter()
    denominator = summary["denominator"]
    for thread in raw["threads"]:
        if thread["name"] not in {"voteboat-matche", "matched-client", "matched-server"}:
            continue
        cache = {}

        def resolve(stack):
            if stack in cache:
                return cache[stack]
            native, expanded, infos = [], [], []
            cursor = stack
            while cursor is not None:
                frame = thread["stackTable"]["frame"][cursor]
                func = thread["frameTable"]["func"][frame]
                resource = thread["funcTable"]["resource"][func]
                lib = thread["resourceTable"]["lib"][resource] if resource is not None and resource >= 0 else None
                info = symbols.get((lib, thread["frameTable"]["address"][frame]), {})
                name = info.get("function", "[unresolved]")
                native.append(name)
                expanded.extend([name, *[r["function"] for r in info.get("inlines", [])]])
                infos.append(info)
                cursor = thread["stackTable"]["prefix"][cursor]
            cache[stack] = native, expanded, infos
            return cache[stack]

        for stack, weight in zip(thread["samples"]["stack"], thread["samples"]["weight"], strict=True):
            native, names, infos = resolve(stack)
            leaf = native[0] if native else "[null]"
            has = lambda text: any(text in name for name in names)
            selected = {
                "entry_history_any": has("log::entry_history::"),
                "entry_history_get": has("EntryHistory>::get"),
                "entry_iterator_next": any("entry_history::EntryIter" in n and ">::next" in n for n in names),
                "iterator_entry_materialization": any("Vec<voteboat::log::LogEntry>" in n and "SpecFromIter" in n and "EntryIter" in n for n in names),
                "slice_entry_materialization": has("LogEntry as <[_]>::to_vec_in::ConvertVec"),
                "entry_batch_capture": has("entry_batch::EntryBatch as core::convert::From"),
                "entry_backing_arc": has("Arc<voteboat::log::entry_batch::Backing>"),
                "history_descriptor_clone": has("EntryHistory as core::clone::Clone") or any("entry_history::Chunk" in n and "clone" in n for n in names),
                "history_descriptor_growth": has("EntryHistory>::ensure_additional") or any("entry_history::Chunk" in n and any(x in n for x in ["reserve", "grow", "allocate"]) for n in names),
                "history_prefix_discard": has("EntryHistory>::discard_prefix"),
                "range_coalescing": has("EntryRange>::to_cow") or has("EntryRange>::to_vec"),
                "owned_history_materialization": has("EntryHistory>::to_vec") or has("EntryHistory>::into_vec"),
                "pending_plan_copy": has("OwnedNextLog>::plan_copy"),
                "append_for": has("Raft>::append_for"),
                "canonical_completion": has("Raft>::complete_borrowed") or has("Raft>::borrow_completion"),
            }
            for label, yes in selected.items():
                counts[label] += weight if yes else 0
            if leaf == "<voteboat::log::entry_history::EntryHistory>::get":
                project = [n for n in native[1:] if "voteboat::" in n or "voteboat_matched_framework::" in n]
                parents[tuple(project[:5])] += weight
                for record in [infos[0], *infos[0].get("inlines", [])]:
                    getlines[(record.get("function"), record.get("file"), record.get("line"))] += weight
            if selected["iterator_entry_materialization"]:
                materialization[tuple(n for n in native if "voteboat::" in n or "voteboat_matched_framework::" in n)[:6]] += weight

    def rows(counter):
        return [{"frames": list(key), "sample_weight": count, "percent": 100 * count / denominator} for key, count in counter.most_common(8)]

    result = {
        "denominator": denominator,
        "contexts_inclusive_unions": {key: {"sample_weight": count, "percent": 100 * count / denominator} for key, count in counts.items()},
        "history_get_physical_self_direct_project_callers": rows(parents),
        "history_get_physical_leaf_source_bindings": rows(getlines),
        "iterator_materialization_callchains": rows(materialization),
        "top15_physical_self": summary["self_native"][:15],
        "threads": summary["threads"],
        "null_unresolved": summary["totals"],
        "physical_depth_lte2_sample_weight": sum(int(v) for k, v in summary["physical_depth_sample_weights"].items() if int(k) <= 2),
        "synthetic_FDE_leaf_sample_weight": sum(r["sample_weight"] for r in summary["self_native"] if r["function"].startswith("fun_")),
        "limits": ["Context rows are inclusive unions and overlap; do not add them as time or cycles.", "Resolved Arc/descriptor/coalescing frames are not an exhaustive partition of optimized work; zero samples do not establish zero allocations/refcounts.", "Whole-launch sample weights are not throughput-timer CPU costs or paired speedup evidence."]
    }
    (directory / "OWNERSHIP_CONTEXTS.json").write_text(json.dumps(result, indent=2) + "\n")
    output[cell] = result
    print(cell, "denominator", denominator, "contexts", json.dumps(result["contexts_inclusive_unions"]))
    print("get parents", json.dumps(result["history_get_physical_self_direct_project_callers"][:4]))
    print("materialization parents", json.dumps(result["iterator_materialization_callchains"][:4]))
(ROOT.parent / "OWNERSHIP_READOUT.json").write_text(json.dumps(output, indent=2) + "\n")
