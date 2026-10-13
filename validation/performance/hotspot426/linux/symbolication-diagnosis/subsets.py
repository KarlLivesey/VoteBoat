"""Exact subset counts from full saved stacks and per-address local API symbols.
These are sample weights, never clone/allocation call or byte counts.
"""
from collections import Counter
import gzip
import json
from pathlib import Path
import re

ROOT = Path(__file__).resolve().parent
ENTRY_EQ = "<voteboat::log::LogEntry as core::slice::cmp::SlicePartialEq<voteboat::log::LogEntry>>::equal_same_length"
MATCH = "<voteboat_matched_framework::store::MemoryStore as voteboat::log::LogStore>::matches_state"


def project(name):
    return name.startswith(("voteboat::", "<voteboat::", "<&voteboat::", "voteboat_matched_framework::", "<voteboat_matched_framework::"))


for cell in ["bulk", "singleton"]:
    directory = ROOT / f"{cell}-symbolicated"
    summary = json.loads((directory / "SUMMARY.json").read_text())
    data = json.load(gzip.open(summary["raw_profile"], "rt"))
    symbols = {(x["library"], x["address"]): x for x in json.loads((directory / "ADDRESS_SYMBOLS.json").read_text())}
    counts, parents, leaf_synthetic, clone_self, allocation_self = Counter(), Counter(), Counter(), Counter(), Counter()
    for thread in data["threads"]:
        if thread["name"] not in {"voteboat-matche", "matched-client", "matched-server"}:
            continue
        frames, functions, resources = thread["frameTable"], thread["funcTable"], thread["resourceTable"]
        cache = {}

        def stack_names(stack):
            if stack in cache:
                return cache[stack]
            chain = []
            cursor = stack
            while cursor is not None:
                frame = thread["stackTable"]["frame"][cursor]
                function = frames["func"][frame]
                resource = functions["resource"][function]
                library = resources["lib"][resource] if resource is not None else None
                info = symbols.get((library, frames["address"][frame]), {})
                physical = info.get("function", "[unresolved]")
                expanded = [physical, *(x["function"] for x in reversed(info.get("inlines", [])))]
                chain.append((physical, expanded))
                cursor = thread["stackTable"]["prefix"][cursor]
            names = [name for _, expansion in reversed(chain) for name in expansion]
            result = names, chain[0][0] if chain else "[null stack]"
            cache[stack] = result
            return result

        for stack, weight in zip(thread["samples"]["stack"], thread["samples"]["weight"], strict=True):
            names, leaf = stack_names(stack)
            eq = ENTRY_EQ in names
            compact = any(name.startswith("voteboat::snapshot::compact_replica::<") for name in names)
            matches = any(name == MATCH or name.startswith(MATCH + "::{closure#") for name in names)
            clone = any("::clone" in name or "<[_]>::to_vec_in::ConvertVec>::to_vec" in name for name in names)
            allocation = any(name.startswith(("alloc::alloc::", "<alloc::alloc::", "__rustc::__rust_alloc", "__rustc::__rust_dealloc")) or name in {"malloc", "calloc", "realloc", "free"} for name in names)
            for label, selected in [("entry_eq", eq), ("compaction", compact), ("matches_state", matches),
                                    ("entry_eq_and_compaction", eq and compact), ("entry_eq_without_compaction", eq and not compact),
                                    ("matches_state_and_compaction", matches and compact), ("matches_state_without_compaction", matches and not compact),
                                    ("compaction_without_entry_eq", compact and not eq), ("clone_related", clone),
                                    ("allocation_deallocation_related", allocation), ("clone_without_compaction", clone and not compact)]:
                counts[label] += weight if selected else 0
            if re.fullmatch(r"fun_[0-9a-fA-F]+", leaf):
                leaf_synthetic[leaf] += weight
            if "::clone" in leaf or "<[_]>::to_vec_in::ConvertVec>::to_vec" in leaf:
                clone_self[leaf] += weight
            if leaf.startswith(("alloc::alloc::", "<alloc::alloc::", "__rustc::__rust_alloc", "__rustc::__rust_dealloc")) or leaf in {"malloc", "calloc", "realloc", "free"}:
                allocation_self[leaf] += weight
            if eq and not compact:
                before = names[:names.index(ENTRY_EQ)]
                path = []
                for name in before:
                    if project(name) and (not path or path[-1] != name):
                        path.append(name)
                parents[tuple(path) or ("[no project caller symbol]",)] += weight
    n = summary["denominator"]
    assert counts["entry_eq"] == counts["entry_eq_and_compaction"] + counts["entry_eq_without_compaction"]
    assert counts["matches_state"] == counts["matches_state_and_compaction"] + counts["matches_state_without_compaction"]
    assert counts["compaction"] == counts["entry_eq_and_compaction"] + counts["compaction_without_entry_eq"]
    assert sum(parents.values()) == counts["entry_eq_without_compaction"]
    result = {"denominator": n, "unit": "saved Samply application-thread sample weights",
              "predicates": {"entry_eq": ENTRY_EQ + " present in native/per-address-inline stack",
                             "compaction": "function starts voteboat::snapshot::compact_replica::<",
                             "matches_state": MATCH + " or its closure present",
                             "clone_related": "clone/to_vec function context present; not clone-call count",
                             "allocation_deallocation_related": "alloc::alloc/Global allocation, rust allocation shim or malloc/calloc/realloc/free context; not allocation count"},
              "subsets": {name: {"sample_weight": value, "percent_of_all_selected_samples": 100 * value / n} for name, value in counts.items()},
              "entry_eq_outside_compaction_top5_project_ancestries": [{"root_to_eq_parent": list(path), "sample_weight": value,
                     "percent_of_all_selected_samples": 100 * value / n,
                     "percent_of_entry_eq_without_compaction": 100 * value / counts["entry_eq_without_compaction"]} for path, value in parents.most_common(5)],
              "synthetic_FDE_boundary_leaf_names": [{"function": name, "sample_weight": value, "percent": 100 * value / n} for name, value in leaf_synthetic.most_common()],
              "clone_related_native_self": [{"function": name, "sample_weight": value, "percent": 100 * value / n} for name, value in clone_self.most_common(10)],
              "allocation_related_native_self": [{"function": name, "sample_weight": value, "percent": 100 * value / n} for name, value in allocation_self.most_common(10)],
              "limitations": ["Subset intersections are explicit; matches_state and compaction overlap and must not be summed.",
                              "fun_<hex> names identify FDE boundaries, not known semantic symbols; raw names remain unchanged.",
                              "Source/inline locations are per-address API results rather than coalesced sidecar chains.",
                              "Allocation/clone contexts are sampled stacks, not actual call counts or allocated bytes.",
                              "Capture loss and launch-to-exit scope from SUMMARY.json remain applicable."]}
    (directory / "SUBSETS.json").write_text(json.dumps(result, indent=2) + "\n")
    print(cell, {name: round(value["percent_of_all_selected_samples"], 3) for name, value in result["subsets"].items()})
    print("outside eq top5", [(path, value) for path, value in parents.most_common(5)])
