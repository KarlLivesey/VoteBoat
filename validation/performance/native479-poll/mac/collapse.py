"""Offline Mac Samply 0.13.1 conversion. Raw inputs are never modified.

CPU weights are sampled threadCPUDelta microseconds, not wall percentages.
Per-address JSON replies from the owned headless localhost server supply matching476 symbols. Primary totals use physical native frames; debug0 limits source/inline precision.
"""
import argparse
from collections import Counter
import gzip
import hashlib
import json
from pathlib import Path


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def normalized_uuid(value):
    # These Mac profiles use UUID debug IDs with Breakpad age zero.
    value = value.replace("-", "").lower()
    assert len(value) == 32 or (len(value) == 33 and value.endswith("0")), value
    return value[:32]


def label(value):
    # Preserve exact originals in JSON; folded format reserves semicolons/newlines.
    return value.replace(";", "∷").replace("\n", " ").replace("\r", " ")


def rank(counter, total):
    return [{"function": name, "cpu_us": weight,
             "percent_of_observed_selected_thread_cpu": 100 * weight / total}
            for name, weight in counter.most_common()]


def convert(profile_path, symbols_path, output):
    with gzip.open(profile_path, "rt") as stream:
        profile = json.load(stream)
    address_symbols = json.loads(symbols_path.read_text())
    symbols = {(row["library"], row["address"]):row for row in address_symbols}
    collapsed_cpu, collapsed_wall = Counter(), Counter()
    self_cpu, inclusive_cpu, source_frames = Counter(), Counter(), Counter()
    rows, exclusions, exact_labels = [], [], {}
    physical_stacks, callers = Counter(), Counter()
    native_pids={t["pid"] for t in profile["threads"] if "native_benchmark" in t["name"]}
    assert len(native_pids)==1, native_pids
    totals = Counter()
    for thread in profile["threads"]:
        name = thread["name"]
        if thread["pid"] not in native_pids:
            exclusions.append({"name": name, "tid": thread["tid"]})
            continue
        samples, stacks, frames = thread["samples"], thread["stackTable"], thread["frameTable"]
        functions, resources = thread["funcTable"], thread["resourceTable"]
        assert samples["weightType"] == "samples"
        assert all(len(samples[key]) == samples["length"]
                   for key in ["stack", "weight", "threadCPUDelta"])
        row = Counter(samples=samples["length"], cpu_us=0, wall_sample_weight=0)
        frame_cache, stack_cache = {}, {}

        def resolve_frame(index):
            if index in frame_cache:
                return frame_cache[index]
            function = frames["func"][index]
            resource = functions["resource"][function]
            raw = thread["stringArray"][functions["name"][function]]
            library = None
            if resource is not None and resource >= 0:
                library_slot = resources["lib"][resource]
                if library_slot is not None and library_slot >= 0:
                    library = profile["libs"][library_slot]
            info = None
            if library is not None:
                info = symbols.get((library_slot, frames["address"][index]))
            if info is None or "function" not in info:
                native = "[unresolved] " + (library["name"] + "!" if library else "") + raw
                result = ([native], native, True, [])
            else:
                native = info["function"]
                inline = [item["function"] for item in reversed(info.get("inlines") or [])]
                result = ([native, *inline], native, False, [])
            frame_cache[index] = result
            return result

        def resolve_stack(index):
            if index in stack_cache:
                return stack_cache[index]
            if index is None:
                return (["[null stack]"], ["[null stack]"], True, [], False)
            chain, seen = [], set()
            cursor = index
            malformed = False
            while cursor is not None:
                if cursor in seen or not isinstance(cursor, int) or not 0 <= cursor < stacks["length"]:
                    malformed = True
                    break
                seen.add(cursor)
                chain.append(stacks["frame"][cursor])
                cursor = stacks["prefix"][cursor]
            expanded, native, sources, unresolved = [], [], [], False
            if malformed:
                expanded.append("[truncated malformed prefix]")
            for frame in reversed(chain):
                if not isinstance(frame, int) or not 0 <= frame < frames["length"]:
                    expanded.append("[invalid frame]")
                    native.append("[invalid frame]")
                    unresolved = malformed = True
                    continue
                inline, physical, unknown, locations = resolve_frame(frame)
                expanded.extend(inline)
                native.append(physical)
                sources.extend(locations)
                unresolved |= unknown
            result = (expanded or ["[empty stack]"], native or ["[empty stack]"], unresolved, sources, malformed)
            stack_cache[index] = result
            return result

        root = f"{name} tid={thread['tid']}"
        for index, stack in enumerate(samples["stack"]):
            cpu, wall = samples["threadCPUDelta"][index], samples["weight"][index]
            assert isinstance(wall, int) and wall >= 0
            expanded, native, unresolved, sources, malformed = resolve_stack(stack)
            for item in expanded:
                exact_labels[label(item)] = item
            folded = ";".join([root, *(label(item) for item in expanded)])
            collapsed_wall[folded] += wall
            row["wall_sample_weight"] += wall
            if cpu is None:
                row["null_cpu_delta_samples"] += 1
                continue
            assert isinstance(cpu, int) and cpu >= 0
            row["cpu_us"] += cpu
            collapsed_cpu[folded] += cpu
            if not cpu:
                row["zero_cpu_delta_samples"] += 1
                continue
            row["positive_cpu_samples"] += 1
            row["null_stack_cpu_us"] += cpu if stack is None else 0
            row["unresolved_leaf_cpu_us"] += cpu if native[-1].startswith("[unresolved]") else 0
            row["any_unresolved_stack_cpu_us"] += cpu if unresolved else 0
            row["malformed_stack_cpu_us"] += cpu if malformed else 0
            physical_stacks[tuple(native)] += cpu
            for edge in set(zip(native[:-1],native[1:])):callers[edge] += cpu
            self_cpu[native[-1]] += cpu
            # Count each function once per sampled stack, so recursion does not
            # inflate its inclusive numerator. Different functions still overlap.
            for function in set(native):
                inclusive_cpu[function] += cpu
            for source in sources:
                source_frames[json.dumps(source, sort_keys=True)] += cpu
        rows.append({"name": name, "tid": thread["tid"], "pid": thread["pid"], **dict(row)})
        totals.update(row)
    total = totals["cpu_us"]
    assert total > 0 and sum(self_cpu.values()) == total
    assert sum(collapsed_cpu.values()) == total
    assert sum(collapsed_wall.values()) == totals["wall_sample_weight"]
    output.mkdir()
    for filename, values in [("cpu.folded", collapsed_cpu), ("wall-sample-weights.folded", collapsed_wall)]:
        (output / filename).write_text("".join(f"{key} {weight}\n" for key, weight in sorted(values.items()) if weight))
    result = {"profile": str(profile_path.resolve()), "symbols": str(symbols_path.resolve()),
              "input_sha256": {profile_path.name: digest(profile_path), symbols_path.name: digest(symbols_path)},
              "cpu_unit": "threadCPUDelta microseconds attributed to sampled stack",
              "cpu_denominator_us": total, "wall_unit": "Samply sample weights, includes off-CPU waits",
              "totals": dict(totals), "threads": rows, "excluded_threads": exclusions,
              "self_native_functions": rank(self_cpu, total), "inclusive_native_functions": rank(inclusive_cpu, total),
              "physical_callstacks": [{"root_to_leaf":list(stack),"cpu_us":weight,"percent":100*weight/total}for stack,weight in physical_stacks.most_common()],
              "caller_contexts": [{"caller":edge[0],"callee":edge[1],"cpu_us":weight,"percent":100*weight/total}for edge,weight in callers.most_common()],
              "representative_source_frames": [{**json.loads(source), "inclusive_cpu_us_nonadditive": weight}
                                               for source, weight in source_frames.most_common()],
              "limits": ["Launch-to-exit native profile includes setup, warmup, measurement, recovery/retry verification, latency sorting and shutdown; not the throughput timer boundary.",
                         "CPU deltas are interval observations assigned to sampled stacks, not instruction-level cost or elapsed wall percentages.",
                         "Physical native frames use per-address localhost symbolicate/v5 responses. Debug0 limits source and inline precision; unknown frames remain explicit.",
                         "Sampled stack unwinder truncation not explicitly marked in these files cannot be quantified; null/malformed/unresolved observations are retained and counted.",
                         "Native thread names do not identify leader/follower ownership; main-owner versus WAL/network worker CPU split remains per actual names only.",
                         "Inclusive function shares overlap and must not be summed; self native-function shares sum to the observed CPU denominator."]}
    (output / "SUMMARY.json").write_text(json.dumps(result, indent=2) + "\n")
    (output / "LABEL_ORIGINALS.json").write_text(json.dumps(exact_labels, indent=2) + "\n")
    print(json.dumps({"output": str(output), "threads": len(rows), "cpu_us": total,
                      "folded_stacks": sum(bool(v) for v in collapsed_cpu.values()), "totals": dict(totals)}, sort_keys=True))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("profile", type=Path)
    parser.add_argument("symbols", type=Path)
    parser.add_argument("output", type=Path)
    args = parser.parse_args()
    convert(args.profile, args.symbols, args.output)
