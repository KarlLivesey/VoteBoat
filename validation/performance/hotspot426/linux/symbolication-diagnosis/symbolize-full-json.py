"""Read saved Samply physical stacks and symbolize through localhost JSON only.

No browser, sampling, external HTTP, binaries, source or raw profiles are changed.
Linux weights are the saved Samply sample counts, not perf-cycle periods or CPU us.
"""
import argparse
from collections import Counter
import gzip
import hashlib
import json
from pathlib import Path
import urllib.parse
import urllib.request


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def rank(values, denominator):
    return [{"function": name, "sample_weight": count, "percent_of_selected_sample_weight": 100 * count / denominator}
            for name, count in values.most_common()]


def analyze(profile_path, endpoint, output):
    assert urllib.parse.urlparse(endpoint).hostname == "127.0.0.1"
    assert endpoint.endswith("/symbolicate/v5")
    with gzip.open(profile_path, "rt") as stream:
        profile = json.load(stream)
    selected = [t for t in profile["threads"] if t["name"] in {"voteboat-matche", "matched-client", "matched-server"}]
    addresses = set()

    def frame_address(thread, frame):
        function = thread["frameTable"]["func"][frame]
        resource = thread["funcTable"]["resource"][function]
        if resource is None or resource < 0:
            return None
        library = thread["resourceTable"]["lib"][resource]
        address = thread["frameTable"]["address"][frame]
        return (library, address) if library is not None and library >= 0 and address >= 0 else None

    for thread in selected:
        for frame in range(thread["frameTable"]["length"]):
            point = frame_address(thread, frame)
            if point is not None:
                addresses.add(point)
    output.mkdir()
    response_map = {}
    ordered = sorted(addresses)
    memory_map = [[lib["debugName"], lib["breakpadId"]] for lib in profile["libs"]]
    for start in range(0, len(ordered), 256):
        batch = ordered[start:start + 256]
        request = {"memoryMap": memory_map, "stacks": [[[library, address]] for library, address in batch]}
        filename = f"batch-{start // 256:03}"
        (output / f"{filename}.request.json").write_text(json.dumps(request) + "\n")
        connection = urllib.request.Request(endpoint, data=json.dumps(request).encode(), headers={"Content-Type": "application/json"})
        with urllib.request.urlopen(connection, timeout=60) as stream:
            response = stream.read()
        (output / f"{filename}.response.json").write_bytes(response)
        stacks = json.loads(response)["results"][0]["stacks"]
        assert len(stacks) == len(batch)
        for point, frames in zip(batch, stacks, strict=True):
            assert len(frames) == 1
            info = frames[0]
            assert int(info["module_offset"], 16) == point[1]
            response_map[point] = info

    self_native, inclusive_native, self_inline, inclusive_inline = Counter(), Counter(), Counter(), Counter()
    folded, physical_stacks, histogram, totals = Counter(), Counter(), Counter(), Counter()
    rows = []
    for thread in selected:
        sample, table = thread["samples"], thread["stackTable"]
        assert sample["weightType"] == "samples"
        row = Counter(samples=sample["length"], sample_weight=0)
        cache = {}

        def resolve(stack):
            if stack in cache:
                return cache[stack]
            if stack is None:
                return ["[null stack]"], ["[null stack]"], [], 0, True
            frames, seen = [], set()
            cursor = stack
            while cursor is not None:
                assert isinstance(cursor, int) and 0 <= cursor < table["length"] and cursor not in seen
                seen.add(cursor)
                frames.append(table["frame"][cursor])
                cursor = table["prefix"][cursor]
            native, expanded, infos, missing = [], [], [], False
            for frame in reversed(frames):
                point = frame_address(thread, frame)
                info = response_map.get(point, {})
                if "function" not in info:
                    function = thread["frameTable"]["func"][frame]
                    raw = thread["stringArray"][thread["funcTable"]["name"][function]]
                    name = "[unresolved] " + (profile["libs"][point[0]]["name"] + "!" if point else "") + raw
                    missing = True
                else:
                    name = info["function"]
                native.append(name)
                expanded.append(name)
                # API inline chains are per-address, innermost-to-outermost.
                # Reverse into the machine stack's root-to-leaf order.
                expanded.extend(item["function"] for item in reversed(info.get("inlines", [])))
                infos.append({"library_address": point, **info})
            result = native, expanded, infos, len(frames), missing
            cache[stack] = result
            return result

        for stack, weight in zip(sample["stack"], sample["weight"], strict=True):
            assert isinstance(weight, int) and weight >= 0
            native, expanded, infos, depth, missing = resolve(stack)
            row["sample_weight"] += weight
            row["null_stack_sample_weight"] += weight if stack is None else 0
            row["unresolved_leaf_sample_weight"] += weight if native[-1].startswith("[unresolved]") else 0
            row["any_unresolved_stack_sample_weight"] += weight if missing else 0
            histogram[depth] += weight
            self_native[native[-1]] += weight
            self_inline[expanded[-1]] += weight
            for function in set(native):
                inclusive_native[function] += weight
            for function in set(expanded):
                inclusive_inline[function] += weight
            physical_stacks[tuple(native)] += weight
            folded[";".join([f"{thread['name']} tid={thread['tid']}", *(name.replace(';', '∷').replace('\n', ' ') for name in expanded)])] += weight
        rows.append({"name": thread["name"], "tid": thread["tid"], **dict(row)})
        totals.update(row)
    denominator = totals["sample_weight"]
    assert denominator == sum(self_native.values()) == sum(self_inline.values()) == sum(folded.values())
    address_records = [{"library": library, "address": address, **info} for (library, address), info in sorted(response_map.items())]
    (output / "ADDRESS_SYMBOLS.json").write_text(json.dumps(address_records, indent=2) + "\n")
    (output / "full-inline.folded").write_text("".join(f"{stack} {weight}\n" for stack, weight in sorted(folded.items()) if weight))
    result = {"raw_profile": str(profile_path.resolve()), "raw_profile_sha256": sha(profile_path),
              "weight": "Saved Samply on-CPU sample weight(count), not perf cycles or CPU microseconds", "denominator": denominator,
              "threads": rows, "totals": dict(totals), "physical_depth_sample_weights": dict(sorted(histogram.items())),
              "self_native": rank(self_native, denominator), "inclusive_native": rank(inclusive_native, denominator),
              "self_including_inlines": rank(self_inline, denominator), "inclusive_including_inlines": rank(inclusive_inline, denominator),
              "top_native_callstacks": [{"root_to_leaf": list(stack), "sample_weight": weight, "percent": 100 * weight / denominator}
                                        for stack, weight in physical_stacks.most_common(30)],
              "libraries": profile["libs"], "unique_symbol_queries": len(address_records),
              "limits": ["Launch-to-exit scope includes warmup, verification, sorting and cleanup, not only throughput timer.",
                         "Inclusive shares overlap; native and inline representations are separate views and must not be added.",
                         "Sample-frequency normalization differs from raw perf-period cycle weighting; no cross-host CPU-rate comparison.",
                         "Unresolved frames are retained. Four lost chunks reported by perf for bulk remain a capture limitation.",
                         "Thread labels do not identify leader/follower ownership; no leader-specific CPU attribution.",
                         "No browser used. All symbol queries used localhost Samply API with current frozen ELF plus original packed DWP."]}
    (output / "SUMMARY.json").write_text(json.dumps(result, indent=2) + "\n")
    print(json.dumps({"output": str(output), "denominator": denominator, "addresses": len(address_records),
                      "unresolved_leaf_percent": 100 * totals["unresolved_leaf_sample_weight"] / denominator,
                      "top5_self": rank(self_native, denominator)[:5]}, sort_keys=True))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("profile", type=Path)
    parser.add_argument("endpoint")
    parser.add_argument("output", type=Path)
    args = parser.parse_args()
    analyze(args.profile, args.endpoint, args.output)
