from pathlib import Path
import hashlib, json, sys
p = Path(__file__).resolve().parent
workspace = p.parents[2]
base = workspace / 'target/divide-provider/drain391/repo'
selected = json.loads((p / 'SOURCE.json').read_text())
original = json.loads((p / 'BASE_SOURCE.json').read_text())
sha = lambda f: hashlib.sha256(f.read_bytes()).hexdigest()
owned = ['tests/logical_nested_native/' + n for n in ['read.rs','requests.rs']]
result = {
    'inputs': len(selected),
    'copied_differences': [n for n, v in selected.items() if sha(p / 'repo' / n) != v],
    'original391_source_differences': [n for n, v in original.items() if sha(base / n) != v],
    'current_owned_differences': [n for n in owned if sha(workspace / n) != selected[n]],
    'production_differences': [n for n in original if n.startswith('src/') and selected[n] != original[n]],
}
caches = json.loads((p / 'CACHE_SOURCE_GUARDS.json').read_text())
result['terminal_cache_source_differences'] = [n for cache in caches for n,v in cache['artifacts'].items() if sha(Path(n)) != v]
(p / (sys.argv[1] + '-guard.json')).write_text(json.dumps(result, indent=2) + '\n')
print(json.dumps(result), flush=True)
sys.exit(any(v for k, v in result.items() if k != 'inputs'))
