#!/bin/bash
set -euo pipefail
# Exact compilation/fixture/metadata universe; no pack, handoff, target or Git copy.
find src tests examples -type f ! -path '*/target/*' -print
for file in Cargo.toml Cargo.lock rust-toolchain.toml clippy.toml rustfmt.toml \
    .rustfmt.toml build.rs AGENTS.md LICENSE .githooks/pre-push \
    docs/component-contracts.json docs/provider-conformance.json; do
    if [ -f "$file" ]; then printf '%s\n' "$file"; fi
done
find validation -maxdepth 1 -type f \( -name '*.mjs' -o -name '*.sh' \) -print
node -e 'const fs=require("fs"); const i=JSON.parse(fs.readFileSync("docs/component-contracts.json")); for (const file of new Set(i.contracts.flatMap(c=>c.conformance))) { if (!fs.statSync(file).isFile()) throw new Error(`missing conformance input ${file}`); console.log(file); }'
if [ -d .cargo ]; then find .cargo -type f -print; fi
