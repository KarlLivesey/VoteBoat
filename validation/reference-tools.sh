#!/bin/bash
# SPDX-License-Identifier: RPL-1.5
# Test-only raft-proto 0.7 uses protoc 3.x; its bundled Mac compiler is x86-only.
set -euo pipefail
if [[ "$(uname -s)" != Darwin || "$(uname -m)" != arm64 ]]; then exit 0; fi
if [[ -n "${PROTOC:-}" ]] && "$PROTOC" --version | grep -Eq '^libprotoc 3\.'; then
  printf '%s\n' "$PROTOC"
  exit 0
fi
tools_root="${VOTEBOAT_REFERENCE_TOOLS:-${TMPDIR:-/tmp}/voteboat-reference-tools-${UID}}"
mkdir -p "$tools_root"
compiler="$tools_root/bin/protoc"
compiler_hash=9f40368c858666e7036df0b9b58a4acd085fed58ab531294c3e5b5820f8af360
if [[ -f "$compiler" ]] && printf '%s  %s\n' "$compiler_hash" "$compiler" | shasum -a 256 -c >/dev/null 2>&1; then
  printf '%s\n' "$compiler"
  exit 0
fi
archive="$tools_root/protoc-21.12-osx-aarch_64.zip"
curl -fsSL https://github.com/protocolbuffers/protobuf/releases/download/v21.12/protoc-21.12-osx-aarch_64.zip -o "$archive"
printf '%s  %s\n' 96839af0caed64352442fc8236f4bdf7c1cd6efcfaa98fa5db37307a73fc7c70 "$archive" | shasum -a 256 -c >&2
unzip -q -o "$archive" -d "$tools_root"
printf '%s  %s\n' "$compiler_hash" "$compiler" | shasum -a 256 -c >&2
"$compiler" --version >&2
printf '%s\n' "$compiler"
