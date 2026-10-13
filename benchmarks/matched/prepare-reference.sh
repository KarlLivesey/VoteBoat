#!/usr/bin/env bash
# SPDX-License-Identifier: RPL-1.5
set -euo pipefail
pin=e88bb40707f2e511d8ecc44d31edcb1d814a11a8
source_dir=${1:?Usage: prepare-reference.sh NEW_SOURCE_DIRECTORY}
if [[ -e "$source_dir" ]]; then
  printf 'Refusing an existing destination: %s\n' "$source_dir" >&2
  exit 1
fi
git clone --no-checkout https://github.com/databendlabs/openraft.git "$source_dir"
git -C "$source_dir" checkout --detach "$pin"
[[ $(git -C "$source_dir" rev-parse HEAD) == "$pin" ]]
[[ -z $(git -C "$source_dir" status --porcelain) ]]
printf 'Pinned source prepared. No dependencies fetched or builds run.\n'
printf 'After the quiet campaign, resolve and preserve each independent Cargo.lock, then build with --locked.\n'
printf 'Untouched upstream manifest: %s/benchmarks/minimal/Cargo.toml\n' "$source_dir"
