#!/bin/bash
set -e
repo=$1
results=$2
host=$3
cd "$repo"
if [ "$host" = mac ]; then
 export PATH="$HOME/.cargo/bin:$PATH"
 export TMPDIR=/tmp/voteboat-macos-CzK1if/test-roots
 ulimit -n 4096
 hash=(shasum -a 256)
else
 hash=(sha256sum)
fi
"${hash[@]}" -c "$results/source.sha256" > "$results/$host-source.log"
cargo +stable fmt --all -- --check > "$results/$host-fmt.log" 2>&1
printf '%s\n' 'fmt=0' > "$results/$host-exits.txt"
for profile in all default core native; do
 case "$profile" in
  all) flags=(--all-features);;
  default) flags=();;
  core) flags=(--no-default-features);;
  native) flags=(--no-default-features --features native);;
 esac
 cargo +stable clippy --locked --offline --keep-going --all-targets "${flags[@]}" -- -D warnings > "$results/$host-$profile-clippy.log" 2>&1
 printf '%s\n' "clippy_$profile=0" >> "$results/$host-exits.txt"
done
overall=0
for profile in all default; do
 if [ "$profile" = all ]; then flags=(--all-features); else flags=(); fi
 for scope in member; do
  set +e
  cargo +stable test --locked --offline "${flags[@]}" --test native_member_startup -- --nocapture > "$results/$host-$profile-$scope.log" 2>&1
  status=$?
  set -e
  printf '%s\n' "${scope}_${profile}=$status" >> "$results/$host-exits.txt"
  if [ "$status" -ne 0 ]; then overall=$status; fi
 done
done
"${hash[@]}" -c "$results/source.sha256" > "$results/$host-terminal-source.log"
exit "$overall"
