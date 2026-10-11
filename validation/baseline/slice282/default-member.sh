#!/bin/bash
set -e
repo=$1
results=$2
host=$3
cd "$repo"
if [ "$host" = mac ]; then
 export PATH="$HOME/.cargo/bin:$PATH"
 export TMPDIR=/tmp/voteboat-macos-CzK1if/test-roots
 export CARGO_TARGET_DIR=/tmp/voteboat-macos-CzK1if/repo277/target
 ulimit -n 4096
 hash=(shasum -a 256)
else
 hash=(sha256sum)
fi
"${hash[@]}" -c "$results/source.sha256" > "$results/$host-full-default-source.log"
set +e
cargo +stable test --locked --offline --test native_member_startup -- --nocapture > "$results/$host-default-member.log" 2>&1
member_exit=$?
set -e
printf 'member_default=%s\n' "$member_exit" >> "$results/$host-exits.txt"
"${hash[@]}" -c "$results/source.sha256" > "$results/$host-full-terminal-source.log"
exit "$member_exit"
