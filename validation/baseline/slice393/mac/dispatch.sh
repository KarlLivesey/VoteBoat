#!/bin/bash
set -u
qual_base=$1
qual_prior=/tmp/voteboat-macos-read394-vuYC17yB
mkdir -p "$qual_base/results"
cd "$qual_base" || exit 2
shasum -a 256 -c TRANSFER_SHA256.txt > results/transfer_initial.log 2>&1 || exit 2
shasum -a 256 -c DISPATCH_SHA256.txt > results/dispatch_initial.log 2>&1 || exit 2
shasum -a 256 -c "$qual_prior/results/binary_terminal.log" > results/old394_binary_before.log 2>&1 || exit 2
( cd "$qual_prior/repo" && shasum -a 256 -c ../SOURCE_SHA256.txt ) > results/old394_source_before.log 2>&1 || exit 2
tar -xzf MAC_SOURCE.tar.gz || exit 2
cd repo || exit 2
bash ../checks-Mac.sh > ../results/driver.log 2>&1
qual_case_exit=$?
cd .. || exit 2
shasum -a 256 -c "$qual_prior/results/binary_terminal.log" > results/old394_binary_after.log 2>&1 || exit 2
( cd "$qual_prior/repo" && shasum -a 256 -c ../SOURCE_SHA256.txt ) > results/old394_source_after.log 2>&1 || exit 2
shasum -a 256 -c DISPATCH_SHA256.txt > results/dispatch_terminal.log 2>&1 || exit 2
printf 'native_driver_exit=%s\nold_cache_guard=0\n' "$qual_case_exit" > results/DISPATCH_TERMINAL.txt
exit "$qual_case_exit"
