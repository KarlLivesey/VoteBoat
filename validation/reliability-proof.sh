#!/usr/bin/env bash
# SPDX-License-Identifier: RPL-1.5
# Direct-binary qualification; no retries, ignored cases or foreign processes.
set -euo pipefail
config=${1:?usage: reliability-proof.sh CONFIG SUITE... (bin:NAME supported)}
shift
features=()
case "$config" in
  default) ;;
  all-features) features=(--all-features) ;;
  no-default-features) features=(--no-default-features) ;;
  native) features=(--no-default-features --features native) ;;
  *) echo "unknown configuration: $config" >&2; exit 2 ;;
esac
export CARGO_TARGET_DIR=${CARGO_TARGET_DIR:-target/reliability-$config}
export CARGO_BUILD_JOBS=${CARGO_BUILD_JOBS:-2}
output=${RELIABILITY_OUTPUT:-target/tr1b/proof/$config}
mkdir -p "$output"
output=$(cd "$output" && pwd)
summary=$output/results.tsv
if [[ -e "$summary" ]]; then
  echo "Refusing to overwrite proof results: $summary" >&2
  exit 2
fi
printf 'suite\tenvironment\trepetition\tcopy\texit\tseconds\tcommit\n' > "$summary"
commit=$(git rev-parse HEAD)
burners=()
children=()
cleanup() {
  local pid
  for pid in "${children[@]}" "${burners[@]}"; do
    kill "$pid" 2>/dev/null || true
    wait "$pid" 2>/dev/null || true
  done
}
trap cleanup EXIT
# Exec the test directly: a named shell between nice and exec can be reprioritized
# by a host policy daemon. Observe only our child, without changing host policy.
launch() {
  local log=$1 constrained=$2
  shift 2
  if [[ "$constrained" == yes ]]; then
    taskset -c 0-3 nice -n 19 "$@" > "$log" 2>&1 &
  else
    "$@" > "$log" 2>&1 &
  fi
  launched=$!
  children+=("$launched")
}
observe() {
  local pid=$1 expected=$2 metadata=$3 constrained=$4 observed= start=$SECONDS actual state
  expected=$(realpath "$expected")
  printf 'pid=%s constrained=%s executable=%s\n' "$pid" "$constrained" "$expected" > "$metadata"
  cat /proc/loadavg >> "$metadata"
  while [[ -r /proc/$pid/stat ]] && (( SECONDS - start < 5 )); do
    read -r _ _ state _ < "/proc/$pid/stat" || break
    if [[ "$state" == Z ]]; then break; fi
    actual=$(readlink "/proc/$pid/exe" 2>/dev/null || true)
    if [[ "$actual" == "$expected" ]]; then
      ps -o pid=,ni=,comm= -p "$pid" >> "$metadata" || true
      taskset -pc "$pid" >> "$metadata" 2>&1 || true
      observed=yes
      break
    fi
  done
  if [[ "$observed" != yes ]]; then echo 'completed before sample, or startup not yet observed' >> "$metadata"; fi
}
record() {
  local pid=$1 env=$2 rep=$3 copy=$4 start=$5 status=0
  wait "$pid" || status=$?
  local remaining=() child
  for child in "${children[@]}"; do
    if [[ "$child" != "$pid" ]]; then remaining+=("$child"); fi
  done
  children=("${remaining[@]}")
  printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\n' "$suite" "$env" "$rep" "$copy" "$status" "$((SECONDS-start))" "$commit" >> "$summary"
  if (( status != 0 )); then failed=1; fi
}
for suite in "$@"; do
  kind=--test
  name=$suite
  if [[ "$suite" == bin:* ]]; then kind=--bin; name=${suite#bin:}; fi
  cargo +stable test --locked --offline "${features[@]}" "$kind" "$name" --no-run --message-format=json > "$output/$name-build.jsonl" 2> "$output/$name-build.log"
  exe=$(jq -r --arg name "$name" 'select(.reason == "compiler-artifact" and .target.name == $name and .profile.test == true) | .executable // empty' "$output/$name-build.jsonl")
  [[ -x "$exe" ]]
  failed=0
  for env in E1 E2 E3 E4 E5; do
    reps=10
    args=()
    constrained=yes
    case "$env" in
      E2) reps=5; args=(--test-threads=1) ;;
      E3)
        for cpu in 1 2 3; do
          taskset -c "$cpu" sh -c 'while :; do :; done' &
          burners+=("$!")
        done ;;
      E4) reps=5 ;;
      E5) reps=3; constrained=no ;;
    esac
    for ((rep=1; rep<=reps; rep++)); do
      children=()
      start=$SECONDS
      launch "$output/$name-$env-$rep-a.log" "$constrained" "$exe" "${args[@]}"
      first=$launched
      if [[ "$env" == E4 ]]; then
        launch "$output/$name-$env-$rep-b.log" yes "$exe"
        second=$launched
        observe "$first" "$exe" "$output/$name-$env-$rep-a.log.meta" yes
        observe "$second" "$exe" "$output/$name-$env-$rep-b.log.meta" yes
        record "$first" "$env" "$rep" a "$start"
        record "$second" "$env" "$rep" b "$start"
      else
        observe "$first" "$exe" "$output/$name-$env-$rep-a.log.meta" "$constrained"
        record "$first" "$env" "$rep" a "$start"
      fi
      children=()
      # A failure needs diagnosis and a focused fix, never an unchanged rerun.
      if (( failed )); then exit 1; fi
    done
    for pid in "${burners[@]}"; do kill "$pid"; wait "$pid" 2>/dev/null || true; done
    burners=()
    echo "$config $suite $env: $reps repetitions passed"
  done
done
