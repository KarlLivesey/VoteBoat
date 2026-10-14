#!/usr/bin/env bash
set -euo pipefail
job=$1
trap 'rc=$?; printf "%s\n" "$rc" > "$job/RESUME_STATUS"' EXIT
export RUSTUP_TOOLCHAIN=1.98.1
export PATH="$HOME/.rustup/toolchains/1.98.1-aarch64-unknown-linux-gnu/bin:$HOME/.cargo/bin:/usr/local/bin:/usr/bin:/bin:/usr/sbin:/sbin"
run(){ name=$1;shift;printf '%q ' "$@" > "$job/logs/$name.command";printf '\n' >> "$job/logs/$name.command";set +e;"$@" > "$job/logs/$name.stdout" 2> "$job/logs/$name.stderr";result=$?;set -e;printf '%s\n' "$result" > "$job/logs/$name.exit";printf 'COMMAND_END=%s exit=%s\n' "$name" "$result";return "$result"; }
printf '%s\n' artifact_guards > "$job/RESUME_STAGE"
run artifacts-final python3 "$job/guard.py" "$job" artifacts
run source-after-build python3 "$job/guard.py" "$job" source
printf '0\n' > "$job/BUILD_STATUS";printf 'DONE\n' > "$job/BUILD_STAGE"
printf '%s\n' current_cli > "$job/RESUME_STAGE"
run current-cli python3 "$job/cli.py"
run source-after-cli python3 "$job/guard.py" "$job" source
run memory-after free -h
run storage-after df -h "$job" /tmp
run temperature-after vcgencmd measure_temp
run throttled-after vcgencmd get_throttled
run frequency-after vcgencmd measure_clock arm
run governor-after cat /sys/devices/system/cpu/cpufreq/policy0/scaling_governor
run services-after systemctl is-active apcupsd docker containerd
run load-after uptime
printf 'DONE\n' > "$job/RESUME_STAGE"
