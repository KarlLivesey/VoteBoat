set -eu
export PATH="$HOME/.cargo/bin:/usr/local/bin:/usr/bin:/bin:/usr/sbin:/sbin"
job=$(mktemp -d "$HOME/voteboat-p9-pi443-XXXXXX")
printf 'REMOTE_JOB=%s\n' "$job"
mkdir -p "$job/tools/packages" "$job/tools/protobuf" "$job/tool-logs"
trap 'rc=$?; printf "%s\n" "$rc" > "$job/TOOL_STATUS"' EXIT
run() {
 name=$1
 shift
 printf 'START %s\n' "$name"
 printf '%s\n' "$*" > "$job/tool-logs/$name.command"
 set +e
 "$@" > "$job/tool-logs/$name.stdout" 2> "$job/tool-logs/$name.stderr"
 result=$?
 set -e
 cat "$job/tool-logs/$name.stdout"
 cat "$job/tool-logs/$name.stderr" >&2
 printf '%s\n' "$result" > "$job/tool-logs/$name.exit"
 printf 'END %s exit=%s\n' "$name" "$result"
 return "$result"
}
run default-before rustup show active-toolchain
run toolchains-before rustup toolchain list
if rustup toolchain list | grep -q '^1\.98\.1-aarch64-unknown-linux-gnu'; then
 printf 'Rust1.98.1 alreadyinstalled; no installation\n'
else
 run install-rust1981 rustup toolchain install 1.98.1 --profile minimal
fi
run rustc1981 rustup run 1.98.1 rustc -Vv
run cargo1981 rustup run 1.98.1 cargo -V
run rustc-path rustup which --toolchain 1.98.1 rustc
run cargo-path rustup which --toolchain 1.98.1 cargo
run default-after rustup show active-toolchain
cmp "$job/tool-logs/default-before.stdout" "$job/tool-logs/default-after.stdout"
printf 'TOOL_STAGE=protobuf\n'
cd "$job/tools/packages"
run download-protobuf apt-get download protobuf-compiler=3.21.12-11+deb13u1 libprotoc32t64=3.21.12-11+deb13u1
printf '%s\n' 'a873f75cbdf8054a23687e7c9b39ca1a93545edd22a581f4caa560756678f658  protobuf-compiler_3.21.12-11+deb13u1_arm64.deb' '0cf431ed1f7d48d6acd52e1532bc3f7c17048a737115dc9eb900edfa7a08ed7a  libprotoc32t64_3.21.12-11+deb13u1_arm64.deb' > "$job/tool-logs/packages.expected.sha256"
run packages-sha sha256sum -c "$job/tool-logs/packages.expected.sha256"
run extract-compiler dpkg-deb -x protobuf-compiler_3.21.12-11+deb13u1_arm64.deb "$job/tools/protobuf"
run extract-library dpkg-deb -x libprotoc32t64_3.21.12-11+deb13u1_arm64.deb "$job/tools/protobuf"
cat > "$job/tools/protoc" <<'WRAPPER'
#!/bin/sh
set -eu
tools=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
exec env LD_LIBRARY_PATH="$tools/protobuf/usr/lib/aarch64-linux-gnu" "$tools/protobuf/usr/bin/protoc" "$@"
WRAPPER
chmod 755 "$job/tools/protoc"
run protoc-version "$job/tools/protoc" --version
run protoc-elf readelf -h "$job/tools/protobuf/usr/bin/protoc"
run protoc-dynamic readelf -d "$job/tools/protobuf/usr/bin/protoc"
run protoc-loader env "LD_LIBRARY_PATH=$job/tools/protobuf/usr/lib/aarch64-linux-gnu" ldd "$job/tools/protobuf/usr/bin/protoc"
run installed-protobuf dpkg-query -W libprotobuf32t64
rustc_path=$(rustup which --toolchain 1.98.1 rustc)
cargo_path=$(rustup which --toolchain 1.98.1 cargo)
run tool-hashes sha256sum "$rustc_path" "$cargo_path" "$job/tools/protoc" "$job/tools/protobuf/usr/bin/protoc" "$job/tools/protobuf/usr/lib/aarch64-linux-gnu/libprotoc.so.32.0.12"
run filesystem df -h "$HOME" /tmp
run memory free -h
run load uptime
run selected-processes ps -eo pid,user,etime,stat,comm,pcpu --sort=-pcpu
if command -v vcgencmd >/dev/null 2>&1; then
 run temperature vcgencmd measure_temp
 run throttled vcgencmd get_throttled
 run arm-frequency vcgencmd measure_clock arm
fi
run governor cat /sys/devices/system/cpu/cpufreq/policy0/scaling_governor
printf '%s\n' DONE > "$job/TOOL_STAGE"
printf 'TOOLS_SETUP_END job=%s\n' "$job"
