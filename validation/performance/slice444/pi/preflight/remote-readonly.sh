set +e
run() {
 printf '\ncommand:'
 printf ' %s' "$@"
 printf '\n'
 "$@"
 result=$?
 printf 'command_exit=%s\n' "$result"
}
run date -u '+%Y-%m-%dT%H:%M:%SZ'
if [ -r /proc/device-tree/model ]; then run cat /proc/device-tree/model; printf '\n'; fi
run uname -a
run cat /etc/os-release
run getconf LONG_BIT
run lscpu
run free -h
run swapon --show
run df -h / "$HOME" /tmp
run findmnt -T /
run findmnt -T "$HOME"
run lsblk -o NAME,TYPE,SIZE,FSTYPE,MOUNTPOINTS,MODEL,ROTA
run uptime
run cat /proc/loadavg
for policy in /sys/devices/system/cpu/cpufreq/policy*; do
 [ -d "$policy" ] || continue
 for field in affected_cpus scaling_governor scaling_cur_freq scaling_min_freq scaling_max_freq cpuinfo_min_freq cpuinfo_max_freq; do
  [ -r "$policy/$field" ] && run cat "$policy/$field"
 done
done
for zone in /sys/class/thermal/thermal_zone*; do
 [ -d "$zone" ] || continue
 [ -r "$zone/type" ] && run cat "$zone/type"
 [ -r "$zone/temp" ] && run cat "$zone/temp"
done
if command -v vcgencmd >/dev/null 2>&1; then
 run vcgencmd version
 run vcgencmd get_throttled
 run vcgencmd measure_temp
 run vcgencmd measure_clock arm
fi
for tool in rustc cargo rustup protoc cc gcc clang make cmake pkg-config git python3 perl perf apt apt-get dpkg pacman dnf yum; do
 printf '\ntool=%s\n' "$tool"
 command -v "$tool"
 printf 'availability_exit=%s\n' "$?"
done
for tool in rustc cargo rustup protoc cc gcc clang make cmake pkg-config git python3 perl perf; do
 if command -v "$tool" >/dev/null 2>&1; then run "$tool" --version; fi
done
if [ -x "$HOME/.cargo/bin/rustc" ]; then run "$HOME/.cargo/bin/rustc" -Vv; fi
if [ -x "$HOME/.cargo/bin/cargo" ]; then run "$HOME/.cargo/bin/cargo" -V; fi
if [ -x "$HOME/.cargo/bin/rustup" ]; then run "$HOME/.cargo/bin/rustup" show; fi
run ps -eo comm,pcpu,pmem --sort=-pcpu
if command -v systemctl >/dev/null 2>&1; then run systemctl --no-pager --plain list-units --type=service --state=running; fi
