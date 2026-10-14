ssh karllivesey@192.168.192.76 bash -s -- /tmp/voteboat-native442-590f6e7-Il5nTW <<'MAC'
set -euo pipefail
job="$1"
chmod 600 "$job/source.tar"
mkdir "$job/source"
tar -xpf "$job/source.tar" -C "$job/source"
/usr/bin/python3 "$job/verify-source.py" > "$job/source-transfer.log" 2>&1
printf 'BUILD_START %s\n' "$job"
exec bash "$job/run-build.sh"
MAC
