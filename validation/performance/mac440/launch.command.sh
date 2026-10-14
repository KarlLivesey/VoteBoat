ssh karllivesey@192.168.192.76 bash -s -- /tmp/voteboat-mac440-fc4be25-7YJQl3 <<'MAC'
set -euo pipefail
job="$1"
mkdir "$job/source"
tar -xpf "$job/source.tar" -C "$job/source"
/usr/bin/python3 "$job/verify-source.py" > "$job/source-transfer.log" 2>&1
printf 'QUALIFICATION_START %s\n' "$job"
exec bash "$job/run.sh"
MAC
