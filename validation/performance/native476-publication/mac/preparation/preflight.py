"""Read-only remote preflight. No source transfer, build, workload or settings."""
import hashlib
import json
import os
import plistlib
import subprocess
from pathlib import Path

env = os.environ.copy()
env['PATH'] = '/Users/karllivesey/.cargo/bin:/opt/homebrew/bin:/tmp/voteboat-macos-CzK1if/reference-tools296/bin:/usr/bin:/bin:/usr/sbin:/sbin'
protoc = '/tmp/voteboat-macos-CzK1if/reference-tools296/bin/protoc'
records = []
def run(command):
    result = subprocess.run(command, env=env, capture_output=True)
    records.append({'command': command, 'exit': result.returncode,
                    'stdout': result.stdout.decode(), 'stderr': result.stderr.decode()})
    assert result.returncode == 0, records[-1]
    return result.stdout

run(['/usr/bin/sw_vers'])
run(['/usr/bin/uname', '-sm'])
run(['/usr/sbin/sysctl', '-n', 'machdep.cpu.brand_string', 'hw.physicalcpu', 'hw.logicalcpu', 'hw.memsize'])
cargo = run(['/Users/karllivesey/.cargo/bin/cargo', '+stable', '-V']).decode()
rustc = run(['/Users/karllivesey/.cargo/bin/rustc', '+stable', '-Vv']).decode()
baseline = Path('/tmp/voteboat-mac455-7a99ae5-fENMzQ')
assert cargo == (baseline/'logs/cargo.txt').read_text()
assert rustc == (baseline/'logs/rustc.txt').read_text()
paths = [run(['/Users/karllivesey/.cargo/bin/rustup', 'which', '--toolchain', 'stable', tool]).decode().strip()
         for tool in ['rustc', 'cargo']] + [protoc]
hashes = {p: hashlib.sha256(Path(p).read_bytes()).hexdigest() for p in paths}
old_hashes = {line.split(maxsplit=1)[1]: line.split(maxsplit=1)[0]
              for line in (baseline/'logs/tool-sha256.txt').read_text().splitlines()}
assert hashes == old_hashes
assert run([protoc, '--version']).decode().strip() == 'libprotoc 3.21.12'
run(['node', '--version'])
device = run(['/bin/df', '-P', '/private/tmp']).decode().splitlines()[-1].split()[0]
volume = plistlib.loads(run(['/usr/sbin/diskutil', 'info', '-plist', device]))
assert str(volume.get('FilesystemType', '')).lower() == 'apfs'
container = volume.get('APFSContainerReference') or volume.get('ContainerReference')
apfs = plistlib.loads(run(['/usr/sbin/diskutil', 'apfs', 'list', '-plist', container]))
selected = next(row for row in apfs['Containers']
                if row.get('ContainerReference') == container or row.get('APFSContainerReference') == container)
physical = []
for store in selected['PhysicalStores']:
    detail = plistlib.loads(run(['/usr/sbin/diskutil', 'info', '-plist', store['DeviceIdentifier']]))
    physical.append({key: detail.get(key) for key in ['DeviceIdentifier', 'ParentWholeDisk', 'PartOfWhole', 'BusProtocol', 'MediaName', 'SolidState', 'DiskSize', 'TotalSize']})
processes = run(['/bin/ps', '-U', 'karllivesey', '-o', 'pid=,ppid=,comm=']).decode()
live = []
for line in processes.splitlines():
    name = Path(line.split()[-1]).name
    if name in ['cargo', 'rustc', 'native_benchmark', 'voteboat-matched-framework', 'samply', 'perf'] or name.startswith('native_benchmark-'):
        live.append(line)
assert not live, live
print(json.dumps({'scope': 'Read-only Mac476 preflight only; no source copy/build/test/timing/settings changes',
    'commands': records, 'tool_sha256': hashes, 'tool_bytes_equal_original455': True,
    'filesystem': {'scratch': '/private/tmp', 'device': device, 'volume': {k: volume.get(k) for k in
       ['DeviceIdentifier', 'MountPoint', 'VolumeName', 'FilesystemType', 'APFSContainerReference', 'VolumeFreeSpace']},
       'container': container, 'physical_stores': selected['PhysicalStores'], 'physical_store_info': physical,
       'placement': 'All three voter roots share this APFS volume/container/physical store; not independent placement.'},
    'live_owned_build_benchmark_profile_jobs': live}, indent=2))
