"""Finite paired design only; importing or printing the plan starts no workload."""
CELLS = [
    ('singleton', 256, 8, 1),
    ('moderate', 8192, 128, 256),
    ('bulk', 8192, 512, 4096),
    ('overload', 8192, 1024, 4096),
]

def cases(binary, wal_parent, metadata_parent, python, resource_script):
    result = []
    for trial in range(3):
        order = list(range(4))[trial:] + list(range(4))[:trial]
        for cell_index in order:
            cell, offers, rate, window = CELLS[cell_index]
            protocols = ['tcp', 'quic'] if (trial + cell_index) % 2 == 0 else ['quic', 'tcp']
            for protocol in protocols:
                first = 'atomic' if (trial + cell_index + ['tcp', 'quic'].index(protocol)) % 2 == 0 else 'witness'
                arms = [first, 'witness' if first == 'atomic' else 'atomic']
                pair = f't{trial+1}-{cell}-{protocol}'
                for arm in arms:
                    name = pair + '-' + arm
                    root = str(wal_parent / name)
                    metadata = str(metadata_parent / name)
                    command = [str(binary), root, protocol, str(offers), str(window), '1',
                               '--offered', str(rate), '--maintenance', '2', '--ready-items', '16',
                               '--offered-capacity', '4096', '--publication', arm]
                    result.append({'name': name, 'pair': pair, 'trial': trial+1, 'cell': cell,
                                   'protocol': protocol, 'publication': arm, 'offers': offers,
                                   'rate': rate, 'window': window, 'horizon_seconds': offers/rate,
                                   'wal_root': root, 'metadata': metadata, 'command': command,
                                   'resource_command': [str(python), str(resource_script), metadata, *command]})
    assert len(result) == 48 and len({r['name'] for r in result}) == 48
    return result
