"""Four fresh native default/Witness TCP/QUIC functional histories, not timing."""
from pathlib import Path
import collections,csv,datetime,hashlib,json,subprocess,time
job=Path(__file__).resolve().parent
sha=lambda p:hashlib.sha256(Path(p).read_bytes()).hexdigest()
save=lambda p,v:Path(p).write_text(json.dumps(v,indent=2)+'\n')
source=json.loads((job/'SOURCE.json').read_text())
assert source['commit']==(job/'REQUIRED_COMMIT').read_text().strip()
artifacts=json.loads((job/'ARTIFACTS.json').read_text())
artifact=next(r for r in artifacts if r['target']=='native_benchmark')
binary=Path(artifact['copy']);assert sha(binary)==artifact['sha256']
roots=job/'control-wal';roots.mkdir()
result={'scope':'Four fresh defaultFileLogIo/Witness x TCP/QUIC q64/two-group31:1 original Create/Recover functional histories; no performance acceptance','artifact_sha256':artifact['sha256'],'start_utc':datetime.datetime.now(datetime.timezone.utc).isoformat(),'cases':[]}
save(job/'CONTROLS.json',result)
def run(name,args):
    started=time.monotonic();save(job/'logs'/(name+'.command.json'),args)
    with(job/'logs'/(name+'.stdout')).open('xb')as out,(job/'logs'/(name+'.stderr')).open('xb')as err:
        p=subprocess.run(args,stdout=out,stderr=err)
    receipt={'command':args,'exit':p.returncode,'elapsed_s':time.monotonic()-started}
    save(job/'logs'/(name+'.exit.json'),receipt);assert p.returncode==0,receipt
    return receipt
def crc(data):
    value=0xffffffff
    for byte in data:
        value^=byte
        for _ in range(8):value=(value>>1)^(0x82f63b78 if value&1 else 0)
    return value^0xffffffff
for selection in ['default','witness']:
    for protocol in ['tcp','quic']:
        name=selection+'-'+protocol+'-q64-g2-w4096';root=roots/name
        command=[str(binary),str(root),protocol,'512','4096','2','--offered','128','--maintenance','2','--ready-items','64','--offered-capacity','4096','--group-weights','31,1']
        if selection=='witness':command.extend(['--publication','witness'])
        print('START',name,flush=True)
        process=run(name,command)
        validator=run(name+'-validator',['node',str(job/'source/validation/check-native-benchmark.mjs'),'--storage',str(root)])
        summary=dict(token.split('=',1)for token in(root/'summary.txt').read_text().split())
        assert summary['offers']==summary['applied']=='512'
        assert all(summary[k]=='0'for k in ['window_refused','no_leader','admission_refused','not_proposed','unknown','pending'])
        assert summary['ready_visit_items_requested']==summary['ready_visit_items_effective']=='64'
        assert summary['groups']=='2'and summary['workload_group_weights']=='31,1'
        assert summary['leader_placement']=='1:1,2:1'and summary['warmup']=='64'
        assert summary['offered_capacity_effective']=='4096'and summary['recovered_value']=='576'
        assert summary['retry_verified']==summary['workers_joined']=='true'
        assert int(summary['checkpoints'])>0 and int(summary['reclaims'])>0
        with(root/'offers.csv').open()as f:ledger=list(csv.DictReader(f))
        assert len(ledger)==512 and sorted(int(r['operation'])for r in ledger)==list(range(65,577))
        assert collections.Counter(r['status']for r in ledger)=={'applied':512}
        assert collections.Counter(r['group']for r in ledger)=={'1':496,'2':16}
        lines=(job/'logs'/(name+'.stderr')).read_text().splitlines()
        capacity=[x for x in lines if x.startswith('capacity_readback ')]
        publication=[x for x in lines if x.startswith('publication_readback ')]
        assert len(capacity)==6
        for mode in ['Create','Recover']:
            for replica in [1,2,3]:assert sum('mode='+mode+' 'in x and'replica='+str(replica)+' 'in x for x in capacity)==1
        assert all('offered_capacity_effective=4096 'in x and'client_group_requests=4096 'in x and'shard_group_items=4112 'in x for x in capacity)
        with(root/'storage.csv').open()as f:storage=list(csv.DictReader(f))
        assert all(int(row['errors'])==0 for row in storage)
        joined=[r for r in storage if r['stage'].endswith('_join')]
        assert len(joined)==6 and all(int(r['encoded_bytes'])>0 for r in joined)
        physical=[]
        if selection=='witness':
            assert len(publication)==6 and(root/'publication.csv').is_file()
            assert summary['publication_requested']==summary['publication_effective']=='witness'
            assert summary['publication_provider']=='WitnessLogIo'and summary['publication_timings']=='native'
            assert summary['publication_scope']=='ordinary_barrier'and summary['storage_error_scope']=='overlapping_observations'
            for mode in ['Create','Recover']:
                for replica in [1,2,3]:assert sum('mode='+mode+' 'in x and'replica='+str(replica)+' 'in x for x in publication)==1
            assert all('publication_effective=witness 'in x and'publication_scope=ordinary_barrier 'in x for x in publication)
            for replica in [1,2,3]:
                selector=root/('replica'+str(replica))/'WITNESS-CURRENT';assert selector.is_file()
                physical.append({'replica':replica,'selector_sha256':sha(selector),'witness_selector_present':True})
        else:
            assert not publication and not(root/'publication.csv').exists()
            assert not any(k.startswith('publication_')for k in summary)
            for replica in [1,2,3]:
                directory=root/('replica'+str(replica));selector=directory/'CURRENT';current=selector.read_bytes()
                assert len(current)==20 and current[:8]==b'VBLCUR01'and crc(current[:16])==int.from_bytes(current[16:],'little')
                assert not(directory/'WITNESS-CURRENT').exists()
                generation=int.from_bytes(current[8:16],'little')
                wal=directory/('log.wal'if generation==0 else 'log.'+str(generation%2)+'.wal')
                metadata=directory/('MANIFEST'if generation==0 else 'MANIFEST.'+str(generation%2));ack=metadata.read_bytes()
                assert len(ack)==52 and ack[:8]==b'VBLSTR02'and crc(ack[:48])==int.from_bytes(ack[48:],'little')
                assert int.from_bytes(ack[40:48],'little')==wal.stat().st_size
                physical.append({'replica':replica,'generation':generation,'selector_sha256':sha(selector),'wal_sha256':sha(wal),'wal_bytes':wal.stat().st_size,'manifest_sha256':sha(metadata),'witness_selector_present':False})
        result['cases'].append({'name':name,'selection':selection,'protocol':protocol,'process':process,'validator':validator,'summary':summary,'capacity_readbacks':capacity,'publication_readbacks':publication,'physical':physical,'ledger_status_counts':{'applied':512}})
        save(job/'CONTROLS.json',result);print('END',name,0,flush=True)
assert sha(binary)==artifact['sha256']
result.update({'exit':0,'artifact_unchanged':True,'end_utc':datetime.datetime.now(datetime.timezone.utc).isoformat()})
save(job/'CONTROLS.json',result)
