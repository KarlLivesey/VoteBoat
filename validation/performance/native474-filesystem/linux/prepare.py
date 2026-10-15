from pathlib import Path
import json,copy,hashlib,subprocess,shutil
repo=Path.cwd();job=Path(__file__).resolve().parent;old=repo/'target/p9/native455-quantum64/linux';sha=lambda p:hashlib.sha256(Path(p).read_bytes()).hexdigest()
assert not (job/'CAMPAIGN_CONFIG.json').exists()
c=json.loads((old/'CAMPAIGN_CONFIG.json').read_text());template=next(x for x in c['cases'] if x['variant']=='quantum16');cases=[]
for protocol in ['tcp','quic']:
 for trial in range(1,4):
  order=['default','nocow'] if (trial+(protocol=='quic'))%2 else ['nocow','default']
  for variant in order:
   name=f'{protocol}-t{trial}-{variant}';case=copy.deepcopy(template);wal=job/('storage-'+variant)/name;metadata=job/'campaign-metadata'/name;metadata.mkdir(parents=True);collector=metadata/'resources.py';shutil.copy2(Path(template['metadata'])/'resources.py',collector)
   cmd=[c['baseline']['artifact']['copy'],str(wal),protocol,'15000','64','1','--offered','500','--maintenance','2','--ready-items','16'];case.update(name=name,variant=variant,protocol=protocol,trial=trial,wal_root=str(wal),metadata=str(metadata),command=cmd,resource_command=['python3',str(collector),*cmd],resource_collector_sha256=sha(collector));cases.append(case)
c.update(cases=cases,scope='Same immutable published455 native release, defaultBtrfs CoW/compressioneligible versus freshNoCoW/noCompression parents on SAME physical NVMe;onegroup3replicas/TCPQUIC/3pairedrotatingtrials/15000offers500persec/window64/ready16/active2smaintenance/noPause. Combinedfilesystempolicy diagnostic,notisolatedCoW/ext4/enginegain/fullcapacity/headroom.',timing_authorized=False,filesystem_setup={'path':str(job/'FILESYSTEM_SETUP.json'),'sha256':sha(job/'FILESYSTEM_SETUP.json')},reused_fixture_and_artifact_proof=str(old/'READY.json'))
(job/'CAMPAIGN_CONFIG.json').write_text(json.dumps(c,indent=2)+'\n')
s=(old/'campaign.py').read_text();(job/'campaign.py.original').write_text(s)
s=s.replace("guards()\nactive=", """def filesystem_guards(stage):
 rows=[]
 for variant in ['default','nocow']:
  parent=job/('storage-'+variant)
  result=subprocess.run(['lsattr','-d',str(parent)],capture_output=True,text=True)
  assert result.returncode==0
  attrs=result.stdout.split()[0];assert ('C' in attrs)==(variant=='nocow'),(variant,attrs)
  rows.append({'variant':variant,'path':str(parent),'attributes':attrs})
 (job/('FILESYSTEM_'+stage+'.json')).write_text(json.dumps(rows,indent=2)+'\\n')
 assert sha(job/'FILESYSTEM_SETUP.json')==config['filesystem_setup']['sha256']
filesystem_guards('BEFORE')
guards()
active=""")
s=s.replace(" guards()\n # Allworkload", " guards()\n filesystem_guards('AFTER')\n attrs=subprocess.run(['lsattr','-R',str(job/'storage-default'),str(job/'storage-nocow')],capture_output=True)\n (job/'FILESYSTEM_FILES.stdout').write_bytes(attrs.stdout);(job/'FILESYSTEM_FILES.stderr').write_bytes(attrs.stderr);(job/'FILESYSTEM_FILES.exit').write_text(str(attrs.returncode)+'\\n')\n assert attrs.returncode==0\n # Allworkload")
(job/'campaign.py').write_text(s)
# Record actual mount/device/frequency/security policy and fresh directory attributes before timing.
commands=[['findmnt','-T',str(job),'--json','-o','TARGET,SOURCE,FSTYPE,OPTIONS'],['lsblk','-o','NAME,TYPE,FSTYPE,SIZE,MOUNTPOINTS'],['uname','-a'],['lscpu']]
records=[]
for cmd in commands:
 r=subprocess.run(cmd,capture_output=True,text=True);records.append({'command':cmd,'exit':r.returncode,'stdout':r.stdout,'stderr':r.stderr});assert r.returncode==0
settings={}
for p in [Path('/proc/sys/kernel')/n for n in ['perf_event_paranoid','kptr_restrict','perf_event_mlock_kb']]:settings[str(p)]=p.read_text().strip()
for p in Path('/sys/devices/system/cpu/cpufreq').glob('policy*'):
 settings[str(p)]={n:(p/n).read_text().strip() for n in ['scaling_governor','energy_performance_preference','scaling_driver','affected_cpus'] if (p/n).exists()}
(job/'HOST_SETUP.json').write_text(json.dumps({'commands':records,'settings':settings,'topology':'All3 replica WALs on same physical NVMe; no deployment extrapolation. CPU policy remains unchanged acrosspairs; no causal attribution tofrequency.'},indent=2)+'\n')
ready={'state':'PREPARED_NOT_TIMED_AWAITING_FINISHED_CHECKS_PUBLICATION_REVIEW_QUIET','campaign_config_sha256':sha(job/'CAMPAIGN_CONFIG.json'),'campaign_driver_sha256':sha(job/'campaign.py'),'case_count':len(cases),'same_native_artifact_sha256':c['baseline']['artifact']['sha256'],'source_commit':c['baseline']['source_commit'],'filesystem_setup_sha256':sha(job/'FILESYSTEM_SETUP.json'),'host_setup_sha256':sha(job/'HOST_SETUP.json'),'no_build_or_source_copy':True,'case_roots_all_absent':all(not Path(x['wal_root']).exists() for x in cases)}
assert ready['case_roots_all_absent'];(job/'READY.json').write_text(json.dumps(ready,indent=2)+'\n');print(json.dumps(ready))
