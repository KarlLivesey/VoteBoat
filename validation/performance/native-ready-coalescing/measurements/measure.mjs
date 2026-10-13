import fs from 'node:fs';
import path from 'node:path';
import crypto from 'node:crypto';
import {spawnSync} from 'node:child_process';
const [reference,candidate,out]=process.argv.slice(2);
if(!reference||!candidate||!out)throw Error('usage: measure.mjs REFERENCE_ROOT CANDIDATE_ROOT FRESH_OUTPUT');
fs.mkdirSync(out);
const hash=p=>crypto.createHash('sha256').update(fs.readFileSync(p)).digest('hex');
const roots={reference,candidate};
const sources=Object.fromEntries(Object.entries(roots).map(([v,r])=>[v,JSON.parse(fs.readFileSync(`${r}/SOURCE.json`))]));
const artifacts=Object.fromEntries(Object.entries(roots).map(([v,r])=>[v,JSON.parse(fs.readFileSync(`${r}/ARTIFACTS.json`))]));
function identities(){
 for(const[v,r]of Object.entries(roots)){
  for(const[p,h]of Object.entries(sources[v].sha256))if(hash(`${sources[v].sourceRoot??`${r}/repo`}/${p}`)!==h)throw Error(`${v} frozen source changed: ${p}`);
  for(const a of artifacts[v])if(hash(a.copy)!==a.sha256||a.compiler_fresh!==false||a.profile.opt_level!=='3'||!a.profile.debug_assertions||!a.profile.overflow_checks)throw Error(`${v} artifact/source/profile mismatch`);
 }
 for(const a of artifacts.reference)if(artifacts.candidate.some(b=>b.target===a.target&&b.sha256===a.sha256))throw Error('different variants selected identical artifacts');
}
function run(variant,label,target,args){
 const artifact=artifacts[variant].find(a=>a.target===target);
 if(!artifact)throw Error(`missing ${target}`);
 const argv=[artifact.copy,...args];
 fs.writeFileSync(`${out}/${label}.command`,JSON.stringify(argv)+'\n');
 fs.writeFileSync(`${out}/stage.txt`,label+'\n');
 const result=spawnSync(artifact.copy,args,{encoding:'utf8',maxBuffer:16*1024*1024});
 fs.writeFileSync(`${out}/${label}.stdout`,result.stdout??'');
 fs.writeFileSync(`${out}/${label}.stderr`,result.stderr??'');
 fs.appendFileSync(`${out}/exits.txt`,`${label}=${result.status}\n`);
 if(result.error||result.status!==0)throw Error(`${label}: ${result.error??result.stderr}`);
 return result.stdout;
}

function csv(file){
 const [header,...rows]=fs.readFileSync(file,'utf8').trim().split(/\r?\n/).map(s=>s.split(','));
 return rows.map(r=>Object.fromEntries(header.map((k,i)=>[k,r[i]])));
}
function native(variant,protocol,window,trial){
 const label=`native-${protocol}-w${window}-t${trial}-${variant}`;
 const directory=`${out}/${label}`;
 const raw=run(variant,label,'native_benchmark',[directory,protocol,'256',`${window}`,'--journal-timings']);
 const summary=Object.fromEntries([...raw.matchAll(/(\w+)=([^\s]+)/g)].map(m=>[m[1],m[2]]));
 if(summary.operations!=='256'||summary.warmup!=='64'||summary.recovered_value!=='320'||summary.retry_verified!=='true'||summary.workers_joined!=='true'||summary.assembly!=='startup'||summary.replicas!=='3'||summary.groups!=='1'||summary.payload_bytes!=='8'||summary.heartbeat_ms!=='50'||summary.election_min_ms!=='1000'||summary.election_spread_ms!=='1000'||summary.journal_timings!=='true')throw Error(`${label} native semantic/setting mismatch`);
 const rows=csv(`${directory}/samples.csv`).sort((a,b)=>+a.operation-+b.operation);
 if(rows.length!==256||new Set(rows.map(r=>r.operation)).size!==256)throw Error(`${label} original completion count mismatch`);
 if(rows.some((r,i)=>+r.operation!==i+65||+r.value!==+r.operation||r.group!=='1'||+r.completed_ns<+r.submitted_ns||+r.latency_ns!==+r.completed_ns-+r.submitted_ns||+r.applied_index===0||(i&&+r.applied_index<=+rows[i-1].applied_index)))throw Error(`${label} original applied history mismatch`);
 if(+summary.max_inflight<1||+summary.max_inflight>window)throw Error(`${label} unbounded window`);
 const widths=csv(`${directory}/proposal-batches.csv`);
 if(widths.some(r=>r.queue_selection_reason_observed!=='false'||r.group!=='1'||r.incarnation!=='1'))throw Error('observer scope mismatch');
 const histograms={};
 for(const phase of ['warmup','measurement']){
  const h=widths.filter(r=>r.phase===phase&&+r.replica===1&&+r.width>0);
  if(!h.length||h.some(r=>!Number.isInteger(+r.width)||+r.width<1||+r.width>Math.min(64,window)||!Number.isInteger(+r.visits)||+r.visits<1||+r.original_proposals!==+r.width*+r.visits))throw Error(`${label} invalidwidths`);
  const weighted=h.reduce((s,r)=>s+(+r.width*+r.visits),0);
  if(weighted!==(phase==='warmup'?64:256))throw Error(`${label} widthweighted original count mismatch`);
  histograms[phase]={weightedOriginals:weighted,visits:h.reduce((s,r)=>s+(+r.visits),0),widths:Object.fromEntries(h.map(r=>[r.width,+r.visits]))};
 }
 const journal=csv(`${directory}/journal.csv`),physical=[];
 for(const replica of [1,2,3]){
  const before=journal.filter(r=>r.stage==='before_measurement'&&+r.replica===replica),after=journal.filter(r=>r.stage==='after_measurement'&&+r.replica===replica);
  if(before.length!==1||after.length!==1)throw Error('physical I/O phase row mismatch');
  const delta={};
  for(const key of ['io_append_calls','io_append_ns','sync_calls','sync_ns','publish_calls','publish_ns']){
   const a=+after[0][key],b=+before[0][key];if(!Number.isSafeInteger(a)||!Number.isSafeInteger(b)||a<b)throw Error('physical I/O counter mismatch');delta[key]=a-b;
  }
  if(delta.io_append_calls<1||delta.sync_calls<1||delta.publish_calls<1)throw Error('missing physical durability activity');
  physical.push({replica,...delta});
 }
 return {kind:'durable_native',variant,protocol,window,trial,completed:rows.length,summary,histograms,physical};
}
const cases=[];
try{
 identities();
 for(const[p,h]of Object.entries(sources.reference.sha256).filter(([p])=>p==='examples/native_benchmark.rs'||p.startsWith('examples/benchmark/')))if(sources.candidate.sha256[p]!==h)throw Error(`benchmark source differs ${p}`);
 fs.writeFileSync(`${out}/IDENTITY_BEFORE.json`,JSON.stringify({sources,artifacts},null,2)+'\n');
 for(const protocol of ['tcp','quic'])for(const window of [32,1])for(let trial=1;trial<=3;trial++){
  for(const variant of trial%2?['reference','candidate']:['candidate','reference'])cases.push(native(variant,protocol,window,trial));
 }
 identities();
 if(cases.length!==24)throw Error('incomplete campaign');
 fs.writeFileSync(`${out}/RESULTS.json`,JSON.stringify({scope:'Native414 durable original-application before/after306b26b vs prior frozen observer; same instrumented benchmark; three replicas share one host/device; not an OpenRaft floor claim. Includes default-equivalent BatchLimits changes, no singleton-cause attribution from observer.',cases,sourceUnchanged:true,artifactsUnchanged:true,benchmarkSourceIdentical:true},null,2)+'\n');
 fs.writeFileSync(`${out}/terminal.txt`,'passed=24 failed=0 sourceUnchanged=true artifactsUnchanged=true benchmarkSourceIdentical=true\n');
 fs.writeFileSync(`${out}/stage.txt`,'terminal\n');
 console.log(JSON.stringify({passed:24,failed:0}));
}catch(error){
 fs.writeFileSync(`${out}/PARTIAL_RESULTS.json`,JSON.stringify({valid_campaign:false,cases,error:String(error)},null,2)+'\n');
 fs.writeFileSync(`${out}/terminal.txt`,`valid_campaign=false successful_cases=${cases.length} error=${error}\n`);
 fs.writeFileSync(`${out}/stage.txt`,'failed\n');
 throw error;
}
