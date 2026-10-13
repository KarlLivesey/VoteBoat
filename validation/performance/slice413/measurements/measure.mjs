import fs from 'node:fs';
import path from 'node:path';
import crypto from 'node:crypto';
import {spawnSync} from 'node:child_process';
const [reference,candidate,out]=process.argv.slice(2);
if(process.argv.length!==5||!reference||!candidate||!out)throw Error('usage: measure.mjs REFERENCE_ROOT CANDIDATE_ROOT FRESH_OUTPUT');
fs.mkdirSync(out);
const hash=p=>crypto.createHash('sha256').update(fs.readFileSync(p)).digest('hex');
const roots={reference,candidate};
const sources=Object.fromEntries(Object.entries(roots).map(([v,r])=>[v,JSON.parse(fs.readFileSync(`${r}/SOURCE.json`))]));
const artifacts=Object.fromEntries(Object.entries(roots).map(([v,r])=>[v,JSON.parse(fs.readFileSync(`${r}/ARTIFACTS.json`))]));
function identities(){
 for(const[v,r]of Object.entries(roots)){
  for(const[p,h]of Object.entries(sources[v].sha256))if(hash(`${r}/repo/${p}`)!==h)throw Error(`${v} frozen source changed: ${p}`);
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

function stats(values){
 const sorted=[...values].sort((a,b)=>a-b);
 return {mean_ns:values.reduce((a,b)=>a+b,0)/values.length,median_ns:sorted[Math.ceil(values.length/2)-1],p99_ns:sorted[Math.ceil(values.length*.99)-1],min_ns:sorted[0],max_ns:sorted.at(-1)};
}
function staging(variant,historyKind,history,mode,trial){
 const label=`staging-${historyKind}-h${history}-${mode}-t${trial}-${variant}`;
 const csv=`${out}/${label}.csv`;
 const raw=run(variant,label,'log_staging_benchmark',[csv,'0',`${history}`,'256',mode,historyKind]);
 const summary=Object.fromEntries([...raw.matchAll(/(\w+)=([^\s;]+)/g)].map(m=>[m[1],m[2]]));
 if(summary.scope!=='VOLATILE'||summary.native_codec!=='true'||summary.state_and_restart_verified!=='true'||summary.unrelated_groups!=='0'||summary.history!==`${history}`||summary.measured_iterations!=='256'||summary.warmup_iterations!=='16')throw Error(`${label} oracle/summary scope mismatch`);
 const [header,...lines]=fs.readFileSync(csv,'utf8').trim().split(/\r?\n/).map(s=>s.split(','));
 const expected=['scope','workload','unrelated_groups','total_groups','history_per_group','warmup_iterations','iteration','append_ns','barrier_ns','total_ns','encoded_wal_bytes','manifest_bytes','append_calls','simulated_sync_calls','simulated_publish_calls','state_verified','recovery_verified','history_kind','snapshot_base'];
 if(JSON.stringify(header)!==JSON.stringify(expected)||lines.length!==256||lines.some(r=>r.length!==header.length))throw Error(`${label} CSV shape mismatch`);
 const rows=lines.map(row=>Object.fromEntries(header.map((k,i)=>[k,row[i]])));
 const workload=mode==='append-commit'?'append_and_commit':'pure_append';
 for(const[r,i]of rows.map((r,i)=>[r,i])){
  if(r.scope!=='volatile_native_log_staging'||r.workload!==workload||r.unrelated_groups!=='0'||r.total_groups!=='1'||r.history_per_group!==`${history}`||r.warmup_iterations!=='16'||r.iteration!==`${i}`||r.history_kind!==historyKind||r.snapshot_base!==`${Number(historyKind==='checkpoint')}`||r.state_verified!=='true'||r.recovery_verified!=='true'||r.append_calls!=='1'||r.simulated_sync_calls!=='1'||r.simulated_publish_calls!=='1'||r.manifest_bytes!=='52')throw Error(`${label} sample${i} semantic mismatch`);
  for(const key of ['append_ns','barrier_ns','total_ns','encoded_wal_bytes'])if(!Number.isSafeInteger(Number(r[key]))||Number(r[key])<0)throw Error(`${label} sample${i} numeric mismatch`);
  if(Number(r.encoded_wal_bytes)===0||Number(r.total_ns)<Number(r.append_ns)+Number(r.barrier_ns))throw Error(`${label} sample${i} timing/codec mismatch`);
 }
 const shapes=rows.map(r=>Object.fromEntries(Object.entries(r).filter(([k])=>!['append_ns','barrier_ns','total_ns'].includes(k))));
 const shapeHash=crypto.createHash('sha256').update(JSON.stringify(shapes)).digest('hex');
 return {kind:'volatile_native_staging',variant,historyKind,history,mode,trial,completed:rows.length,warmup:16,shapeHash,append:stats(rows.map(r=>Number(r.append_ns))),barrier:stats(rows.map(r=>Number(r.barrier_ns))),total:stats(rows.map(r=>Number(r.total_ns)))};
}
const cases=[];
try{
 identities();
 const overlay=sources.reference.diagnosticOverlay;
 if(JSON.stringify(overlay)!==JSON.stringify(sources.candidate.diagnosticOverlay))throw Error('diagnostic overlays differ');
 for(const[v,r]of Object.entries(roots))for(const[p,h]of Object.entries(overlay))if(hash(`${r}/repo/${p}`)!==h)throw Error(`${v} diagnostic overlay changed`);
 fs.writeFileSync(`${out}/IDENTITY_BEFORE.json`,JSON.stringify({sources,artifacts},null,2)+'\n');
 for(const historyKind of ['plain','stable','joint','checkpoint'])for(const history of [256,4096])for(const mode of ['append','append-commit'])for(let trial=1;trial<=3;trial++){
  const pair=[];
  for(const variant of trial%2?['reference','candidate']:['candidate','reference']){const result=staging(variant,historyKind,history,mode,trial);cases.push(result);pair.push(result);}
  if(pair[0].shapeHash!==pair[1].shapeHash)throw Error('paired transition/codec/sample shapes differ');
 }
 identities();
 if(cases.length!==96)throw Error('incomplete campaign');
 fs.writeFileSync(`${out}/RESULTS.json`,JSON.stringify({scope:'413 VOLATILE native-log configured/checkpoint staging; real codec/validation/exact simulated publication; no device durability, consensus, network/application TPS or matched OpenRaft floor claim',cases,sourceUnchanged:true,artifactsUnchanged:true,identicalDiagnosticOverlay:true},null,2)+'\n');
 fs.writeFileSync(`${out}/terminal.txt`,'passed=96 failed=0 sourceUnchanged=true artifactsUnchanged=true identicalDiagnosticOverlay=true\n');
 fs.writeFileSync(`${out}/stage.txt`,'terminal\n');
 console.log(JSON.stringify({passed:96,failed:0}));
}catch(error){
 fs.writeFileSync(`${out}/PARTIAL_RESULTS.json`,JSON.stringify({valid_campaign:false,cases,error:String(error)},null,2)+'\n');
 fs.writeFileSync(`${out}/terminal.txt`,`valid_campaign=false successful_cases=${cases.length} error=${error}\n`);
 fs.writeFileSync(`${out}/stage.txt`,'failed\n');
 throw error;
}
