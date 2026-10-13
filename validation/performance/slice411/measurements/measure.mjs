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
function native(variant,protocol,window,trial){
 const label=`native-${protocol}-w${window}-t${trial}-${variant}`;
 const directory=`${out}/${label}`;
 const raw=run(variant,label,'native_benchmark',[directory,protocol,'256',`${window}`,'--journal-timings']);
 const summary=Object.fromEntries([...raw.matchAll(/(\w+)=([^\s]+)/g)].map(m=>[m[1],m[2]]));
 if(summary.operations!=='256'||summary.warmup!=='64'||summary.recovered_value!=='320'||summary.retry_verified!=='true'||summary.workers_joined!=='true'||summary.assembly!=='startup')throw Error(`${label} native semantic scope mismatch`);
 const rows=fs.readFileSync(`${directory}/samples.csv`,'utf8').trim().split('\n').slice(1).map(s=>s.split(',').map(Number));
 if(rows.length!==256||new Set(rows.map(r=>r[0])).size!==256)throw Error(`${label} original completion count mismatch`);
 rows.sort((a,b)=>a[0]-b[0]);
 if(rows.some((r,i)=>r[0]!==i+65||r[5]!==r[0]||r[6]!==1||r[2]<r[1]||r[3]!==r[2]-r[1]||r[4]===0||(i&&r[4]<=rows[i-1][4])))throw Error(`${label} original applied history mismatch`);
 if(Number(summary.max_inflight)<1||Number(summary.max_inflight)>window)throw Error(`${label} unbounded window`);
 return {kind:'native',variant,protocol,window,trial,completed:rows.length,summary,case_directory:directory};
}
function framework(variant,clients,batch,trial){
 const label=`framework-c${clients}-b${batch}-t${trial}-${variant}`;
 const raw=run(variant,label,'framework_benchmark',['--clients',`${clients}`,'--operations','16384','--warmup','1024','--batch',`${batch}`,'--retained-entries','32768']);
 const result=JSON.parse(raw);
 fs.writeFileSync(`${out}/${label}.json`,raw);
 if(result.completed!==16384||result.completed_batches!==16384/batch||result.applied_frontiers.some(n=>n!==17409)||result.retained_entries.some(n=>n!==17409)||result.comparison_gate!=='unmatched'||result.peak_pending_operations>clients*batch)throw Error(`${label} framework scope/completion mismatch`);
 return {kind:'framework',variant,clients,batch,trial,result};
}
const cases=[];
try{
 identities();
 fs.writeFileSync(`${out}/IDENTITY_BEFORE.json`,JSON.stringify({sources,artifacts},null,2)+'\n');
 for(const protocol of ['tcp','quic'])for(const window of [1,32])for(let trial=1;trial<=3;trial++){
  for(const variant of trial%2?['reference','candidate']:['candidate','reference'])cases.push(native(variant,protocol,window,trial));
 }
 for(const clients of [1,64,256,512,1024,4096])for(const batch of [1,4])for(let trial=1;trial<=3;trial++){
  for(const variant of trial%2?['reference','candidate']:['candidate','reference'])cases.push(framework(variant,clients,batch,trial));
 }
 identities();
 if(cases.length!==96)throw Error('incomplete campaign');
 fs.writeFileSync(`${out}/RESULTS.json`,JSON.stringify({scope:'411 real synchronized FileLogIo/peer/application diagnostics; 412 unmatched volatile public-core/provider diagnostics, no411owner-overlap path in framework',cases,sourceUnchanged:true,artifactsUnchanged:true},null,2)+'\n');
 fs.writeFileSync(`${out}/terminal.txt`,'passed=96 failed=0 sourceUnchanged=true artifactsUnchanged=true\n');
 fs.writeFileSync(`${out}/stage.txt`,'terminal\n');
 console.log(JSON.stringify({passed:96,failed:0}));
}catch(error){
 fs.writeFileSync(`${out}/PARTIAL_RESULTS.json`,JSON.stringify({valid_campaign:false,cases,error:String(error)},null,2)+'\n');
 fs.writeFileSync(`${out}/terminal.txt`,`valid_campaign=false successful_cases=${cases.length} error=${error}\n`);
 fs.writeFileSync(`${out}/stage.txt`,'failed\n');
 throw error;
}
