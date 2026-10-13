import fs from 'node:fs';
import path from 'node:path';
import crypto from 'node:crypto';
import {spawnSync} from 'node:child_process';
const root = 'target/p9/delta409';
const out = `${root}/measure`;
fs.mkdirSync(out, {recursive:true});
const hash = p => crypto.createHash('sha256').update(fs.readFileSync(p)).digest('hex');
function inputs(dir) {
  return fs.readdirSync(dir,{withFileTypes:true}).flatMap(e => {
    const p=path.join(dir,e.name);
    return e.isDirectory()?inputs(p):e.name.endsWith('.rs')?[p]:[];
  });
}
const sourcePaths=['Cargo.toml','Cargo.lock','clippy.toml',...inputs('src'),...inputs('examples')];
const sourceBefore=Object.fromEntries(sourcePaths.map(p=>[p,hash(p)]));
const referencePaths=['Cargo.toml','Cargo.lock','clippy.toml',...inputs(`${root}/reference/src`).map(p=>path.relative(`${root}/reference`,p)),...inputs(`${root}/reference/examples`).map(p=>path.relative(`${root}/reference`,p))];
const referenceBefore=Object.fromEntries(referencePaths.map(p=>[p,hash(`${root}/reference/${p}`)]));
fs.writeFileSync(`${root}/SOURCE.json`,JSON.stringify({base:'6c404559a796bf97956dbe50cc8e25e40e7c1cee',sha256:sourceBefore,reference:referenceBefore},null,2)+'\n');
const artifacts=[];
for(const [variant,report] of [['reference','reference-build.jsonl'],['reference','reference-staging-build.jsonl'],['candidate','candidate-build.jsonl']]) {
  for(const line of fs.readFileSync(`${root}/${report}`,'utf8').trim().split('\n')) {
    const row=JSON.parse(line);
    if(row.reason!=='compiler-artifact' || !['framework_benchmark','log_staging_benchmark','voteboat'].includes(row.target.name))continue;
    if(row.profile.opt_level!=='3'||!row.profile.debug_assertions||!row.profile.overflow_checks)throw Error('wrong release profile');
    for(const original of row.filenames.filter(p=>p.endsWith('.rlib')||p===row.executable)) {
      const copy=`${root}/artifacts/${variant}-${path.basename(original)}`;
      fs.copyFileSync(original,copy);
      if(!artifacts.some(a=>a.copy===copy))artifacts.push({variant,target:row.target.name,profile:row.profile,features:row.features,original,copy,sha256:hash(copy)});
    }
  }
}
fs.writeFileSync(`${root}/ARTIFACTS.json`,JSON.stringify(artifacts,null,2)+'\n');
const cases=[];
function execute(variant,label,example,args) {
  const exe=`${root}/artifacts/${variant}-${example}`;
  const command=[exe,...args];
  fs.writeFileSync(`${out}/${label}.command`,command.join(' ')+'\n');
  fs.writeFileSync(`${out}/stage.txt`,label+'\n');
  const run=spawnSync(exe,args,{encoding:'utf8',maxBuffer:8*1024*1024});
  fs.writeFileSync(`${out}/${label}.log`,run.stderr??'');
  fs.appendFileSync(`${out}/exits.txt`,`${label}=${run.status}\n`);
  if(run.error||run.status!==0)throw Error(`${label}: ${run.error??run.stderr}`);
  return run.stdout;
}
for(const clients of [1,64,256,1024])for(const batch of [1,4])for(let trial=1;trial<=3;trial++) {
  const variants=trial%2?['reference','candidate']:['candidate','reference'];
  for(const variant of variants) {
    const label=`framework-c${clients}-b${batch}-t${trial}-${variant}`;
    const raw=execute(variant,label,'framework_benchmark',['--clients',`${clients}`,'--operations','4096','--warmup','1024','--batch',`${batch}`]);
    const result=JSON.parse(raw);
    if(result.completed!==4096||result.applied_frontiers.some(n=>n!==5121)||result.comparison_gate!=='unmatched')throw Error('framework scope mismatch');
    fs.writeFileSync(`${out}/${label}.json`,raw);
    cases.push({kind:'framework',variant,clients,batch,trial,result});
  }
}
for(const unrelated of [0,8])for(const history of [256,4096])for(const mode of ['append','replace'])for(let trial=1;trial<=3;trial++) {
  const variants=trial%2?['reference','candidate']:['candidate','reference'];
  for(const variant of variants) {
    const label=`native-u${unrelated}-h${history}-${mode}-t${trial}-${variant}`;
    const csv=`${out}/${label}.csv`;
    const args=[csv,`${unrelated}`,`${history}`,'512',...(mode==='append'?['append']:[])];
    const raw=execute(variant,label,'log_staging_benchmark',args);
    fs.writeFileSync(`${out}/${label}.stdout`,raw);
    const rows=fs.readFileSync(csv,'utf8').trim().split('\n').slice(1).map(line=>line.split(','));
    if(rows.length!==512||rows.some(r=>r[1]!== (mode==='append'?'pure_append':'suffix_replacement') ||r.slice(-2).join(',')!=='true,true'))throw Error('native verification scope mismatch');
    cases.push({kind:'native',variant,unrelated,history,mode,trial,mean_ns:rows.reduce((s,r)=>s+Number(r[9]),0)/rows.length});
  }
}
const sourceUnchanged=sourcePaths.every(p=>hash(p)===sourceBefore[p]);
const referenceUnchanged=referencePaths.every(p=>hash(`${root}/reference/${p}`)===referenceBefore[p]);
const artifactsUnchanged=artifacts.every(a=>hash(a.copy)===a.sha256);
if(!sourceUnchanged||!referenceUnchanged||!artifactsUnchanged)throw Error('measurement identity changed');
fs.writeFileSync(`${root}/RESULTS.json`,JSON.stringify({scope:'Unmatched volatile CPU diagnostics; not an OpenRaft floor or durable application comparison',sourceUnchanged,referenceUnchanged,artifactsUnchanged,cases},null,2)+'\n');
fs.writeFileSync(`${out}/stage.txt`,'terminal\n');
console.log(JSON.stringify({cases:cases.length,sourceUnchanged,artifactsUnchanged}));
