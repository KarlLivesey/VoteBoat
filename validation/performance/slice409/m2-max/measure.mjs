import fs from 'node:fs';
import crypto from 'node:crypto';
import {spawnSync} from 'node:child_process';
const root=process.argv[2];
const out=root+'/measure';
fs.mkdirSync(out,{recursive:true});
const hash=p=>crypto.createHash('sha256').update(fs.readFileSync(p)).digest('hex');
const sourcePaths=JSON.parse(fs.readFileSync(root+'/candidate/SOURCE.json')).sha256;
const referencePaths=JSON.parse(fs.readFileSync(root+'/reference/SOURCE.json')).sha256;
const artifacts=JSON.parse(fs.readFileSync(root+'/ARTIFACTS.json'));
const sourceMatches=(variant,manifest)=>Object.entries(manifest).every(([p,h])=>hash(root+'/'+variant+'/repo/'+p)===h);
if(!sourceMatches('candidate',sourcePaths)||!sourceMatches('reference',referencePaths)||!artifacts.every(a=>hash(a.copy)===a.sha256))throw Error('initial identity changed');
const cases=[];
function execute(variant,label,example,args) {
  const exe=`${root}/artifacts/${variant}-${example}`;
  const command=[exe,...args];
  fs.writeFileSync(`${out}/${label}.command`,command.join(' ')+'\n');
  fs.writeFileSync(`${out}/stage.txt`,label+'\n');
  const run=spawnSync(exe,args,{encoding:'utf8',maxBuffer:8*1024*1024});
  fs.writeFileSync(`${out}/${label}.log`,run.stderr??'');
  fs.writeFileSync(`${out}/${label}.stdout`,run.stdout??'');
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
const sourceUnchanged=sourceMatches('candidate',sourcePaths);
const referenceUnchanged=sourceMatches('reference',referencePaths);
const artifactsUnchanged=artifacts.every(a=>hash(a.copy)===a.sha256);
if(!sourceUnchanged||!referenceUnchanged||!artifactsUnchanged||cases.length!==96)throw Error('measurement identity/count changed');
fs.writeFileSync(root+'/RESULTS.json',JSON.stringify({scope:'Unmatched volatile CPU diagnostics on Apple M2 Max; not an OpenRaft floor or durable application comparison',sourceUnchanged,referenceUnchanged,artifactsUnchanged,cases},null,2)+'\n');
fs.writeFileSync(out+'/stage.txt','terminal\n');
console.log(JSON.stringify({cases:cases.length,sourceUnchanged,referenceUnchanged,artifactsUnchanged}));
