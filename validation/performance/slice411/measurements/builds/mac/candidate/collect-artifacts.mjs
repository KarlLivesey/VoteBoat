import fs from 'node:fs';
import path from 'node:path';
import crypto from 'node:crypto';
const root=process.argv[2];
const hash=p=>crypto.createHash('sha256').update(fs.readFileSync(p)).digest('hex');
const source=fs.realpathSync(`${root}/repo`)+'/';
const selected=new Set(), artifacts=[];
fs.mkdirSync(`${root}/artifacts`,{recursive:true});
for(const line of fs.readFileSync(`${root}/results/build.jsonl`,'utf8').trim().split('\n')) {
  const row=JSON.parse(line);
  if(row.reason!=='compiler-artifact'||!['voteboat','native_benchmark','framework_benchmark'].includes(row.target.name))continue;
  if(row.fresh||!row.target.src_path.startsWith(source)||row.profile.opt_level!=='3'||!row.profile.debug_assertions||!row.profile.overflow_checks)throw Error('baseline source/profile/nonfresh mismatch');
  selected.add(row.target.name);
  for(const original of row.filenames.filter(p=>p.endsWith('.rlib')||p===row.executable)) {
    const copy=`${root}/artifacts/${path.basename(original)}`;
    if(fs.existsSync(copy))throw Error('original artifact already captured');
    const sha256=hash(original);
    fs.copyFileSync(original,copy);
    if(original===row.executable)fs.chmodSync(copy,0o755);
    if(hash(copy)!==sha256||hash(original)!==sha256)throw Error('artifact changed duringcopy');
    artifacts.push({target:row.target.name,compiler_source:row.target.src_path,compiler_fresh:row.fresh,profile:row.profile,features:row.features,original,copy,sha256});
  }
}
if(selected.size!==3||artifacts.length!==3)throw Error('missing required original benchmark/library artifacts');
fs.writeFileSync(`${root}/ARTIFACTS.json`,JSON.stringify(artifacts,null,2)+'\n');
console.log(JSON.stringify({targets:[...selected],artifacts:artifacts.length,timed_cases:0}));
