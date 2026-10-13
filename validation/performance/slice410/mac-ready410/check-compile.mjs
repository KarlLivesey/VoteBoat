import fs from 'node:fs';
const root=process.argv[2];
const source=fs.realpathSync(`${root}/repo`)+'/';
const rows=fs.readFileSync(`${root}/results/clippy_all.jsonl`,'utf8').trim().split('\n').map(line=>JSON.parse(line));
const libraries=rows.filter(row=>row.reason==='compiler-artifact'&&row.target.name==='voteboat'&&row.target.kind.includes('lib'));
if(!libraries.length||libraries.some(row=>row.fresh||!row.target.src_path.startsWith(source)))throw Error('missing genuine library compilation from ready410 snapshot');
console.log(JSON.stringify(libraries.map(row=>({target:row.target.name,fresh:row.fresh,source:row.target.src_path,profile:row.profile,features:row.features}))));
