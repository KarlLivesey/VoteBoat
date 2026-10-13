import fs from 'node:fs';
import path from 'node:path';
import crypto from 'node:crypto';
const [root, variant] = process.argv.slice(2);
const hash = p => crypto.createHash('sha256').update(fs.readFileSync(p)).digest('hex');
const manifest = `${root}/ARTIFACTS.json`;
const artifacts = fs.existsSync(manifest) ? JSON.parse(fs.readFileSync(manifest)) : [];
const selected = new Set();
fs.mkdirSync(`${root}/artifacts`, {recursive:true});
for (const line of fs.readFileSync(`${root}/results/build_${variant}.jsonl`, 'utf8').trim().split('\n')) {
  const row = JSON.parse(line);
  if (row.reason !== 'compiler-artifact' || !['framework_benchmark','log_staging_benchmark','voteboat'].includes(row.target.name)) continue;
  if (row.profile.opt_level !== '3' || !row.profile.debug_assertions || !row.profile.overflow_checks) throw Error('wrong asserted release profile');
  if (row.fresh) throw Error('variant must actually compile its library and examples');
  if (!row.target.src_path.startsWith(fs.realpathSync(`${root}/${variant}/repo`)+'/')) throw Error('compiler source belongs to another snapshot');
  selected.add(row.target.name);
  for (const original of row.filenames.filter(p => p.endsWith('.rlib') || p === row.executable)) {
    const copy = `${root}/artifacts/${variant}-${path.basename(original)}`;
    if (artifacts.some(a => a.copy === copy)) throw Error('artifact copy already exists');
    const sha256 = hash(original);
    fs.copyFileSync(original, copy);
    if (original === row.executable) fs.chmodSync(copy, 0o755);
    if (hash(copy) !== sha256 || hash(original) !== sha256) throw Error('artifact identity changed during copy');
    if (artifacts.some(a => a.variant !== variant && a.target === row.target.name && a.sha256 === sha256)) throw Error('changed variants selected identical artifacts');
    artifacts.push({variant,target:row.target.name,profile:row.profile,features:row.features,compiler_fresh:row.fresh,compiler_source:row.target.src_path,original,copy,sha256});
  }
}
if (selected.size !== 3) throw Error('missing original executable/library profile');
fs.writeFileSync(manifest, JSON.stringify(artifacts,null,2)+'\n');
console.log(JSON.stringify({variant,targets:[...selected],artifacts:artifacts.filter(a=>a.variant===variant).length}));
