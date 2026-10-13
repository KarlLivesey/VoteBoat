import fs from 'node:fs';
import path from 'node:path';
import crypto from 'node:crypto';
const root = 'target/p9/mac-perf409';
const hash = p => crypto.createHash('sha256').update(fs.readFileSync(p)).digest('hex');
const result = JSON.parse(fs.readFileSync(`${root}/RESULTS.json`));
const artifacts = JSON.parse(fs.readFileSync(`${root}/ARTIFACTS.json`));
const exits = fs.readFileSync(`${root}/measure/exits.txt`, 'utf8').trim().split('\n');
if (result.cases.length !== 96 || exits.length !== 96 || exits.some(line => !line.endsWith('=0'))) throw Error('wrong case count/exits');
if (!result.sourceUnchanged || !result.referenceUnchanged || !result.artifactsUnchanged) throw Error('remote identity changed');
for (const artifact of artifacts) {
  if (hash(`${root}/artifacts/${path.basename(artifact.copy)}`) !== artifact.sha256) throw Error('local artifact hash mismatch');
  if (artifact.profile.opt_level !== '3' || !artifact.profile.debug_assertions || !artifact.profile.overflow_checks) throw Error('artifact profile mismatch');
  if (artifact.compiler_fresh !== false) throw Error('variant was not actually compiled');
  if (artifacts.some(other=>other.variant!==artifact.variant&&other.target===artifact.target&&other.sha256===artifact.sha256)) throw Error('variants selected identical artifacts');
}
const guards = [];
for (const variant of ['reference', 'candidate']) {
  const source = JSON.parse(fs.readFileSync(`${root}/${variant}/SOURCE.json`));
  for (const phase of ['start', 'built', 'terminal']) {
    const name = `source_${variant}_${phase}`;
    const count = fs.readFileSync(`${root}/results/${name}.log`, 'utf8').split('\n').filter(line => line.endsWith(': OK')).length;
    if (count !== source.inputs) throw Error('source guard count mismatch');
    guards.push({variant, phase, matching_inputs:count});
  }
}
const median = values => [...values].sort((a,b)=>a-b)[Math.floor(values.length/2)];
const framework = [];
for (const clients of [1,64,256,1024]) for (const batch of [1,4]) {
  const matching = result.cases.filter(c => c.kind === 'framework' && c.clients === clients && c.batch === batch);
  const select = variant => matching.filter(c => c.variant === variant);
  const reference_ops_s = median(select('reference').map(c=>c.result.operations_per_second));
  const candidate_ops_s = median(select('candidate').map(c=>c.result.operations_per_second));
  framework.push({clients,batch,reference_ops_s,candidate_ops_s,speedup:candidate_ops_s/reference_ops_s,
    reference_request_p99_ms:median(select('reference').map(c=>c.result.request_p99_ns/1e6)),
    candidate_request_p99_ms:median(select('candidate').map(c=>c.result.request_p99_ns/1e6))});
}
const native = [];
for (const unrelated of [0,8]) for (const history of [256,4096]) for (const mode of ['append','replace']) {
  const matching = result.cases.filter(c => c.kind === 'native' && c.unrelated === unrelated && c.history === history && c.mode === mode);
  const select = variant => matching.filter(c => c.variant === variant).map(c=>c.mean_ns);
  const reference_mean_ns = median(select('reference'));
  const candidate_mean_ns = median(select('candidate'));
  native.push({unrelated,initial_history:history,mode,reference_mean_ns,candidate_mean_ns,speedup:reference_mean_ns/candidate_mean_ns});
}
const summary = {scope:result.scope,hardware:{chip:'Apple M2 Max',physical_cores:12,logical_cores:12,memory_bytes:103079215104,os:'macOS 27.0 build 26A428 / Darwin arm64'},
  cases:96,successful_case_exits:96,source_guards:guards,immutable_artifacts_verified:artifacts.length,
  separate_target_directories:true,variant_artifacts_distinct:true,invalid_attempt:'attempt-shared-cache; excluded because both variants selected identical reference artifacts',
  aggregation:'Median of three trials for each variant; native values are medians of per-trial 512-row means',framework,native,
  limitations:['Volatile exact-ticket CPU diagnostics; no physical persistence or real network', 'Unmatched OpenRaft comparison; these do not establish any framework floor', 'Single cooperative framework driver; not multicore or independent-group scaling evidence', 'Framework p99 unit is all original results in one client batch', 'Native append histories grow by 528 entries including warmup; replacement histories remain fixed']};
fs.writeFileSync(`${root}/SUMMARY.json`,JSON.stringify(summary,null,2)+'\n');
console.log(JSON.stringify(summary,null,2));
