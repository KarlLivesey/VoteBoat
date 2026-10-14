"""Sampled current435 client contexts; exact per-RVA inlines/source, not call counts."""
from pathlib import Path
from collections import Counter
import gzip,json
ROOT=Path(__file__).resolve().parent
CLIENT='<voteboat_matched_framework::voteboat_engine::Client>::write::{closure#0}'
WRAPPER='<voteboat_matched_framework::workload::Client>::write::{closure#0}'
for cell in ['bulk','singleton']:
 d=ROOT/(cell+'-symbolicated');summary=json.loads((d/'SUMMARY.json').read_text());raw=json.load(gzip.open(summary['raw_profile'],'rt'));symbols={(r['library'],r['address']):r for r in json.loads((d/'ADDRESS_SYMBOLS.json').read_text())};counts=Counter();lines=Counter();leaflines=Counter();uncertain=Counter()
 for t in raw['threads']:
  if t['name'] not in {'voteboat-matche','matched-client','matched-server'}:continue
  def resolve(stack):
   names=[];source=[];records=[]
   while stack is not None:
    frame=t['stackTable']['frame'][stack];func=t['frameTable']['func'][frame];resource=t['funcTable']['resource'][func];lib=t['resourceTable']['lib'][resource] if resource is not None and resource>=0 else None;r=symbols.get((lib,t['frameTable']['address'][frame]),{});expanded=[r,*r.get('inlines',[])];records.append(expanded)
    names.extend(x.get('function','[unresolved]') for x in expanded)
    source.extend(x for x in expanded if x.get('function')==CLIENT)
    stack=t['stackTable']['prefix'][stack]
   return names,source,records
  for idx,stack in enumerate(t['samples']['stack']):
   weight=(t['samples'].get('weight') or [1]*t['samples']['length'])[idx];weight=1 if weight is None else weight
   names,src,recs=resolve(stack);client=CLIENT in names or WRAPPER in names
   if not client:continue
   counts['client_write_context']+=weight
   alloc=any(n.startswith(('alloc::alloc::','<alloc::alloc::','__rustc::__rust_alloc','__rustc::__rust_dealloc')) or n in {'malloc','calloc','realloc','free'} for n in names)
   channel=any(n.startswith('tokio::sync::oneshot::channel') for n in names)
   receive=any('tokio::sync::oneshot::Receiver<' in n and '::poll' in n or 'tokio::sync::oneshot::Inner<' in n and '::poll_recv' in n for n in names)
   for label,yes in [('client_alloc_dealloc_context',alloc),('client_oneshot_construct_context',channel),('client_oneshot_construct_alloc_context',channel and alloc),('client_receiver_poll_context',receive),('client_receiver_poll_alloc_context',receive and alloc),('client_oneshot_sender_context',any('tokio::sync::oneshot::Sender<' in n and '::send' in n for n in names)),('client_mpsc_request_send_context',any('tokio::sync::mpsc::bounded::Sender<' in n and '::send' in n for n in names))]:
    counts[label]+=weight if yes else 0
   for typ,label in [('Vec<voteboat_matched_framework::voteboat_engine::Original>','client_flat_originals_vec_construction'),('Vec<(voteboat::identity::OperationId, tokio::sync::oneshot::Receiver<','client_waits_vec_construction')]:
    yes=any(typ in n and ('::with_capacity' in n or '::try_allocate_in' in n) for n in names);counts[label]+=weight if yes else 0
   for x in src:lines[(x.get('file','unknown'),x.get('line'))]+=weight
   if recs:
    inner_client=[x for x in recs[0] if x.get('function')==CLIENT]
    for x in inner_client:leaflines[(x.get('file','unknown'),x.get('line'))]+=weight
    if WRAPPER in names and not src:uncertain['wrapper_without_engine_inline']+=weight
 n=summary['denominator'];rows=lambda c:[{'file':k[0],'line':k[1],'sample_weight':v,'percent_of_selected':100*v/n} for k,v in c.most_common()]
 result={'selected_sample_weight':n,'contexts':{k:{'sample_weight':v,'percent_of_selected':100*v/n} for k,v in counts.items()},'per_RVA_engine_client_source_context':rows(lines),'engine_client_source_in_native_leaf_RVA':rows(leaflines),'unresolved_classification':dict(uncertain),'limits':['Inclusive context intersections overlap; do not add them or subtract to estimate CPU costs.','Current flat Original constructor and existing waits Vec are classified; absent constructor frames mean unknown cost, not zero allocation.','Source/inline attribution is actual current435 per-address response; optimized/missing frames prevent an exhaustive partition.','Allocation/deallocation and Vec constructor contexts are sampled work, not allocator call counts, bytes or causality.','Receiver polling includes ready, pending and error checks; no wake/re-poll counts or blocked duration inferred.','No channels/outcomes removed; changing await order still requires failure/cancel contract review.','Actual435 capture losses/import warnings retained separately; no missing samples reconstructed.']}
 (d/'CLIENT_CONTEXTS.json').write_text(json.dumps(result,indent=2)+'\n');print(cell,result['contexts'])
