import json,pathlib,collections,sys,hashlib
j=pathlib.Path('target/p9/slice466');errors=json.loads((j/'diagnostic3/errors.json').read_text());edits=collections.defaultdict(set)
def owned(n):return not (n.startswith('src/raft/') or n.startswith('src/log/transition/tests') or n.startswith('src/log/owned/tests') or n=='src/log/comparison/tests.rs' or n.startswith('src/log/entry_'))
for m in errors:
 if not m.get('code',{}).get('code')=='E0308':continue
 for c in m.get('children',[]):
  conversion='call `Into::into`' in c['message'] and ('EntryBatch' in c['message'] or 'EntryHistory' in c['message'])
  borrow=c['message']=='consider borrowing here'
  if not (conversion or borrow):continue
  for s in c.get('spans',[]):
   n=s['file_name'];replacement=s.get('suggested_replacement')
   if not owned(n) or replacement not in ('.into()','entries: ','&'):continue
   if replacement=='&' and not borrow:continue
   assert s['byte_start']==s['byte_end'];edits[n].add((s['byte_start'],replacement))
report=[]
for n,changes in sorted(edits.items()):
 p=pathlib.Path(n);before=p.read_bytes();frozen=j/'diagnostic3_before-source'/n;assert frozen.read_bytes()==before,('Source changed since diagnostic',n);data=before
 for offset,replacement in sorted(changes,reverse=True):data=data[:offset]+replacement.encode()+data[offset:]
 p.write_bytes(data);report.append({'path':n,'edits':len(changes),'before_sha256':hashlib.sha256(before).hexdigest(),'after_sha256':hashlib.sha256(data).hexdigest()})
(j/'ROOT_CONVERSION_EDITS.json').write_text(json.dumps(report,indent=2)+'\n');print(json.dumps({'files':len(report),'edits':sum(x['edits'] for x in report),'scope':'Typed constructor .into or Cow borrow fixes only; agent-owned regions excluded.'}))
