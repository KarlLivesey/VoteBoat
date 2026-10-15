set pagination off
set confirm off
set disable-randomization off
set print thread-events off
starti
python
import gdb,json,os
from pathlib import Path
job=Path('/home/karllivesey/Documents/ChatGPT/VoteBoat/target/p9/native463-linux/diagnostics/stages')
schema=json.loads((job/'SCHEMA.json').read_text())
exe=os.path.realpath(gdb.current_progspace().filename)
pid=gdb.selected_inferior().pid
bases=[]
for line in Path('/proc/%s/maps'%pid).read_text().splitlines():
 fields=line.split()
 if len(fields)>=6 and os.path.realpath(fields[-1])==exe:
  bases.append(int(fields[0].split('-')[0],16)-int(fields[2],16))
assert bases and len(set(bases))==1,bases
base=bases[0]; counts={name:0 for name in schema['rva']}; exits=[]
class Counter(gdb.Breakpoint):
 def __init__(self,name,rva):
  super().__init__('*0x%x'%(base+rva),internal=True)
  self.name=name; self.silent=True
 def stop(self):
  counts[self.name]+=1
  return False
bps=[Counter(name,rva) for name,rva in schema['rva'].items()]
def ended(event):
 exits.append(getattr(event,'exit_code',None))
gdb.events.exited.connect(ended)
end
continue
python
(job/'COUNTS.json').write_text(json.dumps({'counts':counts,'inferior_exit_codes':exits,'load_bias':hex(base),'scope':schema['scope']},indent=2)+'\n')
assert exits==[0],exits
end
