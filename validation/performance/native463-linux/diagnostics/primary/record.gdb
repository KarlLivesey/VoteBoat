set debuginfod enabled off
set pagination off
set confirm off
set disable-randomization off
set print thread-events off
starti
python
import gdb,json,os
from pathlib import Path
job=Path('/home/karllivesey/Documents/ChatGPT/VoteBoat/target/p9/native463-linux/diagnostics/primary')
schema=json.loads((job/'SCHEMA.json').read_text())
exe=os.path.realpath(gdb.current_progspace().filename)
pid=gdb.selected_inferior().pid
bases=[]
for line in Path('/proc/%s/maps'%pid).read_text().splitlines():
 fields=line.split()
 if len(fields)>=6 and os.path.realpath(fields[-1])==exe:
  bases.append(int(fields[0].split('-')[0],16)-int(fields[2],16))
# ELF first PT_LOAD has offset0/vaddr0; use that mapping rather than
# assuming later page-aligned load segments have identical offset bias.
import struct
elf=Path(exe).read_bytes()
phoff=struct.unpack_from('<Q',elf,32)[0]
entsize=struct.unpack_from('<H',elf,54)[0]
num=struct.unpack_from('<H',elf,56)[0]
loads=[struct.unpack_from('<IIQQQQQQ',elf,phoff+n*entsize) for n in range(num)]
assert any(p[0]==1 and p[2]==0 and p[3]==0 for p in loads)
zero=[int(line.split()[0].split('-')[0],16) for line in Path('/proc/%s/maps'%pid).read_text().splitlines() if len(line.split())>=6 and os.path.realpath(line.split()[-1])==exe and int(line.split()[2],16)==0]
assert len(zero)==1,zero
base=zero[0]; counts={name:0 for name in schema['rva']}; exits=[]
observations={'tip_commit':[], 'rpc_compare':[], 'rpc_equal':0, 'rpc_unequal':0}
class Counter(gdb.Breakpoint):
 def __init__(self,name,rva):
  super().__init__('*0x%x'%(base+rva),internal=True)
  self.name=name; self.silent=True
 def stop(self):
  counts[self.name]+=1
  if self.name=='Core_tip_commit_compare@4580f4' and len(observations['tip_commit'])<32:
   observations['tip_commit'].append({'rsi_tip':int(gdb.parse_and_eval('$rsi')), 'rax_commit':int(gdb.parse_and_eval('$rax'))})
  if self.name=='Core_rpc_equality_call@4584dc' and len(observations['rpc_compare'])<256:
   left=int(gdb.parse_and_eval('$rdi'));right=int(gdb.parse_and_eval('$rsi'))
   a=bytes(gdb.selected_inferior().read_memory(left,16));b=bytes(gdb.selected_inferior().read_memory(right,16))
   sent=int(gdb.parse_and_eval('$r15'))
   read=lambda p,n:bytes(gdb.selected_inferior().read_memory(p,n))
   value=lambda p:int.from_bytes(read(p,8),'little')
   context_equal=(read(sent,16)==read(left+0xa0,16) and value(sent+0x10)==value(left+0xb0) and value(sent+0x20)==value(left+0xc0) and value(sent+0x30)==value(left+0xd0))
   observations['rpc_compare'].append({'actual_tag_hex':a[:2].hex(),'expected_tag_hex':b[:2].hex(),'actual_u64_at8':int.from_bytes(a[8:16],'little'),'expected_tip_u64_at8':int.from_bytes(b[8:16],'little'),'request_start':value(sent+0xc8),'request_end':value(sent+0xd0),'exact_context_fields_equal':context_equal,'configuration_equal':value(sent+0xc0)==value(left+0xe0)})
  if self.name=='Core_rpc_equality_result@4584e1':
   observations['rpc_equal' if int(gdb.parse_and_eval('$al')) else 'rpc_unequal']+=1
  return False
bps=[Counter(name,rva) for name,rva in schema['rva'].items()]
def ended(event):
 exits.append(getattr(event,'exit_code',None))
gdb.events.exited.connect(ended)
end
continue
python
(job/'COUNTS.json').write_text(json.dumps({'counts':counts,'observations':observations,'inferior_exit_codes':exits,'load_bias':hex(base),'scope':schema['scope']},indent=2)+'\n')
assert exits==[0],exits
end
