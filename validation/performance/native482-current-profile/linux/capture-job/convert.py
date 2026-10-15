"""Original headless perf readouts after both recordings terminated."""
from pathlib import Path
import json,os,subprocess
job=Path(__file__).resolve().parent;config=json.loads((job/'CONFIG.json').read_text());out=job/'readouts';assert not out.exists();out.mkdir()
perf=config['perf_command'][0];data=job/'capture/perf.data';env=os.environ.copy();env['LD_LIBRARY_PATH']=config['perf_library_path']
commands={
 'self':[perf,'report','-i',str(data),'--stdio','--no-children','--sort','comm,symbol','-n','--percent-limit','0'],
 'inclusive':[perf,'report','-i',str(data),'--stdio','--children','--sort','comm,symbol','-n','--percent-limit','0'],
 'stacks':[perf,'script','-i',str(data),'-F','comm,pid,tid,time,event,ip,sym,dso,period'],
 'leaves':[perf,'script','-G','-i',str(data),'-F','comm,pid,tid,time,event,ip,sym,dso,period'],
 'header':[perf,'report','-i',str(data),'--stdio','--header-only'],
}
for name,argv in commands.items():
 (out/(name+'.command.json')).write_text(json.dumps({'argv':argv,'LD_LIBRARY_PATH':env['LD_LIBRARY_PATH']},indent=2)+'\n')
 with(out/(name+'.stdout')).open('wb')as stdout,(out/(name+'.stderr')).open('wb')as stderr:run=subprocess.run(argv,stdout=stdout,stderr=stderr,env=env)
 (out/(name+'.exit')).write_text(str(run.returncode)+'\n');print(name,run.returncode,flush=True)
