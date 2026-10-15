"""484 fixed adjacent published480/483 Witness pairs; starts no workload."""
CELLS=[('singleton',16,256,8,1,1,None),('moderate',16,8192,128,256,1,None),('loaded',64,16000,2000,4096,1,None),('skew',64,8192,1024,4096,2,'31,1')]
def retained_bound(offers,groups,weights=None):
    if weights is None:return (offers+64+groups-1)//groups+64
    schedule=[g for g,w in enumerate(map(int,weights.split(',')))for _ in range(w)]
    counts=[0]*groups
    for operation in range(65,65+offers):counts[schedule[(operation-1)%len(schedule)]]+=1
    return max(counts[g]+64//groups+int(g<64%groups)+64 for g in range(groups))
def validate_bound(offers,groups,weights=None):
    assert retained_bound(offers,groups,weights)<=16384,'Existing default retained-entry guard rejects workload'
def cases(binaries,wal_parent,metadata_parent,python,resource_script):
    result=[]
    for trial in range(3):
        order=list(range(4))[trial:]+list(range(4))[:trial]
        for cell_index in order:
            cell,ready,offers,rate,window,groups,weights=CELLS[cell_index];validate_bound(offers,groups,weights)
            protocols=['tcp','quic']if(trial+cell_index)%2==0 else['quic','tcp']
            for protocol in protocols:
                first='baseline'if(trial+cell_index+['tcp','quic'].index(protocol))%2==0 else'candidate'
                pair=f't{trial+1}-{cell}-{protocol}'
                for arm in [first,'candidate'if first=='baseline'else'baseline']:
                    name=pair+'-'+arm;root=str(wal_parent/name);metadata=str(metadata_parent/name)
                    command=[str(binaries[arm]),root,protocol,str(offers),str(window),str(groups),'--offered',str(rate),'--maintenance','2','--ready-items',str(ready),'--offered-capacity','4096','--publication','witness']
                    if weights is not None:command+=['--group-weights',weights]
                    result.append({'name':name,'pair':pair,'trial':trial+1,'cell':cell,'protocol':protocol,'arm':arm,'publication':'witness','ready_items':ready,'offers':offers,'rate':rate,'window':window,'groups':groups,'weights':weights,'group_pending_ceiling':(window+groups-1)//groups,'retained_bound':retained_bound(offers,groups,weights),'horizon_seconds':offers/rate,'wal_root':root,'metadata':metadata,'command':command,'resource_command':[str(python),str(resource_script),metadata,*command]})
    assert len(result)==48 and len({c['name']for c in result})==48
    return result
