"""478 paired existing ready quanta; construction starts no workload."""
CELLS = [
    ('singleton',256,8,1,1,None),
    ('moderate',8192,128,256,1,None),
    ('bulk',8192,1024,4096,1,None),
    ('skew',8192,1024,4096,2,'31,1'),
]

def cases(binary, wal_parent, metadata_parent, python, resource_script):
    result=[]
    for trial in range(3):
        order=list(range(4))[trial:]+list(range(4))[:trial]
        for cell_index in order:
            cell,offers,rate,window,groups,weights=CELLS[cell_index]
            protocols=['tcp','quic']if(trial+cell_index)%2==0 else['quic','tcp']
            for protocol in protocols:
                first=16 if(trial+cell_index+['tcp','quic'].index(protocol))%2==0 else 64
                pair=f't{trial+1}-{cell}-{protocol}'
                for ready in [first,64 if first==16 else 16]:
                    name=pair+f'-q{ready}';root=str(wal_parent/name);metadata=str(metadata_parent/name)
                    command=[str(binary),root,protocol,str(offers),str(window),str(groups),'--offered',str(rate),
                             '--maintenance','2','--ready-items',str(ready),'--offered-capacity','4096','--publication','witness']
                    if weights is not None:command+=['--group-weights',weights]
                    result.append({'name':name,'pair':pair,'trial':trial+1,'cell':cell,'protocol':protocol,
                                   'publication':'witness','ready_items':ready,'offers':offers,'rate':rate,'window':window,
                                   'groups':groups,'weights':weights,'group_pending_ceiling':(window+groups-1)//groups,
                                   'horizon_seconds':offers/rate,'wal_root':root,'metadata':metadata,'command':command,
                                   'resource_command':[str(python),str(resource_script),metadata,*command]})
    assert len(result)==48 and len({c['name']for c in result})==48
    return result
