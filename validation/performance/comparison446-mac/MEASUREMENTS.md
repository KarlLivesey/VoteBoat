# Mac446 paired measurements

OriginalwholecampaignSSH63802/PID32209 actually terminates0 (b0ae9f), bulk12then singleton12, all once. Bothcell identity/outputcheckers0; everycase counts/frontiers pass, allstderr empty.20Mrequestedbulk completes19,988,480 originals/4,997,120requests+1024warmups withfrontiers19,989,505×3; singleton completes1,048,576 originals/requests+1024warmups/frontiers1,049,601×3. Timer endsafterallclientjoins beforelatencyflatten/sort/followerverification/shutdown.

| Cell/variant | Median completed originals/s | Median request p99 ms |
|---|---:|---:|
| Bulk435OFF |5,405,801.462|3.575125|
| Bulk446OFF |6,513,577.006|3.281709|
| Bulk446ON |6,356,781.961|3.386500|
| BulkOpenRaft |2,213,078.713|9.449375|
| Singleton435OFF |66,005.686|0.029167|
| Singleton446OFF |65,650.619|0.031458|
| Singleton446ON |60,769.645|0.030709|
| SingletonOpenRaft |11,555.304|0.120417|

Bulk all3OFF throughput ratios1.200040/1.194593/1.227070 andp99 ratios0.926153/0.920026/0.868565 improve. Singleton all3rates regress (0.975389/0.998954/0.981748) andall3p99 regress (1.089455/1.011417/1.074895). Fullrawoutputs andeverytail retained; no claimallcasesfaster, no retune/rerun. ON bulkp99 trials3.386500/3.330458/3.388500ms; ON singleton0.032500/0.030709/0.030292ms. Rootandindependentreviewretain446forrepeatableboth-hostbulkutility, withallMacsingletonrate/p99regressions,Linuxmixedtails andextra sparsemetadata-memorycost explicitlyretained asdebt. Diagnostic singletonfloorheadroom doesnotclearthoseregressions. This is nothealthy-margin/fullcurve acceptance.

Activecandidatebulk checkpoints3901–3902 perreplica, snapshotinstalls0–3710; observedlogicalpeak retainedentries21964,peerqueue17,clientqueue4009,proposalwidth6352. Singleton checkpoints209,peakretention6000,peerqueue3/clientqueue1/proposalwidth1. These bounds are logicalobservations, notphysicalRSS/allocation/debt proof. No separate refusalcounter orRSSmeasurement; no workloadfailure occurred. Observer times are nestedinclusivewall andarenotsummedCPU orcriticalpath. RealTCP/TLS/QUIC/nativepublication evidence remainsseparate.

Initial44setuppayloadbytes remainunchanged; original setupmanifest preserved as SETUP_ORIGINAL.sha256 (f7a50f983d9756be5518f6252989fb98035915c56646ce8bd9290b864f2d26fe). Fresh69qualification/fourfreshassertedartifacts/dSYM/currentcompiler/source1439/3CLIproofs remainin ../mac446. Initialmodeguardfailure63642 andfocusedallowlistfix43205 arepreserved; no sourcepermission/binary/build/CLI/test repeat. Captured source6c7f09b2c7d14606bc1d4308b83b5a5974f3ab19 versusimmutable435, exactprofile/settings/defaultmaintenance/envelopes/outcomesunchanged. No binaries/buildtrees/generatedstates inarchive. FullP9goal remainsactive; no Pi commands/newstage/source changes here.
