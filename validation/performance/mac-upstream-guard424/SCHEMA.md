# Pinned upstream guard-extended driver

Purpose: permit the unchanged full20M workload to complete in slow singleton cells.
The original400s driver and its proven artifacts remain untouched. The new source
is explicitly guard-extended, not an untouched reference. Pin e88bb40707f2e511d8ecc44d31edcb1d814a11a8;
copy the existing exact1357-file guarded reference including its independent196-
package lock. Change only WAIT_TIMEOUT Duration::from_secs(400) to3600 in the
benchmark binary. The same constant also bounds optional delay validation; campaign
uses defaultzero delay, with no selected network pacing or workload changes.
Engine/library/store/network/configuration/stats/defaultfeatures all remain exact.

Data/sequence: preserve original full source map then derive candidate map differing
at exactly one path. Retain exact one-line patch, original/candidate hashes and
manifest/lock/compiler identity. Build fresh asserted opt3/thinLTO/codegen1 with
same pinned compiler and default benchmark features, fresh target directory. Real
OpenRaft engine-library and driver artifacts must be fresh:false, actual source
paths/profile guarded, immutable copies separately named. Unchanged engine rlib
identity is expected to differ due to crate paths, so prove source/profile rather
than pretending identical bytes. No new dependency resolution or fetching.

Failure/cleanup: preserve command/stderr/exit and partial artifacts on failure;
no rate from incomplete work and no loop of unchanged retries. No timing while
build/copy active. The common3600s finite guard prevents stalls; runner allowance
must include setup/warmup/cleanup. Full workloads still round like pinned source,
all errors preserved. Retain original400s identity as the published-source anchor;
short untouched diagnostic and long guard-extended campaigns are labeled separately.

Acceptance: verify1357 original hashes before copying; candidate differs only in
one exact constant with all1356 other hashes preserved. Frozen map before/after,
fresh asserted engine/driver compiler artifacts, real small three-voter batch1/4
functional runs under defaultpolicies with expected total/latency/completion output;
no publishedfloor or sustained win from qualification. Parent424shared-driver
qualification remains independent. Full timing requires qualified shared424artifact,
samehost/compiler/profile/workload plus explicit measured semantic differences.

Mac execution: original424qualifier mustterminal first. Copyonly1357 verifiedsourcepaths fromexistingMacanchor intoa new/tmp sourceoutsidetheVoteBoatworkspace. Independent196packageminimallock remainsidentical. Changeexactoneconstant, comparecandidatehashmap withqualifiedLinuxguardmap, preserveoriginal400s source/artifact. Buildfreshengine+driver actualfalsefresh proofs/defaultfeatures/thin1 andcopymode beforefunctionalCLI(batch1/4,16384originals,1024warmup,threevoters). Retainoriginalhandles/exits/rawfirstfailures. No timedbenchmarks; no newdependencyresolution orfetch. Afterhandbackholdallnewbuilds/copiesuntilroot timingrelease.

Focused preflight correction: original Macjob26464/PID85023 failed beforeCargo on an inaccurate149-package assertion. Both anchors actuallyhave196package records withunchangedSHA3ef72b...; matchedsharedlock150records/149nonrootdependenciesisindependent. RootverifiedviaTOML andauthorizedmetadata/assertion-onlycorrection. Preserveoriginalfailure/scripts/logs, use newcorrectedjob/source/target; all1357 candidatehashes andone400→3600sourcepatch remainidentical. No resolution, source/lockhashchanges, orunchangedrestart.
