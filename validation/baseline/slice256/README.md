# Mixed Linux/macOS service acceptance

The existing counter executable runs two Linux replicas and one macOS arm64
replica on separate hosts. Both TCP/TLS and QUIC pass the same sequence:

1. Commit operation25601/delta7, retry it as duplicate, quorum-read7.
2. Kill Linux voter1; commit25602/delta5 with the remaining Linux/Mac quorum,
   quorum-read12 and recover voter1.
3. Kill the observed current leader; commit25603/delta3, quorum-read15 and
   recover that leader.
4. Admit checkpoints, explicitly quit/join all processes, cold-reopen all three,
   recover each original operation as its original duplicate receipt and read15.
5. Commit25604/delta2, read17 and explicitly quit all processes again.

Sixteen service incarnations terminate: four deliberate SIGKILL losses and twelve
successful graceful exits. Mac PID absence is checked after each exit. All six
successful temporary stores are removed after owned processes finish. There are
no unknown write replies in this recorded schedule. Fixture certificates are
the repository's public demo credentials; command endpoints remain local and
Mac commands run through the authorized SSH connection.

Linux stable1.98.1/macOS stable1.98.0 build the same 653 source inputs, verified
at terminal; binary digests and builds are retained. Production matches91576fa;
only the temporary test observer from254e differs, as source-difference.json
records. Formatting and all four strict Clippy profiles have zero diagnostics
on both hosts; Linux checks also verify restored91576fa build inputs.
This establishes selected separate-host static service compatibility,
not recursive lifecycle acceptance, a throughput result or a full platform pass.

The first preflight finds an occupied45002 port before starting any process.
The recorded runner then searches at most eight blocks, checks both hosts and
provisions an immutable peer file for each live cluster. TCP selects47200;
QUIC selects46100. Failed preflight/input source remain distinct from acceptance.
The runner records the actual host-specific paths/addresses for this execution;
those must be provisioned explicitly for another deployment.
Raw.sha256 hashes input bytes; published text trims trailing whitespace.
