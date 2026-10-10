# Explicit placement plans

`voteboat-counter placement-plan` turns an operator-provided membership and host
sample into the existing administration file. It can add a learner, replace one
voter or prepare an explicit voter policy. The native planner chooses a learner
by failure-domain occupancy, load, free bytes and node ID. Runtime authorization,
catch-up, joint consensus and durable finalization still govern execution.

## Input

For a counter group with voters1/2, configuration3, and a newly provisioned
node4 whose store is404/incarnation7:

```text
voteboat-placement-v1
group 1 1
placement 2 false
sample 1 0 100 1 1024
current 3 - m:2 v:1 v:2
history 1000
replica 1 1 1 1 true 1048576 1 100
replica 2 2 1 2 true 1048576 1 100
replica 4 404 7 4 true 1048576 1 100
```

Lines are ordered. `sample` gives generation, observed time, expiry time, current
time and minimum free bytes. Times use one host monotonic clock and milliseconds.
`current` gives the stable configuration ID, comma-separated learner IDs (`-`
for none), and the existing prefix policy grammar. `history` lists used
configuration operation IDs separated by commas, or `-`. Each `replica` gives
node, store ID, store incarnation, failure domain, enabled flag, free bytes,
free replica slots and load in0..1000.

Obtain membership and history from the actual inspected state and supply a
consistent host sample. Input is an operator assertion, not a quorum receipt or
resource reservation. Planning validates its internal consistency and declared
expiry; execution independently checks the actual current configuration and
store identities. This counter CLI targets group1/incarnation1. Rust hosts retain
the general public placement interfaces.

Input is bounded to64KiB and64 replicas. It must include all current voters and
learners, plus any candidate targets. New replica candidates need a free slot
and enough space; promotion of an existing learner does not allocate a second
copy. Failure-domain requirements remain authoritative at execution.

## Generate and execute

Choose one action:

```sh
voteboat-counter placement-plan input.txt learner 1001 > learner.plan
voteboat-counter placement-plan input.txt replace 1 1001 1002 retain > replacement.plan
voteboat-counter placement-plan input.txt voters 1002 retire w:2 1 v:1 1 v:2 > voters.plan
```

Replacement preserves the old policy tree and weights, substitutes the selected
new voter, and emits learner, joint and final records. `retain` leaves removed
voters as learners after finalization; `retire` removes them. Voter-policy changes
may promote only already assigned learners; runtime still requires live readiness.

Review and preserve the generated file. It includes exact store IDs/incarnations
on each replica line and the complete original operation IDs and records. Feed
it to the existing [administration path](COUNTER_SERVICE.md#trusted-startup-administration-plan):

```sh
voteboat-counter serve recover-member /your/node1 1 43000 /your/tls \
  --deployment deployment.txt --admin-plan replacement.plan
```

Provision the same plan on the participating members. `--remote-admin-plan`
can instead require explicit authenticated `configure OPERATION` requests.
A missing target stays non-voting until the existing enrollment and readiness
steps succeed. The planner does not create stores, issue certificates or move
application ownership. Resume an interrupted operation using the **same saved
file**; rerunning selection may choose a different target.

Generated replica lines use `replica NODE DOMAIN STORE INCARNATION`. Existing
`replica NODE DOMAIN` files remain accepted. An explicit identity mismatch with
the selected deployment fails before runtime files are opened.

TCP/QUIC tests execute a generated voter change and replacement, stop before
new-node enrollment, and recover the same records after checkpoints. Automatic
online sample collection, live plan publication and broader fault/platform
coverage remain separate work.
