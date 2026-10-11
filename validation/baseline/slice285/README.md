# Lifecycle receipt uncertainty285 — focused acceptance, full family pending

Retained/imported parent and repeated-target writes retain the actual accepted
Unknown(LeadershipChanged) observation from the bounded original-byte retry
helper. They use the same BucketReceipt operation/duplicate assertion as endpoint
writes: required retries remain duplicate; a fresh duplicate requires actual
accepted uncertainty. NotLeader alone cannot justify a duplicate.

The actual endpoint regression passes2/all and1/default on each host. It forces
a locally durable uncommitted command, observes its real unknown result, checks
original commitment, cold retry, exact data/outbox and replayed first-application
fresh receipt, and rejects a missing required duplicate. Formatting/four strict
profiles pass, and668 inputs verify before/after these regressions.

The corrected complete native delegation family is running73 all-feature tests
on separate frozen Linux/Mac images, followed by default tests. It is not accepted
yet. Linux has reported a QUIC checkpoint split read-progress timeout; the live
log/store/source remain separate. Original Mac269 is terminal62/7 on older662
inputs. These selected regressions do not certify full lifecycle acceptance.
