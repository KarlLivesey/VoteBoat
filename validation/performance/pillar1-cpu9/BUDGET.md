# CPU-9 ownership budget: source ledger

This ledger addresses the released one-ring change. Linux probes and the966-case
independent old-VecDeque capacity/FIFO oracle passed with the normal source.
It does not establish Apple machine movement, stack frame size, RSS, timing, or
a gain. Final ownership/feature/control/strict checks remain separate gates.

Persistent storage is the identical `capacity * size_of::<Effect>()` allocation
plus the exact original payload/backing/spare-capacity charges. On this Linux
64-bit build Effect is 272 bytes, VecDeque and EffectQueue are both 32 bytes,
EffectFront is 16, its individual exclusive guards are 8, VolatilePersist is 24,
and the temporary prefix Drain is 40. There is one ring descriptor per Actor or
Active owner, no new retained field, queue, descriptor, pool, counter or startup
allocation. The test-only shutdown capture is excluded from normal builds and
exists only to inspect completed real actor tasks, not in the product budget.
The largest real payload measured256 bytes; payload256 + borrow8 fits within
Effect272. Result<Vec<Effect>, RaftError> and Result<EffectQueue, RaftError> both
measured32 bytes, so cold Result plumbing adds no return-envelope header.

Capacity policy is part of the proof. Empty construction allocates nothing.
Known-N Vec output is adopted using the existing Vec-to-VecDeque conversion,
including capacity one. Queue concatenation reserves its source length once,
before any front guard exists, matching the old exact-size iterator extension;
it then transfers fronts without another growth. The source ring stays live
until concatenation finishes, as the old source Vec/IntoIter did. The destination
old/new allocation overlap on growth and every source spare slot remain paid.
Native empty-queue release drops the allocation at its original lifecycle point.

The simultaneous temporary sets are bounded by the old owned transfer, not by
unspent reservation slack. Front selection holds only its 16-byte enum. In the
volatile take phase that selection has been consumed: guard8 + token24 + Drain40
is at most72 bytes versus the original owned Effect272. The drain is dropped
without iteration, retains the allocation and ends before core completion.
Staging's None arm retains only its original token fields; the Some arm adds no
volatile locals alongside the owned LogUpdate. Both arms retain the original
plan/Pending and its payload costs and emit only after the same checks/install.

For large extraction the guard replaces the existing mutable deque receiver.
Its internal owned pop remains the original full-Effect pop and remains paid.
The typed payload escapes only as the guard is consumed; reconstruction of an
owned durable lease or a cold snapshot Effect occurs after that borrow ends.
The largest payload plus its single borrow must fit the old Effect envelope
(checked by size probes); selection and returned payload are separate ownership
phases. No guard survives core completion, append/growth, application, callback,
provider submission or async acceptance. No second copy of the payload's backing
is created. Actual compiler-generated temporaries remain an Apple gate.

Cold conversion/concatenation keeps the old simultaneous source and destination
slot capacities. The former source Vec24 and IntoIter32 become one source ring32
and a selected front (at most16 before take, or guard8 during take). Its payload
extraction remains owned; full-envelope moves of genuinely large values and
growth/insert/truncate remain paid. No compact-storage accounting is claimed.

For Active adoption, old Stepped/WorkerDelivery/callback Vec ownership remains
the real external boundary and is adopted without a second allocation. For a
callback the original live lease + old tail ring + callback Vec + insertion
growth overlap are all still present and charged. Written early Send retains
its original extra reservation and Send-only restrictions. Snapshot image/load
reservations and external application/read/capsule leases are unchanged. A
dropped front has no Drop action and cannot refund any of those credits.

The `effect_bytes` factor remains exactly4 *272 per actual capacity slot, with
the original payload charges; Raft's bound and pending-plan accounting remain
unchanged. No added fixed header or joint peak is assigned to that factor.
Defaults remain256 visits/256MiB, including16 control visits/32MiB protected
against bulk input. No active/lease/driver capacity, admission ceiling, startup
preallocation or traffic-class partition is increased or reduced. The strict
capacity/FIFO oracle, real callback/reservation/lease tests and provider mutants
must pass; Cargo feature compilation alone does not endorse these claims.
