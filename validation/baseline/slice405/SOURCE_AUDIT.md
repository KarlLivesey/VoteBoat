# Capability-boundary note

This audit was captured before the user explicitly removed the fixed shutdown-
speed gate from P8. Its pending test outcomes remain pending facts, not missing
capabilities. The source reconciliation and historical failures are unchanged.
See [current feature status](../../../docs/FEATURE_STATUS.md) for the final
user-confirmed boundary. No pending test is relabelled as passed.

# Draft final requirement/source audit405

**Not complete yet:** Mac405 full original cold-QUIC remains pending. The current full routed192 regression is an additional pending check. Linux405 full nested all15/default13 are now terminal passes. This read-only audit ran no tests and changed no production/docs.

Current frozen source has1307 compilation inputs; all1307 current hashes match. Compared with broad384:23 existing inputs change,2 are added,1 removed. Only2 production files change: the additive diagnostic-clock implementation. After removing exactly that addition, both old runtime files compare byte-for-byte with384. Cargo/dependency/toolchain/feature files are unchanged.

## Replacements

| Family | Concrete evidence | Remaining |
| --- | --- | --- |
| clock | Linux runtime54/54/37/54 +native2; Mac selected1+2. Old monotonic/scheduler code is byte-identical after removing new diagnostic declarations/impl/private tests. No consensus caller. | None for selected scope |
| startup | Linux and Mac full startup all57/default42; current fixture source unchanged after396. Cooperative drain callback keeps surviving replicas polling during source cleanup; the earlier bare failure cause is not inferred. No new production behavior. | None for selected scope |
| logical_activation_nested | Linux393 all14 plus exact lower-fixture replacement398 repeated2, default400 full8. Mac393 Joint2, Mac398 repeated2, prior383 imported/Unknown6 and earlier first replacement. No changed405 activation fixture. | None for selected scope |
| logical_nested_native | Current405 full Linux nested all15/0 in124.51s and default13/0 in46.77s, each with0filtered. Mac TCP4021/0 with full27-store recovery/cleanup in850.99s. Current405 only shares stronger original-close predicate, no production change. Mac failed-QUIC405 needs actual terminal result; do not promote partial log to pass. | Mac405 full original coldQUIC terminal result and source/artifact guards |
| routed | Existing pure close predicate moved to shared test module, same caller poll/background/diagnostic functions. Strengthened live-set/time and local/connector counts. Six controls+actual routed longRTT1+nativeQUIC1 pass; original broad384 routed history evidence retained. Additional current full192 routed regression is running; not counted successful. | Current full routed192 regression (additional confidence check) |

## Numbered requirements

71 inherited403 rows retain their concrete reviewed assertion bundles and bounded evidence. Clock and native100-group obligations are now closed by actual raw Mac results. Rows touching deep logical recovery remain pending the final QUIC result. No absent named functionality was found; this is not an unbounded correctness or universal provider proof.

| ID | Status | Outstanding gate |
| --- | --- | --- |
| R01 | bounded_requirement_evidence_reconciled |  |
| R02 | bounded_requirement_evidence_reconciled |  |
| R03 | bounded_requirement_evidence_reconciled |  |
| R04 | bounded_requirement_evidence_reconciled |  |
| R05 | bounded_requirement_evidence_reconciled |  |
| R06 | bounded_requirement_evidence_reconciled |  |
| R07 | bounded_requirement_evidence_reconciled |  |
| R08 | bounded_requirement_evidence_reconciled |  |
| R09 | pending_final_mac_deep_quic | Q-DEEP-MAC |
| R10 | bounded_requirement_evidence_reconciled |  |
| R11 | bounded_requirement_evidence_reconciled |  |
| R12 | bounded_requirement_evidence_reconciled |  |
| R13 | bounded_requirement_evidence_reconciled |  |
| R14 | bounded_requirement_evidence_reconciled |  |
| R15 | bounded_requirement_evidence_reconciled |  |
| R16 | bounded_requirement_evidence_reconciled |  |
| R17 | bounded_requirement_evidence_reconciled |  |
| R18 | bounded_requirement_evidence_reconciled |  |
| R19 | pending_final_mac_deep_quic | Q-DEEP-MAC |
| VB-000 | bounded_requirement_evidence_reconciled |  |
| VB-001 | bounded_requirement_evidence_reconciled |  |
| VB-002 | bounded_requirement_evidence_reconciled |  |
| VB-003 | bounded_requirement_evidence_reconciled |  |
| VB-004 | bounded_requirement_evidence_reconciled |  |
| VB-005 | bounded_requirement_evidence_reconciled |  |
| VB-006 | bounded_requirement_evidence_reconciled |  |
| VB-007 | bounded_requirement_evidence_reconciled |  |
| VB-008 | bounded_requirement_evidence_reconciled |  |
| VB-009 | bounded_requirement_evidence_reconciled |  |
| VB-010 | bounded_requirement_evidence_reconciled |  |
| VB-011 | bounded_requirement_evidence_reconciled |  |
| I01 | bounded_requirement_evidence_reconciled |  |
| I02 | bounded_requirement_evidence_reconciled |  |
| I03 | bounded_requirement_evidence_reconciled |  |
| I04 | bounded_requirement_evidence_reconciled |  |
| I05 | bounded_requirement_evidence_reconciled |  |
| I06 | bounded_requirement_evidence_reconciled |  |
| I07 | pending_final_mac_deep_quic | Q-DEEP-MAC |
| I08 | bounded_requirement_evidence_reconciled |  |
| I09 | pending_final_mac_deep_quic | Q-DEEP-MAC |
| I10 | bounded_requirement_evidence_reconciled |  |
| I11 | pending_final_mac_deep_quic | Q-DEEP-MAC |
| I12 | bounded_requirement_evidence_reconciled |  |
| C01 | bounded_requirement_evidence_reconciled |  |
| C02 | bounded_requirement_evidence_reconciled |  |
| C03 | bounded_requirement_evidence_reconciled |  |
| C04 | bounded_requirement_evidence_reconciled |  |
| C05 | bounded_requirement_evidence_reconciled |  |
| C06 | bounded_requirement_evidence_reconciled |  |
| C07 | bounded_requirement_evidence_reconciled |  |
| C08 | bounded_requirement_evidence_reconciled |  |
| C09 | bounded_requirement_evidence_reconciled |  |
| C10 | bounded_requirement_evidence_reconciled |  |
| C11 | bounded_requirement_evidence_reconciled |  |
| C12 | bounded_requirement_evidence_reconciled |  |
| C13 | bounded_requirement_evidence_reconciled |  |
| C14 | bounded_requirement_evidence_reconciled |  |
| C15 | bounded_requirement_evidence_reconciled |  |
| C16 | bounded_requirement_evidence_reconciled |  |
| C17 | bounded_requirement_evidence_reconciled |  |
| C18 | bounded_requirement_evidence_reconciled |  |
| C19 | bounded_requirement_evidence_reconciled |  |
| C20 | bounded_requirement_evidence_reconciled |  |
| C21 | bounded_requirement_evidence_reconciled |  |
| C22 | bounded_requirement_evidence_reconciled |  |
| C23 | bounded_requirement_evidence_reconciled |  |
| C24 | bounded_requirement_evidence_reconciled |  |
| P8-striped | bounded_requirement_evidence_reconciled |  |
| P8-relays | bounded_requirement_evidence_reconciled |  |
| P8-logical | pending_final_mac_deep_quic | Q-DEEP-MAC |
| P8-transactions | bounded_requirement_evidence_reconciled |  |

## Preserved failures

- Broad Linux384 all2436/2 (startup); default2124/0,core1028/0,native1511/0 are historical only.
- Mac315b native179/1 remains original failed matrix; selected native100403 now passes original case.
- Mac383 repeated2 and Joint2 remain failed in that source; later393/398 targeted passes replace those paths.
- Mac398 coldTCP/QUIC fail old15s CheckpointPublished;402 extends only test checkpoint watchdog while requiring actual frontier/ref/router/worker.
- Mac402 QUIC0/1 843.17s fails old15s CloseDrained after local work settles. No provider state/deadline was captured;405 shared predicate must prove eligibility dynamically.

The JSON contains exact input deltas, per-fixture Rust depinfo mappings, stable evidence hashes and unchanged production proof. Actual405 pending output must be inspected and added before final completion. P9/P10/Windows and unselected future adapters retain the explicit user scope recorded in403.
