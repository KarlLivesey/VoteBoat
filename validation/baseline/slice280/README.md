# Retained original-record guard280: partial results

The reconciliation-only path consumes an actual admission-journal NotProposed
refusal only if the admitting core's durable log contains the exact original
Joint record and the current Joint identifies its operation. It grants no new
admission/commitment; all replicas must still commit original Joint before the
phase advances. Ordinary/controlled Final assertions remain strict.

Linux startup passes53/all and38/default. Mac50/all pass with3 failures and
37/default with1 failure; all failures hit original Joint reconciliation's10s
deadline. Formatting/four strict builds and668 before/terminal guards verify.
Actual retained-entry reports remain in the log. This is not full startup
acceptance.281 observes the real cancelled admission execution before acting,
so local absence while original work is still queued cannot trigger early retry.
Raw/normalized evidence and immutable original sources/failed roots are retained.
