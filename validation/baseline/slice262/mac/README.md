# Terminal Mac262 lifecycle run

The original frozen654-input image is terminal: 62 passed, 5 failed, exit101,
in10988.80s. Its start and terminal source guards verify; its source manifest
matches the retained Linux262 image. This is an older image, not current main.

Two failures are the automatic metadata read assertion at metadata_lookup.rs247,
subsequently corrected and separately qualified in268. Three failures remain
separate older progress reports: QUIC repeated moves WAL/checkpoint at split.rs63,
and TCP repeated moves WAL at delegation_repeat.rs597. No terminal result from
this run qualifies a newer correction or diagnoses those progress failures.

The complete emitted log and actual exit record are retained here. Native failed
stores and immutable checkout remain on the Mac. Raw and normalized hashes are
separate. These finite results leave full platform/P0–P7 acceptance open.
