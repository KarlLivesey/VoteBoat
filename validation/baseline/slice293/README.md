# Shared host/native wire provisioning obligations

Linux and macOS each pass72 named executions across the complete wire and
configuration-capacity targets: all23, default23, core-only4 and native-only22.
Formatting and strict all-target Clippy for all four profiles have zero diagnostics
on both hosts. Executed before/terminal673-input guards verify; current Rust
publication inputs match. The zero-context runtime patch applies over292.

The same provisioning/scope assertions now run with the existing independent
single-fixture host codec (compatibility defaults) and native direct codecs.
They check sizing versus encoded length, exact supplied bytes and decoded
messages, too-short/too-long destination refusal, original-input reuse and
wrong-peer scope refusal in every encode/decode path. Three deliberately defective
providers trip these assertions: false size, altered supplied bytes and scope-
blind encoding. Refusal may modify destination bytes; no rollback is required.

Full existing configuration-capacity targets check native frame/decoded footprints,
retained membership operation history, declared application envelopes and
unsupported format/policy/input refusal. The independent host supports only
ReadProbe and refuses optional configuration counting; no full alternative codec
or host implementation of supported counting is certified.

Metadata integrity passes for108 inventory entries and15 partial reviews/97
operations. The direct metadata checker suite passes27 cases. Its first run
failed the old14/88 expected counts; that original log is retained separately.
Expected counts and wire-specific rejection cases were updated for the actual
new review, with no acceptance waiver. Metadata shape/anchors remain evidence
of references only. No production provider, wire format, ownership, timer,
consensus or persistent format changes; broader provider/lifecycle/P0–P7 remains
open, including the independent original Mac285 run.
