# Independent roots for retained failed histories

Delegated::with_metadata previously selected one directory per process/protocol/
checkpoint. A failed history retains its stores; a later history may then Create
against those files. A controlled actual FileLogIo/NativeLogStore check closes
and retains the first store, then constructs another under the same profile.
The old selector fails0/1 on both Linux and macOS with AlreadyExists at the second
Create. Its source matches negative-source.sha256; negative.patch reconstructs
that selector from the fixed source.

The fixed selector allocates an exclusively created directory using a checked
process-local sequence and at most32 collision attempts. It never deletes an
existing directory. The native regression passes1/0 on both hosts, comparing
the retained original bytes and explicitly closing/removing successful stores.
Formatting and default/all/core/native strict Clippy report zero diagnostics on
both hosts;653 build inputs verify at terminal. No production/API/format/protocol/
deadline/quota/fault-cut change. The private helper exists only to give each
history independent store ownership, including after another history fails.

The first Linux setup omitted the parent200 directory; its NotFound failure is
retained separately and is not causal evidence. FileLogIo creates the selected
store directory, so the corrected setup explicitly creates its parent first.

The four affected real cross-authority TCP/QUIC WAL/checkpoint histories run
separately on fixed Linux/Mac sources and remain pending at publication. This
scope establishes the controlled root-isolation correction; it does not identify
the first failure in the unchanged broad Mac family, classify every later failure
as contamination or claim that either original Mac run now passes. Failed roots
remain untouched. Preserved Linux254d65/0 applies to its earlier source only.

Linux stable1.98.1/macOS arm64 stable1.98.0; Mac child-shell descriptor limit4096
and task-owned TMPDIR remain explicit. Raw.sha256 hashes input bytes;
published.sha256 hashes path-normalized text with trailing whitespace removed.
final.patch is relative to7fc57d0; negative.patch applies to the fixed test file.
