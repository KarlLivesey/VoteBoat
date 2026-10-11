// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
use super::*;
use std::panic::{catch_unwind, AssertUnwindSafe};

enum Defect {
    Size,
    Bytes,
    Scope,
}
struct Broken {
    base: FixtureCodec,
    defect: Defect,
}
impl WireCodec for Broken {
    fn format_version(&self) -> u16 {
        self.base.format_version()
    }
    fn header_bytes(&self) -> usize {
        self.base.header_bytes()
    }
    fn limits(&self) -> WireLimits {
        self.base.limits()
    }
    fn frame_length(&self, bytes: &[u8]) -> Result<usize, WireError> {
        self.base.frame_length(bytes)
    }
    fn encode_batch(&self, binding: WireScope, input: &[Message]) -> Result<Vec<u8>, WireError> {
        self.base.encode_batch(
            if matches!(self.defect, Defect::Scope) {
                scope()
            } else {
                binding
            },
            input,
        )
    }
    fn decode_batch(&self, binding: WireScope, frame: &[u8]) -> Result<Vec<Message>, WireError> {
        self.base.decode_batch(binding, frame)
    }
    fn encoded_length(&self, binding: WireScope, input: &[Message]) -> Result<usize, WireError> {
        let size = self.base.encoded_length(binding, input)?;
        Ok(size + usize::from(matches!(self.defect, Defect::Size)))
    }
    fn encode_into(
        &self,
        binding: WireScope,
        input: &[Message],
        destination: &mut [u8],
    ) -> Result<(), WireError> {
        self.base.encode_into(binding, input, destination)?;
        if matches!(self.defect, Defect::Bytes) {
            destination[0] ^= 1;
        }
        Ok(())
    }
}
fn detects(defect: Defect) {
    let input = message(Rpc::ReadProbe);
    let codec = Broken {
        base: FixtureCodec {
            expected: input.clone(),
        },
        defect,
    };
    assert!(catch_unwind(AssertUnwindSafe(|| fixture_roundtrip(codec, input))).is_err());
}
#[test]
fn shared_assertions_detect_false_encoded_size() {
    detects(Defect::Size);
}
#[test]
fn shared_assertions_detect_altered_supplied_bytes() {
    detects(Defect::Bytes);
}
#[test]
fn shared_assertions_detect_scope_blind_encoding() {
    detects(Defect::Scope);
}
