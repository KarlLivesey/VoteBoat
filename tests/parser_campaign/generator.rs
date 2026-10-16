// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
use std::panic::{catch_unwind, AssertUnwindSafe};

pub(super) const SEEDS: [u64; 8] = [1, 7, 19, 43, 101, 257, 1021, 65537];
const CASES: usize = 192;
#[derive(Clone, Debug)]
pub(super) struct Case {
    pub(super) seed: u64,
    pub(super) index: usize,
    pub(super) mode: usize,
    pub(super) bytes: Vec<u8>,
}
impl Case {
    pub(super) fn original(&self) -> bool {
        self.mode == 0
    }
    #[cfg(feature = "native")]
    pub(super) fn integrity_damage(&self) -> bool {
        matches!(self.mode, 1 | 2)
    }
    pub(super) fn repair(&self) -> bool {
        matches!(self.mode, 5 | 6)
    }
}
#[derive(Default, Debug)]
pub(super) struct Counts {
    accepted: usize,
    refused: usize,
    structured_overwrites: usize,
}
fn next(random: &mut u64) -> u64 {
    *random = random
        .wrapping_mul(6_364_136_223_846_793_005)
        .wrapping_add(1);
    *random
}
fn position(random: &mut u64, bound: usize) -> usize {
    usize::try_from(next(random) % u64::try_from(bound).unwrap()).unwrap()
}
fn mutate(original: &[u8], fields: &[(usize, usize)], seed: u64, index: usize) -> Case {
    let mut random = seed ^ (u64::try_from(index).unwrap() + 1).wrapping_mul(0x9e37_79b9_7f4a_7c15);
    let mut bytes = original.to_vec();
    let mode = index % 8;
    let at = position(&mut random, original.len());
    match mode {
        0 => (),
        1 => bytes[at] ^= 1 << (next(&mut random) % 8),
        2 => bytes.truncate(at),
        3 => {
            let count = 1 + position(&mut random, 16);
            let inserted = (0..count).map(|_| next(&mut random).to_le_bytes()[0]);
            bytes.splice(at..at, inserted);
        }
        4 => {
            bytes.drain(at..at + 1 + position(&mut random, bytes.len() - at));
        }
        5 => {
            let count = 1 + position(&mut random, (bytes.len() - at).min(8));
            for byte in &mut bytes[at..at + count] {
                *byte = next(&mut random).to_le_bytes()[0];
            }
        }
        6 => {
            let (offset, width) = fields[position(&mut random, fields.len())];
            let value = [0, 1, u64::MAX, u64::from(u32::MAX)][position(&mut random, 4)];
            bytes[offset..offset + width].copy_from_slice(&value.to_le_bytes()[..width]);
        }
        _ => {
            let length = position(&mut random, original.len() + 33);
            bytes = (0..length)
                .map(|_| next(&mut random).to_le_bytes()[0])
                .collect();
        }
    }
    Case {
        seed,
        index,
        mode,
        bytes,
    }
}
pub(super) fn campaign(
    name: &str,
    original: &[u8],
    fields: &[(usize, usize)],
    check: impl FnMut(&Case) -> bool,
) {
    campaign_with(name, original, fields, false, check);
}
/// Every generated case must be accepted, rather than a mix of outcomes.
#[cfg(feature = "native")]
pub(super) fn campaign_all_accepted(
    name: &str,
    original: &[u8],
    fields: &[(usize, usize)],
    check: impl FnMut(&Case) -> bool,
) {
    campaign_with(name, original, fields, true, check);
}
fn campaign_with(
    name: &str,
    original: &[u8],
    fields: &[(usize, usize)],
    all_accepted: bool,
    mut check: impl FnMut(&Case) -> bool,
) {
    assert!(!original.is_empty());
    assert!(fields
        .iter()
        .all(|(offset, width)| *width <= 8 && offset + width <= original.len()));
    let mut counts = Counts::default();
    for seed in SEEDS {
        for index in 0..CASES {
            let case = mutate(original, fields, seed, index);
            assert!(case.bytes.len() <= original.len() + 32);
            let before = case.bytes.clone();
            let result = catch_unwind(AssertUnwindSafe(|| check(&case)));
            let accepted = result.unwrap_or_else(|_| {
                panic!(
                    "{name} seed={} case={} mode={} bytes={:02x?}",
                    case.seed, case.index, case.mode, case.bytes
                )
            });
            assert_eq!(before, case.bytes, "borrowed parser input changed");
            counts.accepted += usize::from(accepted);
            counts.refused += usize::from(!accepted);
            counts.structured_overwrites += usize::from(case.repair());
        }
    }
    if all_accepted {
        assert_eq!(counts.refused, 0);
    } else {
        assert!(counts.accepted > 0 && counts.refused > 0);
    }
    assert_eq!(counts.accepted + counts.refused, SEEDS.len() * CASES);
    println!(
        "{name}: seeds={SEEDS:?} cases={} {counts:?}",
        SEEDS.len() * CASES
    );
}
pub(super) fn crc(bytes: &[u8]) -> u32 {
    let mut value = !0_u32;
    for byte in bytes {
        value ^= u32::from(*byte);
        for _ in 0..8 {
            value = if value & 1 == 0 {
                value >> 1
            } else {
                (value >> 1) ^ 0x82f6_3b78
            };
        }
    }
    !value
}
#[cfg(feature = "native")]
pub(super) fn seal_tail(bytes: &mut [u8]) {
    let end = bytes.len() - 4;
    let checksum = crc(&bytes[..end]).to_le_bytes();
    bytes[end..].copy_from_slice(&checksum);
}
#[test]
fn independent_crc_has_the_standard_castagnoli_vector() {
    assert_eq!(crc(b"123456789"), 0xe306_9283);
}
