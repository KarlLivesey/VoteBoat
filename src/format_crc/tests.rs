// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
use super::{crc32c, crc32c_extend};

// Original production bitwise implementation, independent of the lookup table.
fn bitwise_extend(previous: u32, bytes: &[u8]) -> u32 {
    let mut crc = !previous;
    for byte in bytes {
        crc ^= u32::from(*byte);
        for _ in 0..8 {
            crc = (crc >> 1) ^ (0x82f63b78 & 0u32.wrapping_sub(crc & 1));
        }
    }
    !crc
}

#[test]
fn known_vector_and_empty_extensions_preserve_finalized_seed() {
    assert_eq!(crc32c(b""), 0);
    assert_eq!(crc32c(b"123456789"), 0xe306_9283);
    assert_eq!(crc32c_extend(crc32c(b"1234"), b"56789"), 0xe306_9283);
    for previous in [0, 1, u32::MAX, 0x8000_0000, 0x0123_4567, 0xaa55_aa55] {
        assert_eq!(crc32c_extend(previous, b""), previous);
    }
}

#[test]
fn all_byte_values_match_original_with_diverse_previous_bits() {
    let previous_values = [0, u32::MAX, 0xaa55_aa55, 0x55aa_55aa, 0x8123_4567]
        .into_iter()
        .chain((0..32).flat_map(|bit| [1u32 << bit, !(1u32 << bit)]));
    for previous in previous_values {
        for byte in 0..=u8::MAX {
            assert_eq!(
                crc32c_extend(previous, &[byte]),
                bitwise_extend(previous, &[byte]),
                "seed {previous:#010x}, byte {byte:#04x}"
            );
        }
    }
}

#[test]
fn payload_boundaries_and_chunked_updates_match_original() {
    for len in [
        0, 1, 7, 8, 15, 16, 31, 32, 37, 63, 64, 65, 255, 256, 257, 1023, 1024, 4096,
    ] {
        let bytes: Vec<_> = (0..len)
            .map(|index| ((index * 37 + 19) & 0xff) as u8)
            .collect();
        for previous in [0, u32::MAX, 0x8000_0001, 0x0123_4567, 0xaa55_aa55] {
            let expected = bitwise_extend(previous, &bytes);
            assert_eq!(crc32c_extend(previous, &bytes), expected, "length {len}");
            for split in 0..=len.min(257) {
                let first = crc32c_extend(previous, &bytes[..split]);
                assert_eq!(
                    crc32c_extend(first, &bytes[split..]),
                    expected,
                    "length {len}, split {split}"
                );
            }
            let mut accumulated = previous;
            for chunk in bytes.chunks(37) {
                accumulated = crc32c_extend(accumulated, b"");
                accumulated = crc32c_extend(accumulated, chunk);
            }
            assert_eq!(crc32c_extend(accumulated, b""), expected);
        }
    }
}
