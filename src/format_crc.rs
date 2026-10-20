// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
// Unless explicitly acquired and licensed from Licensor under another license,
// the contents of this file are subject to the Reciprocal Public License
// ("RPL") Version 1.5, or subsequent versions as allowed by the RPL, and You may
// not copy or use this file in either source code or executable form, except
// in compliance with the terms and conditions of the RPL.
//
// All software distributed under the RPL is provided strictly on an "AS IS"
// basis, WITHOUT WARRANTY OF ANY KIND, EITHER EXPRESS OR IMPLIED, AND LICENSOR
// HEREBY DISCLAIMS ALL SUCH WARRANTIES, INCLUDING WITHOUT LIMITATION, ANY
// WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE, QUIET
// ENJOYMENT, OR NON-INFRINGEMENT. See the RPL for specific language governing
// rights and limitations under the RPL.
/// Standard reflected CRC32C (Castagnoli), for accidental corruption only.
pub(crate) fn crc32c(bytes: &[u8]) -> u32 {
    crc32c_extend(0, bytes)
}
/// Slicing-by-8: eight independent table lookups per 8-byte word instead of
/// one dependent lookup per byte. Same function as the byte-wise table loop
/// (checked against the original bitwise form in the tests); safe Rust only.
pub(crate) fn crc32c_extend(previous: u32, bytes: &[u8]) -> u32 {
    let t = &CRC32C_TABLES;
    let mut crc = !previous;
    let (words, rest) = bytes.as_chunks::<8>();
    for word in words {
        let low = u32::from_le_bytes([word[0], word[1], word[2], word[3]]) ^ crc;
        let high = u32::from_le_bytes([word[4], word[5], word[6], word[7]]);
        crc = t[7][(low & 0xff) as usize]
            ^ t[6][((low >> 8) & 0xff) as usize]
            ^ t[5][((low >> 16) & 0xff) as usize]
            ^ t[4][(low >> 24) as usize]
            ^ t[3][(high & 0xff) as usize]
            ^ t[2][((high >> 8) & 0xff) as usize]
            ^ t[1][((high >> 16) & 0xff) as usize]
            ^ t[0][(high >> 24) as usize];
    }
    for byte in rest {
        crc = (crc >> 8) ^ t[0][((crc ^ u32::from(*byte)) & 0xff) as usize];
    }
    !crc
}

/// `CRC32C_TABLES[0]` is the byte-wise table; `[k][i]` advances `[k - 1][i]`
/// by one more zero byte.
static CRC32C_TABLES: [[u32; 256]; 8] = crc32c_tables();

const fn crc32c_tables() -> [[u32; 256]; 8] {
    let mut tables = [[0; 256]; 8];
    let mut index = 0;
    while index < 256 {
        let mut value = index as u32;
        let mut bit = 0;
        while bit < 8 {
            value = (value >> 1) ^ (0x82f63b78 & 0u32.wrapping_sub(value & 1));
            bit += 1;
        }
        tables[0][index] = value;
        index += 1;
    }
    let mut k = 1;
    while k < 8 {
        let mut index = 0;
        while index < 256 {
            let previous = tables[k - 1][index];
            tables[k][index] = (previous >> 8) ^ tables[0][(previous & 0xff) as usize];
            index += 1;
        }
        k += 1;
    }
    tables
}

#[cfg(test)]
mod tests;
