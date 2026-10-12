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
use super::{invalid, record::*, StripedIo, StripedLimits};
use crate::{format_crc::crc32c, identity::StoreIdentity, log::LogLimits};
use std::io;
#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct Catalog {
    pub epoch: u64,
    pub generation: u64,
    pub lanes: usize,
    pub manifest: Vec<u8>,
    pub records: Vec<Record>,
}
impl Catalog {
    pub fn empty(lanes: usize) -> Self {
        Self {
            epoch: 0,
            generation: 1,
            lanes,
            manifest: Vec::new(),
            records: Vec::new(),
        }
    }
    pub fn encode(&self, limit: usize) -> io::Result<Vec<u8>> {
        let size = self.encoded_len()?;
        if size > limit {
            return Err(invalid("striped catalog budget"));
        }
        let mut out = Vec::with_capacity(size);
        out.extend(b"VBSTRP01");
        out.extend(self.epoch.to_le_bytes());
        out.extend(self.generation.to_le_bytes());
        put(&mut out, self.lanes)?;
        put(&mut out, self.manifest.len())?;
        out.extend(&self.manifest);
        put(&mut out, self.records.len())?;
        for record in &self.records {
            put(&mut out, record.length)?;
            out.extend(record.checksum.to_le_bytes());
            put(&mut out, record.control.len())?;
            put(&mut out, record.parts.len())?;
            out.extend(&record.control);
            for part in &record.parts {
                put(&mut out, part.lane)?;
                out.extend((part.offset as u64).to_le_bytes());
                put(&mut out, part.length)?;
                out.extend(part.checksum.to_le_bytes());
            }
        }
        out.extend(crc32c(&out).to_le_bytes());
        Ok(out)
    }
    pub fn encoded_len(&self) -> io::Result<usize> {
        self.records
            .iter()
            .try_fold(40 + self.manifest.len(), |size, record| {
                size.checked_add(16)
                    .and_then(|n| n.checked_add(record.control.len()))
                    .and_then(|n| {
                        record
                            .parts
                            .len()
                            .checked_mul(20)
                            .and_then(|m| n.checked_add(m))
                    })
                    .ok_or_else(|| invalid("catalog size overflow"))
            })
    }
    pub fn decode(bytes: &[u8], limits: LogLimits, striped: StripedLimits) -> io::Result<Self> {
        if bytes.len() < 40
            || bytes.len() > striped.max_catalog_bytes
            || &bytes[..8] != b"VBSTRP01"
            || crc32c(&bytes[..bytes.len() - 4])
                != u32::from_le_bytes(bytes[bytes.len() - 4..].try_into().unwrap())
        {
            return Err(invalid("striped catalog framing"));
        }
        let mut r = Reader {
            bytes: &bytes[8..bytes.len() - 4],
        };
        let epoch = r.u64()?;
        let generation = r.u64()?;
        let lanes = r.u32()?;
        let n = r.u32()?;
        if n != 52 {
            return Err(invalid("native manifest size"));
        }
        let manifest = r.take(n)?.to_vec();
        let count = r.u32()?;
        if count > striped.max_records || count > r.bytes.len() / 16 {
            return Err(invalid("striped record count"));
        }
        let mut records = Vec::with_capacity(count);
        for _ in 0..count {
            records.push(r.record(limits, striped)?);
        }
        if !r.bytes.is_empty() || epoch == 0 || generation == 0 || !(2..=8).contains(&lanes) {
            return Err(invalid("striped catalog fields"));
        }
        Ok(Self {
            epoch,
            generation,
            lanes,
            manifest,
            records,
        })
    }
    pub fn lengths(&self, limits: LogLimits, striped: StripedLimits) -> io::Result<Vec<usize>> {
        let mut lengths = vec![0usize; self.lanes];
        let mut total = 0usize;
        for record in &self.records {
            let mut size = 0usize;
            for part in &record.parts {
                let length = lengths
                    .get_mut(part.lane)
                    .ok_or_else(|| invalid("payload lane identity"))?;
                if part.offset != *length || part.length == 0 || part.length > striped.chunk_bytes {
                    return Err(invalid("striped chunk discontinuity"));
                }
                *length = length
                    .checked_add(part.length)
                    .ok_or_else(|| invalid("lane size overflow"))?;
                size = size
                    .checked_add(part.length)
                    .ok_or_else(|| invalid("frame size overflow"))?;
            }
            if size != record.length || size == 0 {
                return Err(invalid("striped frame coverage"));
            }
            total = total
                .checked_add(size)
                .ok_or_else(|| invalid("journal size overflow"))?;
        }
        let (_, boundary) =
            super::super::read_manifest(&self.manifest).map_err(super::encode_error)?;
        if total > limits.max_wal_bytes || total as u64 != boundary {
            return Err(invalid("striped durable boundary"));
        }
        Ok(lengths)
    }
}
pub(super) fn select<I: StripedIo>(
    io: &mut I,
    identity: StoreIdentity,
    limits: LogLimits,
    striped: StripedLimits,
) -> io::Result<Catalog> {
    let mut selected: Option<Catalog> = None;
    for lane in 0..io.lane_count() {
        let bytes = match io.read_catalog(lane, striped.max_catalog_bytes) {
            Ok(bytes) => bytes,
            Err(e) if e.kind() == io::ErrorKind::NotFound => continue,
            Err(e) => return Err(e),
        };
        let candidate = Catalog::decode(&bytes, limits, striped)?;
        if candidate.lanes != io.lane_count()
            || super::super::read_manifest(&candidate.manifest)
                .map_err(super::encode_error)?
                .0
                .identity
                != identity
        {
            return Err(invalid("striped catalog store/lane identity"));
        }
        candidate.lengths(limits, striped)?;
        let groups = controls(&candidate.records, limits)?;
        if groups.values().any(|group| {
            group
                .snapshot
                .is_some_and(|snapshot| snapshot.store != identity)
        }) {
            return Err(invalid("foreign striped control snapshot"));
        }
        if let Some(old) = &selected {
            if candidate.epoch == old.epoch && candidate != *old {
                return Err(invalid("divergent striped catalog epoch"));
            }
            if candidate.epoch <= old.epoch {
                continue;
            }
        }
        selected = Some(candidate);
    }
    selected.ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "striped catalogs missing"))
}
fn put(out: &mut Vec<u8>, value: usize) -> io::Result<()> {
    out.extend(
        u32::try_from(value)
            .map_err(|_| invalid("catalog integer overflow"))?
            .to_le_bytes(),
    );
    Ok(())
}
struct Reader<'a> {
    bytes: &'a [u8],
}
impl<'a> Reader<'a> {
    fn take(&mut self, n: usize) -> io::Result<&'a [u8]> {
        if n > self.bytes.len() {
            return Err(invalid("catalog field truncated"));
        }
        let (head, rest) = self.bytes.split_at(n);
        self.bytes = rest;
        Ok(head)
    }
    fn u32(&mut self) -> io::Result<usize> {
        Ok(u32::from_le_bytes(self.take(4)?.try_into().unwrap()) as usize)
    }
    fn u64(&mut self) -> io::Result<u64> {
        Ok(u64::from_le_bytes(self.take(8)?.try_into().unwrap()))
    }
    fn record(&mut self, limits: LogLimits, striped: StripedLimits) -> io::Result<Record> {
        let length = self.u32()?;
        let checksum = self.u32()? as u32;
        let controls = self.u32()?;
        let parts = self.u32()?;
        if length > limits.max_wal_bytes
            || controls > length
            || parts > length
            || controls > self.bytes.len()
            || parts > self.bytes.len().saturating_sub(controls) / 20
        {
            return Err(invalid("catalog record budget"));
        }
        let control = self.take(controls)?.to_vec();
        if super::record::control_frame(&control, limits)? != control {
            return Err(invalid("noncanonical striped controls"));
        }
        let mut references = Vec::with_capacity(parts);
        for _ in 0..parts {
            references.push(Part {
                lane: self.u32()?,
                offset: usize::try_from(self.u64()?)
                    .map_err(|_| invalid("lane offset overflow"))?,
                length: self.u32()?,
                checksum: self.u32()? as u32,
            });
        }
        if striped.chunk_bytes == 0 {
            return Err(invalid("zero stripe chunk"));
        }
        Ok(Record {
            length,
            checksum,
            control,
            parts: references,
        })
    }
}
