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
use super::{catalog, invalid, record::*, storage, *};
use crate::format_crc::crc32c;
impl<I: StripedIo> StripedJournal<I> {
    fn limits(
        io: &I,
        limits: LogLimits,
        striped: StripedLimits,
    ) -> Result<StripedLimits, StorageError> {
        limits.validate()?;
        if !(2..=8).contains(&io.lane_count()) {
            return Err(StorageError::Rejected("striped lane count"));
        }
        striped.validate(limits)
    }
    pub fn create(
        mut io: I,
        identity: StoreIdentity,
        limits: LogLimits,
        striped: StripedLimits,
    ) -> Result<Self, StorageError> {
        let striped = Self::limits(&io, limits, striped)?;
        for lane in 0..io.lane_count() {
            match io.read_catalog(lane, striped.max_catalog_bytes) {
                Err(e) if e.kind() == io::ErrorKind::NotFound => (),
                Ok(_) => return Err(StorageError::Rejected("striped store already initialized")),
                Err(e) => return Err(storage(e)),
            }
            for generation in [1, 2] {
                if !io
                    .read_payload(lane, generation, limits.max_wal_bytes)
                    .map_err(storage)?
                    .is_empty()
                {
                    return Err(StorageError::Rejected(
                        "striped create requires empty payload",
                    ));
                }
            }
        }
        let lanes = io.lane_count();
        Ok(Self {
            io,
            identity,
            limits,
            striped,
            catalog: Catalog::empty(lanes),
            lengths: vec![0; lanes],
            next_lane: 0,
            synchronized: false,
        })
    }
    pub fn recover(
        mut io: I,
        identity: StoreIdentity,
        limits: LogLimits,
        striped: StripedLimits,
    ) -> Result<Self, StorageError> {
        let striped = Self::limits(&io, limits, striped)?;
        let catalog = catalog::select(&mut io, identity, limits, striped).map_err(storage)?;
        let lengths = catalog.lengths(limits, striped).map_err(storage)?;
        let next_lane =
            catalog.records.iter().map(|r| r.parts.len()).sum::<usize>() % catalog.lanes;
        Ok(Self {
            io,
            identity,
            limits,
            striped,
            catalog,
            lengths,
            next_lane,
            synchronized: false,
        })
    }
    pub fn inspect_control(
        io: &mut I,
        identity: StoreIdentity,
        limits: LogLimits,
        striped: StripedLimits,
    ) -> Result<StripedControlImage, StorageError> {
        let striped = Self::limits(io, limits, striped)?;
        let catalog = catalog::select(io, identity, limits, striped).map_err(storage)?;
        let (binding, boundary) = super::super::read_manifest(&catalog.manifest)?;
        let groups = controls(&catalog.records, limits)
            .map_err(storage)?
            .into_iter()
            .map(|(id, group)| Ok((id, group.try_into()?)))
            .collect::<io::Result<_>>()
            .map_err(storage)?;
        Ok(StripedControlImage {
            binding,
            payload_generation: catalog.generation,
            durable_log_bytes: boundary,
            groups,
        })
    }
    fn stage(
        &mut self,
        bytes: &[u8],
        catalog: &mut Catalog,
        lengths: &mut [usize],
        next: &mut usize,
    ) -> io::Result<()> {
        if catalog.records.len() >= self.striped.max_records {
            return Err(invalid("striped record capacity"));
        }
        let mut record = Record::new(bytes, self.limits)?;
        for chunk in bytes.chunks(self.striped.chunk_bytes) {
            record.parts.push(Part {
                lane: *next,
                offset: lengths[*next],
                length: chunk.len(),
                checksum: crc32c(chunk),
            });
            lengths[*next] = lengths[*next]
                .checked_add(chunk.len())
                .ok_or_else(|| invalid("payload offset overflow"))?;
            *next = (*next + 1) % catalog.lanes;
        }
        catalog.records.push(record);
        let ceiling = if control_only(bytes, self.limits)? {
            self.striped.max_catalog_bytes
        } else {
            self.striped.max_catalog_bytes - self.striped.control_reserve_bytes
        };
        if catalog.encoded_len()? > ceiling {
            return Err(invalid("striped catalog capacity"));
        }
        let record = catalog.records.last().unwrap();
        let mut offset = 0;
        for part in &record.parts {
            self.io.append_payload(
                part.lane,
                catalog.generation,
                &bytes[offset..offset + part.length],
            )?;
            offset += part.length;
        }
        Ok(())
    }
    fn sync(&mut self, generation: u64) -> io::Result<()> {
        for lane in 0..self.catalog.lanes {
            self.io.sync_payload(lane, generation)?;
        }
        Ok(())
    }
    fn publish(&mut self, mut next: Catalog, manifest: &[u8]) -> io::Result<()> {
        let (binding, _) = super::super::read_manifest(manifest).map_err(encode_error)?;
        if binding.identity != self.identity {
            return Err(invalid("striped publication identity"));
        }
        next.epoch = self
            .catalog
            .epoch
            .checked_add(1)
            .ok_or_else(|| invalid("catalog epoch exhausted"))?;
        next.manifest = manifest.to_vec();
        next.lengths(self.limits, self.striped)?;
        let bytes = next.encode(self.striped.max_catalog_bytes)?;
        for lane in 0..next.lanes {
            self.io.publish_catalog(lane, &bytes)?;
        }
        self.catalog = next;
        Ok(())
    }
}
impl<I: StripedIo> JournalIo for StripedJournal<I> {
    fn read_manifest(&mut self) -> io::Result<Vec<u8>> {
        if self.catalog.manifest.is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::NotFound,
                "striped manifest absent",
            ));
        }
        Ok(self.catalog.manifest.clone())
    }
    fn read_log(&mut self, limit: usize) -> io::Result<Vec<u8>> {
        let total = self.catalog.records.iter().map(|r| r.length).sum::<usize>();
        if total > limit {
            return Err(invalid("striped read budget"));
        }
        let payloads = (0..self.catalog.lanes)
            .map(|lane| {
                self.io
                    .read_payload(lane, self.catalog.generation, self.limits.max_wal_bytes)
            })
            .collect::<io::Result<Vec<_>>>()?;
        let mut bytes = Vec::with_capacity(total);
        for record in &self.catalog.records {
            let start = bytes.len();
            for part in &record.parts {
                let chunk = payloads[part.lane]
                    .get(part.offset..part.offset + part.length)
                    .ok_or_else(|| invalid("selected striped payload missing"))?;
                if crc32c(chunk) != part.checksum {
                    return Err(invalid("striped chunk checksum"));
                }
                bytes.extend(chunk);
            }
            record.validate(&bytes[start..], self.limits)?;
        }
        Ok(bytes)
    }
    fn append(&mut self, bytes: &[u8]) -> io::Result<()> {
        let mut next = self.catalog.clone();
        let mut lengths = self.lengths.clone();
        let mut lane = self.next_lane;
        self.synchronized = false;
        self.stage(bytes, &mut next, &mut lengths, &mut lane)?;
        self.catalog = next;
        self.lengths = lengths;
        self.next_lane = lane;
        Ok(())
    }
    fn sync_log(&mut self) -> io::Result<()> {
        self.synchronized = false;
        self.sync(self.catalog.generation)?;
        self.synchronized = true;
        Ok(())
    }
    fn truncate_log(&mut self, length: u64) -> io::Result<()> {
        let total = self
            .catalog
            .records
            .iter()
            .map(|r| r.length as u64)
            .sum::<u64>();
        if length != total {
            return Err(invalid("cannot truncate selected striped frames"));
        }
        self.synchronized = false;
        for lane in 0..self.catalog.lanes {
            self.io
                .truncate_payload(lane, self.catalog.generation, self.lengths[lane] as u64)?;
        }
        Ok(())
    }
    fn publish_manifest(&mut self, bytes: &[u8]) -> io::Result<()> {
        if !self.synchronized {
            return Err(invalid("catalog precedes payload sync"));
        }
        self.publish(self.catalog.clone(), bytes)
    }
    fn supports_replacement(&self) -> bool {
        true
    }
    fn replace_log(&mut self, bytes: &[u8], manifest: &[u8]) -> io::Result<()> {
        let generation = self
            .catalog
            .generation
            .checked_add(1)
            .ok_or_else(|| invalid("payload generation exhausted"))?;
        let mut next = Catalog::empty(self.catalog.lanes);
        next.generation = generation;
        let mut lengths = vec![0; next.lanes];
        let mut lane = 0;
        for i in 0..next.lanes {
            self.io.truncate_payload(i, generation, 0)?;
        }
        self.stage(bytes, &mut next, &mut lengths, &mut lane)?;
        self.sync(generation)?;
        self.publish(next, manifest)?;
        self.lengths = lengths;
        self.next_lane = lane;
        self.synchronized = true;
        Ok(())
    }
}
