    fn validate_proposal<'a>(
        &self,
        operation: OperationId,
        bytes: &[u8],
        pending: impl Iterator<Item = (OperationId, &'a [u8])>,
    ) -> Result<usize, ApplicationError> {
        if bytes.len() != 8 {
            return Err(ApplicationError::InvalidCommand);
        }
        let remaining = self.remaining_operations();
        let mut pending = pending;
        let mut buffered = Vec::new();
        for _ in 0..remaining {
            let Some((id, _)) = pending.next() else {
                // Even if every pending ID and this candidate are novel,
                // their total fits. No distinct-ID tree is needed.
                return Ok(std::mem::size_of::<CounterReceipt>());
            };
            buffered.push(id);
        }
        let mut reserved = BTreeSet::new();
        for id in buffered.into_iter().chain(pending.map(|(id, _)| id)) {
            if !self.dedup.contains_key(&id) {
                reserved.insert(id);
            }
            if reserved.len() > remaining {
                return Err(ApplicationError::DedupCapacity);
            }
        }
        if !self.dedup.contains_key(&operation) {
            reserved.insert(operation);
        }
        if reserved.len() > remaining {
            return Err(ApplicationError::DedupCapacity);
        }
        Ok(std::mem::size_of::<CounterReceipt>())
    }
