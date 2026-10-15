    fn validate_proposal<'a>(
        &self,
        operation: OperationId,
        bytes: &[u8],
        pending: impl Iterator<Item = (OperationId, &'a [u8])>,
    ) -> Result<usize, ApplicationError> {
        if bytes.len() != 8 {
            return Err(ApplicationError::InvalidCommand);
        }
        let mut reserved = BTreeSet::new();
        for (id, _) in pending {
            if !self.dedup.contains_key(&id) {
                reserved.insert(id);
            }
            if reserved.len() > self.remaining_operations() {
                return Err(ApplicationError::DedupCapacity);
            }
        }
        if !self.dedup.contains_key(&operation) {
            reserved.insert(operation);
        }
        if reserved.len() > self.remaining_operations() {
            return Err(ApplicationError::DedupCapacity);
        }
        Ok(std::mem::size_of::<CounterReceipt>())
    }