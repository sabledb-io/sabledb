use bytes::BytesMut;

#[derive(Default, Debug, Clone)]
pub struct GetChangesLimits {
    /// If provided, limit memory by bytes
    memory: Option<u64>,
    /// Limit changes by count. i.e. no more than `changes_count` changes
    changes_count: Option<u64>,
    /// Collect only keys that starts with `prefix`
    prefix: Option<BytesMut>,
}

impl GetChangesLimits {
    pub fn builder() -> GetChangesLimitsBuilder {
        GetChangesLimitsBuilder::default()
    }

    pub fn memory_limit(&self) -> &Option<u64> {
        &self.memory
    }

    pub fn changes_count_limit(&self) -> &Option<u64> {
        &self.changes_count
    }

    pub fn prefix_limit(&self) -> &Option<BytesMut> {
        &self.prefix
    }
}

#[derive(Default)]
pub struct GetChangesLimitsBuilder {
    /// If provided, limit memory by bytes
    memory: Option<u64>,
    /// Limit changes by count. i.e. no more than `changes_count` changes
    changes_count: Option<u64>,
    /// Collect only keys that starts with `prefix`
    prefix: Option<bytes::BytesMut>,
}

impl GetChangesLimitsBuilder {
    pub fn with_memory(mut self, memory: u64) -> Self {
        self.memory = Some(memory);
        self
    }

    pub fn with_max_changes_count(mut self, changes_count: u64) -> Self {
        self.changes_count = Some(changes_count);
        self
    }

    pub fn with_prefix(mut self, prefix: BytesMut) -> Self {
        self.prefix = Some(prefix);
        self
    }

    pub fn build(self) -> GetChangesLimits {
        GetChangesLimits {
            memory: self.memory,
            changes_count: self.changes_count,
            prefix: self.prefix,
        }
    }
}
