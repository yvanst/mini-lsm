use crate::key::{KeySlice, KeyVec};

use super::Block;

/// Builds a block.
pub struct BlockBuilder {
    /// Offsets of each key-value entries.
    offsets: Vec<u16>,
    /// All serialized key-value pairs in the block.
    data: Vec<u8>,
    /// The expected block size.
    block_size: usize,
    /// The first key in the block
    first_key: KeyVec,
}

impl BlockBuilder {
    /// Creates a new block builder.
    pub fn new(block_size: usize) -> Self {
        BlockBuilder {
            offsets: vec![0],
            data: Vec::new(),
            block_size,
            first_key: KeyVec::new(),
        }
    }

    /// Adds a key-value pair to the block. Returns false when the block is full.
    #[must_use]
    pub fn add(&mut self, key: KeySlice, value: &[u8]) -> bool {
        if self.data.is_empty() {
            self.first_key = KeyVec::from_vec(Vec::from(key.raw_ref()));
        } else {
            let add_len = key.len() + value.len();
            if self.data.len() + self.offsets.len() * 2 + add_len >= self.block_size {
                return false;
            }
        }
        let key_len = (key.len() as u16).to_be_bytes();
        let value_len = (value.len() as u16).to_be_bytes();
        let mut entry = Vec::new();
        entry.extend_from_slice(&key_len);
        entry.extend_from_slice(key.raw_ref());
        entry.extend_from_slice(&value_len);
        entry.extend_from_slice(value);
        self.data.extend_from_slice(&entry);

        let loc = self.offsets.last().unwrap() + (entry.len() as u16);
        self.offsets.push(loc);

        true
    }

    /// Check if there is no key-value pair in the block.
    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }

    /// Finalize the block.
    pub fn build(&mut self) -> Block {
        self.offsets.pop();
        self.offsets.push(self.offsets.len() as u16);
        Block {
            data: std::mem::take(&mut self.data),
            offsets: std::mem::take(&mut self.offsets),
        }
    }
}
