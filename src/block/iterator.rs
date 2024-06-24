use std::sync::Arc;

use crate::key::{KeySlice, KeyVec};

use super::Block;

/// Iterates on a block.
pub struct BlockIterator {
    /// The internal `Block`, wrapped by an `Arc`
    block: Arc<Block>,
    /// The current key, empty represents the iterator is invalid
    key: KeyVec,
    /// the current value range in the block.data, corresponds to the current key
    value_range: (usize, usize),
    /// Current index of the key-value pair, should be in range of [0, num_of_elements)
    idx: usize,
    /// The first key in the block
    first_key: KeyVec,
}

impl BlockIterator {
    fn new(block: Arc<Block>) -> Self {
        Self {
            block,
            key: KeyVec::new(),
            value_range: (0, 0),
            idx: 0,
            first_key: KeyVec::new(),
        }
    }

    /// Creates a block iterator and seek to the first entry.
    pub fn create_and_seek_to_first(block: Arc<Block>) -> Self {
        let mut block_iterator = BlockIterator::new(block);
        block_iterator.seek_to_first();
        block_iterator
    }

    /// Creates a block iterator and seek to the first key that >= `key`.
    pub fn create_and_seek_to_key(block: Arc<Block>, key: KeySlice) -> Self {
        let mut block_iterator = BlockIterator::create_and_seek_to_first(block);
        block_iterator.seek_to_key(key);
        block_iterator
    }

    /// Returns the key of the current entry.
    pub fn key(&self) -> KeySlice {
        self.key.as_key_slice()
    }

    /// Returns the value of the current entry.
    pub fn value(&self) -> &[u8] {
        &self.block.data[self.value_range.0..self.value_range.1]
    }

    /// Returns true if the iterator is valid.
    /// Note: You may want to make use of `key`
    pub fn is_valid(&self) -> bool {
        !self.key.is_empty()
    }

    /// Seeks to the first key in the block.
    pub fn seek_to_first(&mut self) {
        let block = self.block.clone();
        let key_len = u16::from_be_bytes([block.data[0], block.data[1]]) as usize;
        let key = KeyVec::from_vec(Vec::from(&block.data[2..2 + key_len]));
        let value_len =
            u16::from_be_bytes([block.data[2 + key_len], block.data[2 + key_len + 1]]) as usize;
        self.key = key.clone();
        self.value_range = (2 + key_len + 2, 2 + key_len + 2 + value_len);
        self.idx = 1;
        self.first_key = key;
    }

    /// Move to the next key in the block.
    pub fn next(&mut self) {
        if self.idx == self.block.offsets.len() - 1 {
            self.key = KeyVec::new();
            return;
        }
        let block = self.block.clone();
        let offset = block.offsets[self.idx] as usize;
        let key_len = u16::from_be_bytes([block.data[offset], block.data[offset + 1]]) as usize;
        let key = KeySlice::from_slice(&block.data[(offset + 2)..(offset + 2 + key_len)]);
        let value_len = u16::from_be_bytes([
            block.data[offset + 2 + key_len],
            block.data[offset + 2 + key_len + 1],
        ]) as usize;

        self.key.set_from_slice(key);
        self.value_range = (
            offset + 2 + key_len + 2,
            offset + 2 + key_len + 2 + value_len,
        );
        self.idx += 1;
    }

    /// Seek to the first key that >= `key`.
    /// Note: You should assume the key-value pairs in the block are sorted when being added by
    /// callers.
    pub fn seek_to_key(&mut self, key: KeySlice) {
        let block = self.block.clone();
        let mut final_idx = 0;
        let mut final_key = KeyVec::new();
        let mut final_key_len = 0;
        let mut final_offset = 0;
        for (i, offset) in block.offsets.iter().enumerate() {
            if i == block.offsets.len() - 1 {
                // invalid the iter
                self.key = KeyVec::new();
                return;
            }
            let offset = *offset as usize;
            let key_len = u16::from_be_bytes([block.data[offset], block.data[offset + 1]]) as usize;
            let iter_key = KeySlice::from_slice(&block.data[(offset + 2)..(offset + 2 + key_len)]);
            if iter_key >= key {
                final_key.set_from_slice(iter_key);
                final_idx = i;
                final_key_len = key_len;
                final_offset = offset;
                break;
            }
        }
        let final_value_len = u16::from_be_bytes([
            block.data[final_offset + 2 + final_key_len],
            block.data[final_offset + 2 + final_key_len + 1],
        ]) as usize;

        self.key = final_key;
        self.value_range = (
            final_offset + 2 + final_key_len + 2,
            final_offset + 2 + final_key_len + 2 + final_value_len,
        );
        self.idx = final_idx;
    }
}
