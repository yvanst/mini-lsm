use std::sync::Arc;

use anyhow::Result;

use super::SsTable;
use crate::{block::BlockIterator, iterators::StorageIterator, key::KeySlice};

/// An iterator over the contents of an SSTable.
pub struct SsTableIterator {
    table: Arc<SsTable>,
    blk_iter: BlockIterator,
    blk_idx: usize,
}

impl SsTableIterator {
    /// Create a new iterator and seek to the first key-value pair in the first data block.
    pub fn create_and_seek_to_first(table: Arc<SsTable>) -> Result<Self> {
        let block = table.read_block_cached(0)?;
        Ok(Self {
            table,
            blk_iter: BlockIterator::create_and_seek_to_first(block),
            blk_idx: 0,
        })
    }

    /// Seek to the first key-value pair in the first data block.
    pub fn seek_to_first(&mut self) -> Result<()> {
        let block = self.table.read_block_cached(0)?;
        self.blk_idx = 0;
        self.blk_iter = BlockIterator::create_and_seek_to_first(block);
        Ok(())
    }

    /// Create a new iterator and seek to the first key-value pair which >= `key`.
    pub fn create_and_seek_to_key(table: Arc<SsTable>, key: KeySlice) -> Result<Self> {
        let mut table_iterator = SsTableIterator::create_and_seek_to_first(table)?;
        table_iterator.seek_to_key(key)?;
        Ok(table_iterator)
    }

    /// Seek to the first key-value pair which >= `key`.
    /// Note: You probably want to review the handout for detailed explanation when implementing
    /// this function.
    // pub fn seek_to_key(&mut self, key: KeySlice) -> Result<()> {
    //     let idx = self
    //         .table
    //         .block_meta
    //         .partition_point(|meta| meta.first_key.as_key_slice() <= key)
    //         .saturating_sub(1);
    //     let block = self.table.read_block_cached(idx)?;
    //     self.blk_iter = BlockIterator::create_and_seek_to_key(block, key);
    //     if self.blk_iter.is_valid() {
    //         self.blk_idx = idx;
    //     } else {
    //         self.blk_idx = idx + 1;
    //         if self.blk_idx < self.table.num_of_blocks() {
    //             let block = self.table.read_block_cached(self.blk_idx)?;
    //             self.blk_iter = BlockIterator::create_and_seek_to_first(block);
    //         }
    //     }
    //     Ok(())
    // }

    fn seek_to_key_inner(table: &Arc<SsTable>, key: KeySlice) -> Result<(usize, BlockIterator)> {
        let mut blk_idx = table.find_block_idx(key);
        let mut blk_iter =
            BlockIterator::create_and_seek_to_key(table.read_block_cached(blk_idx)?, key);
        if !blk_iter.is_valid() {
            blk_idx += 1;
            if blk_idx < table.num_of_blocks() {
                blk_iter =
                    BlockIterator::create_and_seek_to_first(table.read_block_cached(blk_idx)?);
            }
        }
        Ok((blk_idx, blk_iter))
    }
    /// Seek to the first key-value pair which >= `key`.
    pub fn seek_to_key(&mut self, key: KeySlice) -> Result<()> {
        let (blk_idx, blk_iter) = Self::seek_to_key_inner(&self.table, key)?;
        self.blk_iter = blk_iter;
        self.blk_idx = blk_idx;
        Ok(())
    }
}

impl StorageIterator for SsTableIterator {
    type KeyType<'a> = KeySlice<'a>;

    /// Return the `key` that's held by the underlying block iterator.
    fn key(&self) -> KeySlice {
        self.blk_iter.key()
    }

    /// Return the `value` that's held by the underlying block iterator.
    fn value(&self) -> &[u8] {
        self.blk_iter.value()
    }

    /// Return whether the current block iterator is valid or not.
    fn is_valid(&self) -> bool {
        self.blk_iter.is_valid()
    }

    /// Move to the next `key` in the block.
    /// Note: You may want to check if the current block iterator is valid after the move.
    // fn next(&mut self) -> Result<()> {
    //     self.blk_iter.next();
    //     if !self.blk_iter.is_valid() && self.blk_idx < self.table.block_meta.len() {
    //         self.blk_idx += 1;
    //         let block = self.table.read_block_cached(self.blk_idx)?;
    //         self.blk_iter = BlockIterator::create_and_seek_to_first(block);
    //     }
    //     Ok(())
    // }
    fn next(&mut self) -> Result<()> {
        self.blk_iter.next();
        if !self.blk_iter.is_valid() {
            self.blk_idx += 1;
            if self.blk_idx < self.table.num_of_blocks() {
                self.blk_iter = BlockIterator::create_and_seek_to_first(
                    self.table.read_block_cached(self.blk_idx)?,
                );
            }
        }
        Ok(())
    }
}
