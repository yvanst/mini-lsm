use std::path::Path;
use std::sync::Arc;

use anyhow::Result;
use bytes::Bytes;

use super::{BlockMeta, FileObject, SsTable};
use crate::{
    block::BlockBuilder,
    key::{KeyBytes, KeySlice},
    lsm_storage::BlockCache,
};

/// Builds an SSTable from key-value pairs.
pub struct SsTableBuilder {
    builder: BlockBuilder,
    first_key: Vec<u8>,
    last_key: Vec<u8>,
    data: Vec<u8>,
    pub(crate) meta: Vec<BlockMeta>,
    block_size: usize,
}

impl SsTableBuilder {
    /// Create a builder based on target block size.
    pub fn new(block_size: usize) -> Self {
        let builder = BlockBuilder::new(block_size);
        SsTableBuilder {
            builder,
            first_key: Vec::new(),
            last_key: Vec::new(),
            data: Vec::new(),
            meta: Vec::new(),
            block_size,
        }
    }

    /// Adds a key-value pair to SSTable.
    ///
    /// Note: You should split a new block when the current block is full.(`std::mem::replace` may
    /// be helpful here)
    pub fn add(&mut self, key: KeySlice, value: &[u8]) {
        let not_full = self.builder.add(key, value);
        if !not_full {
            let block_meta = BlockMeta {
                offset: self.data.len(),
                first_key: KeyBytes::from_bytes(Bytes::from(self.builder.first_key())),
                last_key: KeyBytes::from_bytes(Bytes::from(self.builder.last_key())),
            };
            self.meta.push(block_meta);
            let block = self.builder.build();
            self.data.extend(block.encode());
            let _ = std::mem::replace(&mut self.builder, BlockBuilder::new(self.block_size));
            let _ = self.builder.add(key, value);
        }
        if self.first_key.is_empty() || self.first_key > self.builder.first_key() {
            self.first_key = self.builder.first_key();
        }
        if self.last_key.is_empty() || self.last_key < self.builder.last_key() {
            self.last_key = self.builder.last_key();
        }
    }

    /// Get the estimated size of the SSTable.
    ///
    /// Since the data blocks contain much more data than meta blocks, just return the size of data
    /// blocks here.
    pub fn estimated_size(&self) -> usize {
        self.data.len()
    }

    /// Builds the SSTable and writes it to the given path. Use the `FileObject` structure to manipulate the disk objects.
    pub fn build(
        mut self,
        id: usize,
        block_cache: Option<Arc<BlockCache>>,
        path: impl AsRef<Path>,
    ) -> Result<SsTable> {
        let block_meta = BlockMeta {
            offset: self.data.len(),
            first_key: KeyBytes::from_bytes(Bytes::from(self.builder.first_key())),
            last_key: KeyBytes::from_bytes(Bytes::from(self.builder.last_key())),
        };
        self.meta.push(block_meta);
        let block = self.builder.build();
        self.data.extend(block.encode());
        let _ = std::mem::replace(&mut self.builder, BlockBuilder::new(self.block_size));

        let extra = self.data.len();
        let mut data = self.data;
        BlockMeta::encode_block_meta(&self.meta, &mut data);
        data.extend((extra as u32).to_be_bytes());

        let file_object = FileObject::create(path.as_ref(), data)?;
        Ok(SsTable {
            file: file_object,
            block_meta: self.meta,
            block_meta_offset: extra,
            id,
            block_cache,
            first_key: KeyBytes::from_bytes(Bytes::copy_from_slice(&self.first_key)),
            last_key: KeyBytes::from_bytes(Bytes::copy_from_slice(&self.last_key)),
            bloom: None,
            max_ts: 0,
        })
    }

    #[cfg(test)]
    pub(crate) fn build_for_test(self, path: impl AsRef<Path>) -> Result<SsTable> {
        self.build(0, None, path)
    }
}
