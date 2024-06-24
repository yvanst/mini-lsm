pub(crate) mod bloom;
mod builder;
mod iterator;
use self::bloom::Bloom;
use crate::block::Block;
use crate::key::{Key, KeyBytes, KeySlice};
use crate::lsm_storage::BlockCache;
use anyhow::bail;
use anyhow::Result;
pub use builder::SsTableBuilder;
use bytes::Buf;
use bytes::Bytes;
pub use iterator::SsTableIterator;
use std::fs::File;
use std::path::Path;
use std::sync::Arc;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BlockMeta {
    /// Offset of this data block.
    pub offset: usize,
    /// The first key of the data block.
    pub first_key: KeyBytes,
    /// The last key of the data block.
    pub last_key: KeyBytes,
}

impl BlockMeta {
    /// Encode block meta to a buffer.
    /// You may add extra fields to the buffer,
    /// in order to help keep track of `first_key` when decoding from the same buffer in the future.
    pub fn encode_block_meta(block_meta: &[BlockMeta], buf: &mut Vec<u8>) {
        let mut count = 0;
        for meta_data in block_meta {
            let mut seg = Vec::new();
            seg.extend((meta_data.offset as u32).to_be_bytes());

            let first_key_len = meta_data.first_key.len() as u16;
            seg.extend(first_key_len.to_be_bytes());
            seg.extend(meta_data.first_key.raw_ref());

            let last_key_len = meta_data.last_key.len() as u16;
            seg.extend(last_key_len.to_be_bytes());
            seg.extend(meta_data.last_key.raw_ref());

            count += seg.len();
            buf.extend(seg);
        }
    }

    /// Decode block meta from a buffer.
    pub fn decode_block_meta(buf: &mut impl Buf) -> Vec<BlockMeta> {
        let mut block_meta = Vec::new();
        while buf.remaining() > 0 {
            let offset = buf.get_u32();

            let first_key_len = buf.get_u16();
            let mut first_key = Vec::new();
            for _ in 0..first_key_len {
                first_key.push(buf.get_u8());
            }

            let last_key_len = buf.get_u16();
            let mut last_key = Vec::new();
            for _ in 0..last_key_len {
                last_key.push(buf.get_u8());
            }

            let meta = BlockMeta {
                offset: offset as usize,
                first_key: Key::from_bytes(Bytes::from_iter(first_key)),
                last_key: Key::from_bytes(Bytes::from_iter(last_key)),
            };
            block_meta.push(meta);
        }
        block_meta
    }
}

/// A file object.
pub struct FileObject(Option<File>, u64);

impl FileObject {
    pub fn read(&self, offset: u64, len: u64) -> Result<Vec<u8>> {
        use std::os::unix::fs::FileExt;
        let mut data = vec![0; len as usize];
        self.0
            .as_ref()
            .unwrap()
            .read_exact_at(&mut data[..], offset)?;
        Ok(data)
    }

    pub fn size(&self) -> u64 {
        self.1
    }

    /// Create a new file object (day 2) and write the file to the disk (day 4).
    pub fn create(path: &Path, data: Vec<u8>) -> Result<Self> {
        std::fs::write(path, &data)?;
        File::open(path)?.sync_all()?;
        Ok(FileObject(
            Some(File::options().read(true).write(false).open(path)?),
            data.len() as u64,
        ))
    }

    pub fn open(path: &Path) -> Result<Self> {
        let file = File::options().read(true).write(false).open(path)?;
        let size = file.metadata()?.len();
        Ok(FileObject(Some(file), size))
    }
}

/// An SSTable.
pub struct SsTable {
    /// The actual storage unit of SsTable, the format is as above.
    pub(crate) file: FileObject,
    /// The meta blocks that hold info for data blocks.
    pub(crate) block_meta: Vec<BlockMeta>,
    /// The offset that indicates the start point of meta blocks in `file`.
    pub(crate) block_meta_offset: usize,
    id: usize,
    block_cache: Option<Arc<BlockCache>>,
    first_key: KeyBytes,
    last_key: KeyBytes,
    pub(crate) bloom: Option<Bloom>,
    /// The maximum timestamp stored in this SST, implemented in week 3.
    max_ts: u64,
}

impl SsTable {
    #[cfg(test)]
    pub(crate) fn open_for_test(file: FileObject) -> Result<Self> {
        Self::open(0, None, file)
    }

    /// Open SSTable from a file.
    pub fn open(id: usize, block_cache: Option<Arc<BlockCache>>, file: FileObject) -> Result<Self> {
        let offset_size = std::mem::size_of::<u32>() as u64;
        let block_meta = file.read(0, file.size() - offset_size)?;

        let block_meta_offset = file.read(file.size() - offset_size, offset_size)?;
        let block_meta_offset = block_meta_offset[..].try_into()?;
        let block_meta_offset = u32::from_be_bytes(block_meta_offset) as usize;

        let mut buf = &(block_meta[block_meta_offset..]);
        let block_meta = BlockMeta::decode_block_meta(&mut buf);
        let first_key = block_meta
            .iter()
            .map(|meta| &meta.first_key)
            .min()
            .unwrap()
            .to_owned();
        let last_key = block_meta
            .iter()
            .map(|meta| &meta.last_key)
            .max()
            .unwrap()
            .to_owned();

        Ok(Self {
            file,
            block_meta,
            block_meta_offset,
            id,
            block_cache,
            first_key,
            last_key,
            bloom: None,
            max_ts: 0,
        })
    }

    /// Create a mock SST with only first key + last key metadata
    pub fn create_meta_only(
        id: usize,
        file_size: u64,
        first_key: KeyBytes,
        last_key: KeyBytes,
    ) -> Self {
        Self {
            file: FileObject(None, file_size),
            block_meta: vec![],
            block_meta_offset: 0,
            id,
            block_cache: None,
            first_key,
            last_key,
            bloom: None,
            max_ts: 0,
        }
    }

    /// Read a block from the disk.
    pub fn read_block(&self, block_idx: usize) -> Result<Arc<Block>> {
        if block_idx >= self.block_meta.len() {
            bail!("no such block id in SST")
        }
        let left = self.block_meta[block_idx].offset;
        let right = if block_idx == self.block_meta.len() - 1 {
            self.block_meta_offset
        } else {
            self.block_meta[block_idx + 1].offset
        };
        let block = self.file.read(left as u64, (right - left) as u64)?;
        let block_decode = Block::decode(&block);

        Ok(Arc::new(block_decode))
    }

    /// Read a block from disk, with block cache. (Day 4)
    pub fn read_block_cached(&self, block_idx: usize) -> Result<Arc<Block>> {
        if let Some(block_cache) = self.block_cache.as_ref() {
            let key = (self.id, block_idx);
            let res = block_cache.try_get_with(key, || self.read_block(block_idx));
            match res {
                Ok(block) => Ok(block),
                Err(_) => bail!("read block error"),
            }
        } else {
            self.read_block(block_idx)
        }
    }

    /// Find the block that may contain `key`.
    /// Note: You may want to make use of the `first_key` stored in `BlockMeta`.
    /// You may also assume the key-value pairs stored in each consecutive block are sorted.
    pub fn find_block_idx(&self, key: KeySlice) -> usize {
        for (idx, block_meta) in self.block_meta.iter().enumerate() {
            if block_meta.last_key.as_key_slice() >= key {
                return idx;
            }
        }
        self.block_meta.len() - 1
    }

    /// Get number of data blocks.
    pub fn num_of_blocks(&self) -> usize {
        self.block_meta.len()
    }

    pub fn first_key(&self) -> &KeyBytes {
        &self.first_key
    }

    pub fn last_key(&self) -> &KeyBytes {
        &self.last_key
    }

    pub fn table_size(&self) -> u64 {
        self.file.1
    }

    pub fn sst_id(&self) -> usize {
        self.id
    }

    pub fn max_ts(&self) -> u64 {
        self.max_ts
    }
}
