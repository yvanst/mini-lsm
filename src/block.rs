mod builder;
mod iterator;

pub use builder::BlockBuilder;
use bytes::Bytes;
pub use iterator::BlockIterator;

/// A block is the smallest unit of read and caching in LSM tree. It is a collection of sorted key-value pairs.
pub struct Block {
    pub(crate) data: Vec<u8>,
    pub(crate) offsets: Vec<u16>,
}

impl Block {
    /// Encode the internal data to the data layout illustrated in the tutorial
    /// Note: You may want to recheck if any of the expected field is missing from your output
    pub fn encode(&self) -> Bytes {
        let offsets_bytes = self
            .offsets
            .iter()
            .flat_map(|o| o.to_be_bytes())
            .collect::<Vec<_>>();
        Bytes::from_iter([self.data.clone(), offsets_bytes].concat())
    }

    /// Decode from the data layout, transform the input `data` to a single `Block`
    pub fn decode(data: &[u8]) -> Self {
        let mut boundary = 0;
        let mut offset_bytes = vec![];
        for i in (0..data.len()).rev() {
            if i % 2 == 1 {
                continue;
            }
            let offset = u16::from_be_bytes([data[i], data[i + 1]]);
            offset_bytes.push(offset);
            if offset == 0 {
                boundary = i;
                break;
            }
        }
        offset_bytes.reverse();
        Block {
            data: Vec::from(&data[0..boundary]),
            offsets: offset_bytes,
        }
    }
}
