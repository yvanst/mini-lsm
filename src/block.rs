mod builder;
mod iterator;

pub use builder::BlockBuilder;
use bytes::{Buf, BufMut, Bytes};
pub use iterator::BlockIterator;

/// A block is the smallest unit of read and caching in LSM tree. It is a collection of sorted key-value pairs.
pub struct Block {
    pub(crate) data: Vec<u8>,
    pub(crate) offsets: Vec<u16>,
}

// impl Block {
//     /// Encode the internal data to the data layout illustrated in the tutorial
//     /// Note: You may want to recheck if any of the expected field is missing from your output
//     pub fn encode(&self) -> Bytes {
//         let offsets_bytes = self
//             .offsets
//             .iter()
//             .flat_map(|o| o.to_be_bytes())
//             .collect::<Vec<_>>();
//         Bytes::from_iter([self.data.clone(), offsets_bytes].concat())
//     }

//     /// Decode from the data layout, transform the input `data` to a single `Block`
//     pub fn decode(data: &[u8]) -> Self {
//         let mut boundary = 0;
//         let mut offset_bytes = vec![];
//         for i in (0..data.len()).rev() {
//             if i % 2 == 1 {
//                 continue;
//             }
//             let offset = u16::from_be_bytes([data[i], data[i + 1]]);
//             offset_bytes.push(offset);
//             if offset == 0 {
//                 boundary = i;
//                 break;
//             }
//         }
//         offset_bytes.reverse();
//         Block {
//             data: Vec::from(&data[0..boundary]),
//             offsets: offset_bytes,
//         }
//     }
// }

pub(crate) const SIZEOF_U16: usize = std::mem::size_of::<u16>();

impl Block {
    pub fn encode(&self) -> Bytes {
        let mut buf = self.data.clone();
        let offsets_len = self.offsets.len();
        for offset in &self.offsets {
            buf.put_u16(*offset);
        }
        // Adds number of elements at the end of the block
        buf.put_u16(offsets_len as u16);
        buf.into()
    }

    pub fn decode(data: &[u8]) -> Self {
        // get number of elements in the block
        let entry_offsets_len = (&data[data.len() - SIZEOF_U16..]).get_u16() as usize;
        let data_end = data.len() - SIZEOF_U16 - entry_offsets_len * SIZEOF_U16;
        let offsets_raw = &data[data_end..data.len() - SIZEOF_U16];
        // get offset array
        let offsets = offsets_raw
            .chunks(SIZEOF_U16)
            .map(|mut x| x.get_u16())
            .collect();
        // retrieve data
        let data = data[0..data_end].to_vec();
        Self { data, offsets }
    }
}
