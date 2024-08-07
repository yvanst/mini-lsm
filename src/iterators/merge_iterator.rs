use super::StorageIterator;
use crate::key::KeySlice;
use anyhow::Result;
use std::cmp;
use std::collections::binary_heap::PeekMut;
use std::collections::BinaryHeap;

struct HeapWrapper<I: StorageIterator>(pub usize, pub Box<I>);

impl<I: StorageIterator> PartialEq for HeapWrapper<I> {
    fn eq(&self, other: &Self) -> bool {
        self.partial_cmp(other).unwrap() == cmp::Ordering::Equal
    }
}

impl<I: StorageIterator> Eq for HeapWrapper<I> {}

impl<I: StorageIterator> PartialOrd for HeapWrapper<I> {
    #[allow(clippy::non_canonical_partial_ord_impl)]
    fn partial_cmp(&self, other: &Self) -> Option<cmp::Ordering> {
        match self.1.key().cmp(&other.1.key()) {
            cmp::Ordering::Greater => Some(cmp::Ordering::Greater),
            cmp::Ordering::Less => Some(cmp::Ordering::Less),
            cmp::Ordering::Equal => self.0.partial_cmp(&other.0),
        }
        .map(|x| x.reverse())
    }
}

impl<I: StorageIterator> Ord for HeapWrapper<I> {
    fn cmp(&self, other: &Self) -> cmp::Ordering {
        self.partial_cmp(other).unwrap()
    }
}

/// Merge multiple iterators of the same type. If the same key occurs multiple times in some
/// iterators, prefer the one with smaller index.
pub struct MergeIterator<I: StorageIterator> {
    iters: BinaryHeap<HeapWrapper<I>>,
    current: Option<HeapWrapper<I>>,
}

impl<I: StorageIterator> MergeIterator<I> {
    pub fn create(iters: Vec<Box<I>>) -> Self {
        let mut binary_heap = BinaryHeap::new();
        for (id, iter) in iters.into_iter().enumerate() {
            if iter.is_valid() {
                binary_heap.push(HeapWrapper(id, iter))
            }
        }
        let current = binary_heap.pop();
        MergeIterator {
            iters: binary_heap,
            current,
        }
    }
}

impl<I: 'static + for<'a> StorageIterator<KeyType<'a> = KeySlice<'a>>> StorageIterator
    for MergeIterator<I>
{
    type KeyType<'a> = KeySlice<'a>;

    fn key(&self) -> KeySlice {
        match &self.current {
            Some(cur) => cur.1.key(),
            None => KeySlice::from_slice([].as_ref()),
        }
    }

    fn value(&self) -> &[u8] {
        match &self.current {
            Some(cur) => cur.1.value(),
            None => [].as_ref(),
        }
    }

    fn is_valid(&self) -> bool {
        self.current
            .as_ref()
            .map(|x| x.1.is_valid())
            .unwrap_or(false)
    }

    fn next(&mut self) -> Result<()> {
        //     match &mut self.current {
        //         Some(current) => {
        //             while !self.iters.is_empty() {
        //                 let mut heap_min = self.iters.pop().unwrap();
        //                 if heap_min.1.key() > current.1.key() {
        //                     self.iters.push(heap_min);
        //                     break;
        //                 } else if heap_min.1.is_valid() {
        //                     let _ = heap_min.borrow_mut().1.next();
        //                     dbg!(heap_min.1.key(), heap_min.1.value());
        //                     self.iters.push(heap_min);
        //                 }
        //             }
        //         }
        //         None => (),
        //     }
        //     if self.current.is_some() {
        //         let mut current = self.current.take().unwrap();
        //         if current.1.is_valid() {
        //             let _ = current.1.next();
        //             self.iters.push(current);
        //         }
        //     }
        //     self.current = self.iters.pop();
        //     Ok(())
        // }
        let current = self.current.as_mut().unwrap();
        // Pop the item out of the heap if they have the same value.
        while let Some(mut inner_iter) = self.iters.peek_mut() {
            debug_assert!(
                inner_iter.1.key() >= current.1.key(),
                "heap invariant violated"
            );
            if inner_iter.1.key() == current.1.key() {
                // Case 1: an error occurred when calling `next`.
                if let e @ Err(_) = inner_iter.1.next() {
                    PeekMut::pop(inner_iter);
                    return e;
                }

                // Case 2: iter is no longer valid.
                if !inner_iter.1.is_valid() {
                    PeekMut::pop(inner_iter);
                }
            } else {
                break;
            }
        }

        current.1.next()?;

        // If the current iterator is invalid, pop it out of the heap and select the next one.
        if !current.1.is_valid() {
            if let Some(iter) = self.iters.pop() {
                *current = iter;
            }
            return Ok(());
        }

        // Otherwise, compare with heap top and swap if necessary.
        if let Some(mut inner_iter) = self.iters.peek_mut() {
            if *current < *inner_iter {
                std::mem::swap(&mut *inner_iter, current);
            }
        }

        Ok(())
    }

    fn num_active_iterators(&self) -> usize {
        self.iters
            .iter()
            .map(|x| x.1.num_active_iterators())
            .sum::<usize>()
            + self
                .current
                .as_ref()
                .map(|x| x.1.num_active_iterators())
                .unwrap_or(0)
    }
}
