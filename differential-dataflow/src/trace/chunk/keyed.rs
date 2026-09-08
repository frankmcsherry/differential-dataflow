//! Optional, cursor-free materialization by key.

use super::{Chunk, ChunkBatch};

/// A chunk layout that can copy selected keys into compact, resident chunks.
///
/// This capability is independent of [`super::NavigableChunk`]. Implementations
/// can use their own indexes, bulk column copies, or sequential scans.
pub trait KeyedChunk: Chunk {
    /// Owned keys used for requests and cache coverage metadata.
    type Key: Ord + Clone;

    /// Copy exactly the requested keys from a sorted, consolidated chunk chain.
    ///
    /// `keys` must be sorted and distinct. The result must remain sorted and
    /// consolidated, preserve timestamps, and satisfy [`super::is_graded`].
    /// Empty requests return no chunks. Copies must not retain unselected source
    /// data: in particular, a small selection from a paged chunk must not pin its
    /// entire decoded body. Implementations should prune using resident metadata
    /// before fetching, and keep the output resident rather than spilling it.
    fn select_keys(chunks: &[Self], keys: &[Self::Key]) -> Vec<Self>;
}

impl<C: KeyedChunk> ChunkBatch<C> {
    /// Materialize selected keys from this batch payload.
    ///
    /// `keys` must be sorted and distinct. The returned batch describes only
    /// these keys; callers must retain the selection and span description alongside it.
    pub fn select_keys(&self, keys: &[C::Key]) -> Self {
        Self::new(C::select_keys(&self.chunks, keys))
    }
}
