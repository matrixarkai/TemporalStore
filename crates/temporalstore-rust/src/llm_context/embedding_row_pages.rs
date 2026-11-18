// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use std::ops::{Index, IndexMut, Range};

const ROWS_PER_PAGE: usize = 64;

/// Process-local quantized rows. A row stays contiguous without a tenant-sized allocation.
#[derive(Debug, Default)]
pub(super) struct RowPages {
    dimensions: usize,
    len: usize,
    pages: Vec<Vec<i8>>,
}

impl RowPages {
    pub(super) fn reserve_rows(&mut self, additional: usize, dimensions: usize) {
        assert!(dimensions > 0);
        if self.dimensions == 0 {
            self.dimensions = dimensions;
        }
        assert_eq!(self.dimensions, dimensions);
        let required = self
            .len
            .saturating_add(additional.saturating_mul(dimensions));
        let pages = required.div_ceil(self.page_bytes());
        self.pages.reserve(pages.saturating_sub(self.pages.len()));
    }

    fn page_bytes(&self) -> usize {
        self.dimensions * ROWS_PER_PAGE
    }

    pub(super) fn len(&self) -> usize {
        self.len
    }

    #[cfg(test)]
    pub(super) fn capacity(&self) -> usize {
        self.pages.iter().map(Vec::capacity).sum()
    }

    #[cfg(test)]
    pub(super) fn from_flat(dimensions: usize, values: Vec<i8>) -> Self {
        assert_eq!(values.len() % dimensions, 0);
        let mut rows = Self::default();
        rows.reserve_rows(values.len() / dimensions, dimensions);
        rows.extend(values);
        rows
    }

    #[cfg(test)]
    pub(super) fn chunks_exact(&self, dimensions: usize) -> impl Iterator<Item = &[i8]> {
        assert_eq!(dimensions, self.dimensions);
        self.pages
            .iter()
            .flat_map(move |page| page.chunks_exact(dimensions))
    }

    #[cfg(test)]
    pub(super) fn as_ptr(&self) -> *const i8 {
        self.pages
            .first()
            .map_or(std::ptr::null(), |page| page.as_ptr())
    }

    pub(super) fn extend(&mut self, values: impl IntoIterator<Item = i8>) {
        assert!(self.dimensions > 0);
        let page_bytes = self.page_bytes();
        let mut values = values.into_iter().peekable();
        while values.peek().is_some() {
            if self
                .pages
                .last()
                .is_none_or(|page| page.len() == page_bytes)
            {
                self.pages.push(Vec::new());
            }
            let page = self.pages.last_mut().unwrap();
            let before = page.len();
            let available = page_bytes - before;
            let (lower, upper) = values.size_hint();
            let incoming = if upper == Some(lower) {
                lower.min(available)
            } else {
                available
            };
            let required = before + incoming;
            if required > page.capacity() {
                let target = required
                    .max(page.capacity().saturating_mul(2))
                    .min(page_bytes);
                page.reserve_exact(target - before);
            }
            page.extend(values.by_ref().take(page_bytes - before));
            self.len += page.len() - before;
        }
    }

    pub(super) fn extend_from_slice(&mut self, values: &[i8]) {
        self.extend(values.iter().copied());
    }

    pub(super) fn truncate(&mut self, len: usize) {
        if len >= self.len {
            return;
        }
        let page_bytes = self.page_bytes();
        let pages = len.div_ceil(page_bytes);
        self.pages.truncate(pages);
        if let Some(last) = self.pages.last_mut() {
            last.truncate(len - (pages - 1) * page_bytes);
        }
        self.len = len;
    }

    pub(super) fn trim_sparse_capacity(&mut self) {
        if let Some(last) = self.pages.last_mut() {
            if last.capacity() > last.len().saturating_mul(4) {
                last.shrink_to(last.len().saturating_mul(2));
            }
        }
        if self.pages.capacity() > self.pages.len().saturating_mul(4) {
            self.pages.shrink_to(self.pages.len().saturating_mul(2));
        }
    }

    /// Move one row during swap removal; neither source nor destination spans pages.
    pub(super) fn copy_within(&mut self, source: Range<usize>, target: usize) {
        let count = source.end - source.start;
        assert!(source.end <= self.len && target + count <= self.len);
        let (from_page, from) = self.locate(source.start);
        let (to_page, to) = self.locate(target);
        assert!(from + count <= self.pages[from_page].len());
        assert!(to + count <= self.pages[to_page].len());
        if from_page == to_page {
            self.pages[from_page].copy_within(from..from + count, to);
        } else if to_page < from_page {
            let (left, right) = self.pages.split_at_mut(from_page);
            left[to_page][to..to + count].copy_from_slice(&right[0][from..from + count]);
        } else {
            let (left, right) = self.pages.split_at_mut(to_page);
            right[0][to..to + count].copy_from_slice(&left[from_page][from..from + count]);
        }
    }

    fn locate(&self, byte: usize) -> (usize, usize) {
        (byte / self.page_bytes(), byte % self.page_bytes())
    }

    #[cfg(test)]
    pub(super) fn resize(&mut self, len: usize, value: i8) {
        if len < self.len {
            self.truncate(len);
        } else {
            self.extend(std::iter::repeat(value).take(len - self.len));
        }
    }
}

impl Index<Range<usize>> for RowPages {
    type Output = [i8];

    fn index(&self, range: Range<usize>) -> &Self::Output {
        assert!(range.start < range.end && range.end <= self.len);
        let (page, at) = self.locate(range.start);
        &self.pages[page][at..at + range.len()]
    }
}

impl IndexMut<Range<usize>> for RowPages {
    fn index_mut(&mut self, range: Range<usize>) -> &mut Self::Output {
        assert!(range.start < range.end && range.end <= self.len);
        let (page, at) = self.locate(range.start);
        &mut self.pages[page][at..at + range.len()]
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn row_pages_match_contiguous_updates_moves_and_truncation() {
        let mut pages = RowPages::default();
        pages.reserve_rows(400, 5);
        let mut plain = (0..400 * 5)
            .map(|value| (value % 127) as i8)
            .collect::<Vec<_>>();
        pages.extend_from_slice(&plain);
        for (from, to) in [(399, 0), (0, 399), (200, 7), (7, 8), (128, 63)] {
            plain.copy_within(from * 5..(from + 1) * 5, to * 5);
            pages.copy_within(from * 5..(from + 1) * 5, to * 5);
        }
        for row in 0..400 {
            assert_eq!(
                &pages[row * 5..(row + 1) * 5],
                &plain[row * 5..(row + 1) * 5]
            );
        }
        pages[65 * 5..66 * 5].fill(-9);
        plain[65 * 5..66 * 5].fill(-9);
        for rows in [255, 128, 127, 65, 64, 63, 7, 1, 0] {
            pages.truncate(rows * 5);
            plain.truncate(rows * 5);
            pages.trim_sparse_capacity();
            assert_eq!(pages.len(), plain.len());
            assert!(pages.capacity() <= 4 * pages.len());
            for row in 0..rows {
                assert_eq!(
                    &pages[row * 5..(row + 1) * 5],
                    &plain[row * 5..(row + 1) * 5]
                );
            }
        }
    }

    #[test]
    fn regrowing_a_trimmed_tail_does_not_overallocate_a_page() {
        let mut pages = RowPages::default();
        pages.reserve_rows(64, 384);
        pages.resize(64 * 384, 7);
        pages.truncate(15 * 384);
        pages.trim_sparse_capacity();
        for _ in 15..128 {
            pages.extend_from_slice(&[9; 384]);
            assert!(pages
                .pages
                .iter()
                .all(|page| page.capacity() <= pages.page_bytes()));
        }
        assert_eq!(pages.pages.len(), 2);
        assert_eq!(pages.len(), 128 * 384);
    }

    #[test]
    fn growth_and_sparse_retirement_do_not_relocate_surviving_full_pages() {
        let mut pages = RowPages::default();
        pages.reserve_rows(64, 384);
        pages.resize(64 * 384, 7);
        let first = pages.as_ptr();
        pages.reserve_rows(4096, 384);
        pages.resize(4096 * 384, 9);
        assert_eq!(pages.as_ptr(), first);
        for rows in (64..4096).rev() {
            pages.truncate(rows * 384);
            pages.trim_sparse_capacity();
            assert_eq!(pages.as_ptr(), first);
        }
        assert_eq!(pages.pages.len(), 1);
        assert!(pages[0..384].iter().all(|value| *value == 7));
    }
}
