use crate::{FileEntry, FileNanoTime, PathBytes, SplitInfo, HASH_SIZE};
use rusqlite::fallible_iterator::{FallibleIterator, IteratorExt};
use rusqlite::{params, Connection, Transaction};
use std::fs;
use std::path::Path;

#[derive(Debug)]
pub struct IndexRow {
    pub entry: FileEntry,
    pub hash: [u8; HASH_SIZE],
}

pub struct ChunkRow {
    pub file_hash: [u8; HASH_SIZE],
    pub chunk_hash: [u8; HASH_SIZE],
    pub bak_n: i32,
    /// Offset of this chunk in the 'bak' file
    pub offset: u64,
    pub size: u64,
}

pub struct IndexDb {
    pub db: Connection,
}

impl IndexDb {
    pub fn new(path: impl AsRef<Path>, delete_old: bool) -> anyhow::Result<Self> {
        if delete_old && path.as_ref().exists() {
            fs::remove_file(path.as_ref())?;
        }
        let db = Connection::open(path)?;
        let sql = include_str!("../index.sql");
        db.execute_batch(sql)?;
        Ok(Self { db })
    }

    pub fn transaction(&mut self) -> anyhow::Result<IndexDbTx> {
        Ok(IndexDbTx(self.db.transaction()?))
    }

    pub fn select_index_all(&self) -> anyhow::Result<Vec<IndexRow>> {
        let mut stmt = self
            .db
            .prepare_cached("select path, size, mtime, hash from `index`")?;
        let map = stmt.query_map(params![], |r| {
            Ok(IndexRow {
                entry: FileEntry {
                    path: PathBytes(r.get_unwrap(0)).into_path_buf(),
                    size: r.get_unwrap(1),
                    mtime: FileNanoTime(r.get_unwrap(2)),
                },
                hash: r.get_unwrap(3),
            })
        })?;
        Ok(map.into_iter().transpose_into_fallible().collect()?)
    }

    pub fn query_index_row_count(&self) -> anyhow::Result<u64> {
        Ok(self
            .db
            .query_row("select count(*) from `index`", params![], |r| {
                Ok(r.get_unwrap::<_, u64>(0))
            })?)
    }
    pub fn query_chunk_row_count(&self) -> anyhow::Result<u64> {
        Ok(self
            .db
            .query_row("select count(*) from chunk", params![], |r| {
                Ok(r.get_unwrap::<_, u64>(0))
            })?)
    }
}

pub struct IndexDbTx<'a>(pub Transaction<'a>);

impl<'a> IndexDbTx<'a> {
    pub fn insert_index_row(&self, row: &IndexRow) -> anyhow::Result<()> {
        let mut stmt = self
            .0
            .prepare_cached("insert into `index` (path, size, mtime, hash) values (?, ?, ?, ?)")?;
        stmt.insert(params![
            &*PathBytes::from(&row.entry.path),
            row.entry.size,
            *row.entry.mtime,
            row.hash
        ])?;
        Ok(())
    }

    pub fn insert_chunk_row(&self, row: &ChunkRow) -> anyhow::Result<()> {
        let mut stmt = self.0.prepare_cached(
            "insert into chunk (file_hash, chunk_hash, bak_n, offset, size) values (?, ?, ?, ?, ?)",
        )?;
        stmt.insert(params![
            row.file_hash,
            row.chunk_hash,
            row.bak_n,
            row.offset,
            row.size
        ])?;
        Ok(())
    }

    pub fn insert_file_split_info(&self, splits: &[SplitInfo]) -> anyhow::Result<()> {
        for x in splits {
            let file_hash = x.file_hash;
            for x in &x.chunks {
                self.insert_chunk_row(&ChunkRow {
                    file_hash: *file_hash,
                    bak_n: x.bak_n,
                    chunk_hash: *x.hash,
                    offset: x.offset,
                    size: x.size,
                })?;
            }
        }
        Ok(())
    }
}
