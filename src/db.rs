use crate::{FileEntry, FileNanoTime, PathBytes, HASH_SIZE};
use rusqlite::fallible_iterator::{FallibleIterator, IteratorExt};
use rusqlite::{params, Connection, Transaction};
use std::path::Path;

pub struct IndexRow {
    pub entry: FileEntry,
    pub hash: [u8; HASH_SIZE],
}

pub struct ChunkRow {
    pub file_hash: [u8; HASH_SIZE],
    /// A comma-separated hash list string
    pub splits: String,
}

pub struct IndexDb {
    pub db: Connection,
}

impl IndexDb {
    pub fn new(path: impl AsRef<Path>) -> anyhow::Result<Self> {
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

    pub fn select_chunk_all(&self) -> anyhow::Result<Vec<ChunkRow>> {
        let mut stmt = self
            .db
            .prepare_cached("select file_hash, splits from chunk")?;
        let map = stmt.query_map(params![], |r| {
            Ok(ChunkRow {
                file_hash: r.get_unwrap(0),
                splits: r.get_unwrap(1),
            })
        })?;
        Ok(map.into_iter().transpose_into_fallible().collect()?)
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
        let mut stmt = self
            .0
            .prepare_cached("insert into chunk (file_hash, splits) values (?, ?)")?;
        stmt.insert(params![row.file_hash, row.splits.as_str()])?;
        Ok(())
    }
}
