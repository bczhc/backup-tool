use crate::{PathBytes, HASH_SIZE};
use rusqlite::{params, Connection, Transaction};
use std::path::Path;

pub struct IndexRow {
    /// Use raw u8 array to support unix paths.
    pub path: PathBytes,
    pub size: u64,
    pub mtime: u64,
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
}

pub struct IndexDbTx<'a>(pub Transaction<'a>);

impl<'a> IndexDbTx<'a> {
    pub fn insert_index_row(&self, row: &IndexRow) -> anyhow::Result<()> {
        let mut stmt = self
            .0
            .prepare_cached("insert into `index` (path, size, mtime, hash) values (?, ?, ?, ?)")?;
        stmt.insert(params![
            row.path.0.as_slice(),
            row.size,
            row.mtime,
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
