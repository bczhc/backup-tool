#![feature(decl_macro)]

use std::fmt::{Display, Formatter};
use blake3::{Hasher};
use bytesize::ByteSize;
use clap::Parser;
use colored::Colorize;
use fern::colors::{Color, ColoredLevelConfig};
use filetime::FileTime;
use once_cell::sync::Lazy;
use std::fs::File;
use std::io;
use std::io::{BufReader, Read, Seek};
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::sync::Mutex;
use std::time::SystemTime;

pub macro mutex_lock($e:expr) {
    $e.lock().unwrap()
}

pub static ARGS: Lazy<Mutex<CliArgs>> = Lazy::new(|| Mutex::new(Default::default()));

pub static CHUNK_SIZE: Lazy<u64> = Lazy::new(|| {
    ByteSize::from_str(&mutex_lock!(ARGS).chunk_size)
        .expect("Failed to parse size string")
        .0
});

pub static BACKUP_SIZE: Lazy<u64> = Lazy::new(|| {
    ByteSize::from_str(&mutex_lock!(ARGS).backup_size)
        .expect("Failed to parse size string")
        .0
});

#[derive(Parser, Default, Debug, Clone)]
pub struct CliArgs {
    /// Source directory to back up
    pub source_dir: PathBuf,
    /// Output directory location
    #[arg(short, long)]
    pub out_dir: PathBuf,
    /// Path to the reference index database file
    ///
    /// On an initial backup, this is not passed.
    #[arg(short, long)]
    pub ref_index: Option<PathBuf>,
    /// Chunk size for each file. Default to 128MiB
    #[arg(short, long, default_value = "128MiB")]
    pub chunk_size: String,
    /// Size of each backup output file. Default to 3GiB
    #[arg(short = 's', long, default_value = "3GiB")]
    pub backup_size: String,
}

pub fn configure_log() -> anyhow::Result<()> {
    let colors = ColoredLevelConfig::new()
        // use builder methods
        .info(Color::Green);

    fern::Dispatch::new()
        .format(move |out, message, record| {
            out.finish(format_args!(
                "[{} {}] {}",
                format!("{}", humantime::format_rfc3339(SystemTime::now())).yellow(),
                colors.color(record.level()),
                message
            ))
        })
        .level(log::LevelFilter::Debug)
        .chain(io::stderr())
        .apply()?;
    Ok(())
}

pub struct FileEntry {
    pub path: PathBuf,
    pub size: u64,
    pub mtime: FileTime,
    pub hash: Option<Hash>,
}

impl FileEntry {}

pub fn index_files(dir: impl AsRef<Path>) -> io::Result<Vec<FileEntry>> {
    let mut collected = Vec::new();
    let walk = jwalk::WalkDir::new(dir).skip_hidden(false);
    for x in walk {
        let e = x?;
        // only accept regular files
        if !e.file_type.is_file() {
            continue;
        }
        let metadata = e.metadata()?;
        let mtime = FileTime::from_last_modification_time(&metadata);
        let entry = FileEntry {
            path: e.path(),
            size: metadata.len(),
            mtime,
            hash: None,
        };
        collected.push(entry);
    }
    Ok(collected)
}

pub fn compute_file_hash(f: impl AsRef<Path>) -> io::Result<Hash> {
    let reader = BufReader::new(File::open(f.as_ref())?);
    read_to_get_hash(reader, None)
}

pub fn read_to_get_hash(
    mut reader: impl Read,
    size: Option<u64>, /* None to read till the end */
) -> io::Result<Hash> {
    let mut hasher = Hasher::new();
    match size {
        None => {
            io::copy(&mut reader, &mut hasher)?;
        }
        Some(s) => {
            io::copy(&mut reader.take(s), &mut hasher)?;
        }
    }
    Ok(hasher.finalize().into())
}

pub fn file_hash_all_and_chunks(f: impl AsRef<Path>) -> io::Result<(Hash, Option<Vec<Hash>>)> {
    let path = f.as_ref();
    let metadata = path.metadata()?;
    let size = metadata.len();
    if size <= *CHUNK_SIZE {
        // file is not chunked
        return Ok((compute_file_hash(path)?, None));
    }
    let reader = BufReader::new(File::open(path)?);
    let mut reader_wrapper = HashReadWrapper::new(reader);
    let mut chunks_hash = Vec::new();
    let n = size / *CHUNK_SIZE;
    let r = size % *CHUNK_SIZE;
    // read n chunks
    for _ in 0..n {
        let hash = read_to_get_hash(&mut reader_wrapper, Some(*CHUNK_SIZE))?;
        chunks_hash.push(hash);
    }
    // ... and the probable remaining
    if r != 0 {
        let hash = read_to_get_hash(&mut reader_wrapper, Some(r))?;
        chunks_hash.push(hash);
    }
    debug_assert_eq!(reader_wrapper.inner.stream_position()?, size);
    Ok((reader_wrapper.finalize(), Some(chunks_hash)))
}

pub struct HashReadWrapper<R: Read> {
    inner: R,
    hasher: Hasher,
}

impl<R: Read> HashReadWrapper<R> {
    pub fn new(reader: R) -> Self {
        Self {
            inner: reader,
            hasher: Default::default(),
        }
    }

    pub fn finalize(&self) -> Hash {
        self.hasher.finalize().into()
    }
}

impl<R: Read> Read for HashReadWrapper<R> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let size = self.inner.read(buf)?;
        self.hasher.update(&buf[..size]);
        Ok(size)
    }
}

pub fn chunks_ranges(file_size: u64) -> Vec<Range> {
    let mut ranges = Vec::new();
    let chunk_size = *CHUNK_SIZE;
    let n = file_size / chunk_size;
    let r = file_size % chunk_size;
    for i in 0..n {
        ranges.push(Range {
            start: chunk_size * i,
            size: chunk_size,
        });
    }
    if r != 0 {
        ranges.push(Range {
            start: chunk_size * n,
            size: r,
        });
    }
    ranges
}

pub struct Range {
    pub start: u64,
    pub size: u64,
}

/// Half of a 32-byte hash is enough.
const HASH_SIZE: usize = 16;

#[derive(Default, Copy, Clone)]
pub struct Hash([u8; HASH_SIZE]);

impl From<blake3::Hash> for Hash {
    fn from(value: blake3::Hash) -> Self {
        let mut half = Hash::default();
        half.0.copy_from_slice(&value.as_bytes()[..HASH_SIZE]);
        half
    }
}

impl Display for Hash {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str(&hex::encode(self.0))
    }
}
