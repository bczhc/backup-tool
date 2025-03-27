#![feature(decl_macro)]

use blake3::Hasher;
use bytesize::ByteSize;
use cfg_if::cfg_if;
use clap::Parser;
use colored::Colorize;
use fern::colors::{Color, ColoredLevelConfig};
use filetime::FileTime;
use once_cell::sync::Lazy;
use std::ffi::OsString;
use std::fmt::{Display, Formatter};
use std::fs::File;
use std::io;
use std::io::{BufReader, Read, Seek, SeekFrom, Write};
use std::marker::PhantomData;
use std::ops::Deref;
use std::path::{Path, PathBuf};
use std::process::{ChildStdin, Command, Stdio};
use std::str::FromStr;
use std::sync::Mutex;
use std::thread::{spawn, JoinHandle};
use std::time::SystemTime;

pub mod db;

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
    let backup_size = ByteSize::from_str(&mutex_lock!(ARGS).backup_size)
        .expect("Failed to parse size string")
        .0;
    if backup_size < *CHUNK_SIZE {
        panic!("backup size > chunk size must require")
    }
    backup_size
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
    /// External program filter for backup files.
    ///
    /// E.g. for compression & encryption, `bash -c 'pbzip2 | openssl enc -aes-256-cbc -pbkdf2'` can be used.
    #[arg(short = 'f', long, allow_hyphen_values = true, num_args = 1..)]
    pub backup_output_filter: Option<Vec<OsString>>,
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

#[derive(Clone, Debug)]
pub struct FileEntry {
    /// Always be relative
    pub path: PathBuf,
    pub size: u64,
    pub mtime: FileNanoTime,
}

impl FileEntry {
    pub fn full_path(&self) -> PathBuf {
        mutex_lock!(ARGS).source_dir.join(&self.path)
    }
}

pub struct ChunkInfo {
    pub hash: Hash,
    pub bak_n: i32,
    pub offset: u64,
    pub size: u64,
}

pub struct SplitInfo {
    pub file_hash: Hash,
    pub chunks: Vec<ChunkInfo>,
}

#[derive(Copy, Clone, Hash, Eq, PartialEq, Debug)]
pub struct FileNanoTime(pub u64);

impl From<FileTime> for FileNanoTime {
    fn from(value: FileTime) -> Self {
        let sec: u64 = value.unix_seconds().try_into().expect("Negative seconds");
        let nano_portion: u64 = value.nanoseconds() as u64;
        Self(sec * 1_000_000_000 + nano_portion)
    }
}

impl Deref for FileNanoTime {
    type Target = u64;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

pub fn index_files(dir: impl AsRef<Path>) -> io::Result<Vec<FileEntry>> {
    let base_dir = dir.as_ref();
    let mut collected = Vec::new();
    let walk = jwalk::WalkDir::new(base_dir).skip_hidden(false);
    for x in walk {
        let e = x?;
        // only accept regular files
        if !e.file_type.is_file() {
            continue;
        }
        let metadata = e.metadata()?;
        let mtime = FileTime::from_last_modification_time(&metadata);
        let relative_path = pathdiff::diff_paths(e.path(), base_dir)
            .expect("Unexpected: cannot get a relative path");
        let entry = FileEntry {
            path: relative_path,
            size: metadata.len(),
            mtime: mtime.into(),
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

impl<R: Read + Seek> Seek for HashReadWrapper<R> {
    fn seek(&mut self, pos: SeekFrom) -> io::Result<u64> {
        self.inner.seek(pos)
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

#[derive(Default, Copy, Clone, Hash, Eq, PartialEq)]
pub struct Hash(pub [u8; HASH_SIZE]);

impl Deref for Hash {
    type Target = [u8; HASH_SIZE];

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

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

/// Represents raw bytes of a `Path`.
///
/// On Linux, a path is naturally a `Vec<u8>`. On Windows, do a conversion.
pub struct PathBytes(pub Vec<u8>);

impl<P: Into<PathBuf>> From<P> for PathBytes {
    fn from(value: P) -> Self {
        let pb = value.into();
        #[allow(clippy::needless_late_init)]
        let bytes: Vec<u8>;
        cfg_if! {
            if #[cfg(unix)] {
                use std::os::unix::ffi::OsStringExt;
                bytes = pb.into_os_string().into_vec();
            } else {
                bytes = pb.to_str().expect("Invalid path: `to_str()` conversion failed").into();
            }
        }
        Self(bytes)
    }
}

impl PathBytes {
    fn into_path_buf(self) -> PathBuf {
        #[allow(clippy::needless_late_init)]
        let buf: PathBuf;
        cfg_if! {
            if #[cfg(unix)] {
                use std::os::unix::ffi::OsStringExt;
                buf = OsString::from_vec(self.0).into();
            } else {
                buf = String::from_utf8(self.0).expect("Invalid UTF-8 in path").into();
            }
        }
        buf
    }
}

impl Deref for PathBytes {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

pub struct ProgramFilterWrapper<W: Write + Send + 'static> {
    _p: PhantomData<W>,
    program_in: Option<ChildStdin>,
    copy_thread_handler: Option<JoinHandle<()>>,
}

impl<W: Write + Send + 'static> ProgramFilterWrapper<W> {
    pub fn new(writer: W, cmd: &[OsString]) -> io::Result<Self> {
        let mut child = Command::new(&cmd[0])
            .args(&cmd[1..])
            .stderr(Stdio::inherit())
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .spawn()?;
        let mut stdout = child.stdout.take().unwrap();
        let stdin = child.stdin.take().unwrap();

        let copy_thread = spawn(move || {
            let mut writer = writer;
            io::copy(&mut stdout, &mut writer).expect("Error: io copy stdout -> writer");
        });

        let this = Self {
            _p: Default::default(),
            program_in: Some(stdin),
            copy_thread_handler: Some(copy_thread),
        };
        Ok(this)
    }

    pub fn wait_for_output_stream(&mut self) {
        if let Some(x) = self.copy_thread_handler.take() {
            x.join().expect("Failed to join thread");
        }
    }
    
    pub fn close_program_in(&mut self) {
        if let Some(x) = self.program_in.take() {
            drop(x);
        }
    }
}

impl<W:Write+Send+'static> Drop for ProgramFilterWrapper<W> {
    fn drop(&mut self) {
        self.close_program_in();
        self.wait_for_output_stream();
    }
}

impl<W: Write + Send> Write for ProgramFilterWrapper<W> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.program_in.as_ref().unwrap().write(buf)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.program_in.as_ref().unwrap().flush()
    }
}

pub enum BakOutputType<W: Write + Send + 'static> {
    Plain(W),
    Filtered(ProgramFilterWrapper<W>),
}

pub struct BakOutputWriter<W: Write + Send + 'static> {
    output: BakOutputType<W>,
}

impl<W: Write + Send + 'static> BakOutputWriter<W> {
    pub fn new(inner: W, filter: Option<&Vec<OsString>>) -> io::Result<Self> {
        let output = if let Some(f) = filter {
            BakOutputType::Filtered(ProgramFilterWrapper::new(inner, f)?)
        } else {
            BakOutputType::Plain(inner)
        };

        Ok(Self { output })
    }
}

impl<W: Write + Send + 'static> Write for BakOutputWriter<W> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        match &mut self.output {
            BakOutputType::Plain(x) => x.write(buf),
            BakOutputType::Filtered(x) => x.write(buf),
        }
    }

    fn flush(&mut self) -> io::Result<()> {
        match &mut self.output {
            BakOutputType::Plain(x) => x.flush(),
            BakOutputType::Filtered(x) => x.flush(),
        }
    }
}
