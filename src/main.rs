#![feature(yeet_expr)]

use anyhow::anyhow;
use backup_tool::db::{IndexDb, IndexRow};
use backup_tool::{
    chunks_ranges, compute_file_hash, configure_log, create_user_dir, index_files,
    index_formatted_name, index_pick_last, mutex_lock, BakOutputWriter, ChunkInfo, CliArgs,
    FileEntry, Hash, HashReadWrapper, SplitInfo, ARGS, BACKUP_SIZE,
};
use clap::Parser;
use log::info;
use std::collections::{HashMap, HashSet};
use std::fs::File;
use std::io::{BufReader, BufWriter, Read, Seek, Write};
use std::path::{Path, PathBuf};
use std::{fs, io};
use yeet_ops::yeet;

fn main() -> anyhow::Result<()> {
    let args = CliArgs::parse();
    *mutex_lock!(ARGS) = args.clone();
    configure_log()?;

    if !args.out_dir.exists() {
        fs::create_dir_all(&args.out_dir)?;
    }
    let child_count = fs::read_dir(&args.out_dir)?.count();
    if child_count != 0 {
        yeet!(anyhow!(
            "Non-empty output directory; please choose another one."
        ));
    }

    let user_dir = create_user_dir(&args.source_dir)?;
    let last_index = index_pick_last(create_user_dir(args.source_dir)?)?;
    let ctx = Context {
        index_db: user_dir.join(index_formatted_name()),
        last_index,
    };

    match &ctx.last_index {
        None => {
            info!("Processing initial backup...");
            initial_backup(&ctx)?
        }
        Some(_) => {
            info!("Processing differential backup...");
            differential_backup(&ctx)?
        }
    }

    fs::copy(&ctx.index_db, args.out_dir.join("index.db"))?;
    Ok(())
}

struct Context {
    index_db: PathBuf,
    last_index: Option<PathBuf>,
}

fn differential_backup(ctx: &Context) -> anyhow::Result<()> {
    // full scan is still needed
    info!("Indexing files...");
    let files = index_files(&mutex_lock!(ARGS).source_dir)?;
    info!("File count: {}", files.len());
    let out_dir = mutex_lock!(ARGS).out_dir.clone();
    let ref_db_path = ctx.last_index.clone().unwrap();
    info!("Picked ref_db: {}", ref_db_path.display());
    let ref_db = IndexDb::new(ref_db_path, false)?;

    // find out differential files by path, mtime and size
    info!("Reading old index database...");
    let old_index = ref_db.select_index_all()?;
    let mut duplicates: Vec<(&FileEntry, Hash)> = Vec::new();
    info!("Deduplicating by metadata...");
    let metadata_map = old_index
        .iter()
        .map(|x| ((x.entry.path.as_path(), x.entry.mtime, x.entry.size), x))
        .collect::<HashMap<_, _>>();
    let old_index_hash_set = old_index.iter().map(|x| &x.hash).collect::<HashSet<_>>();
    let mut remaining = Vec::new();
    for e in &files {
        if let Some(v) = metadata_map.get(&(e.path.as_path(), e.mtime, e.size)) {
            duplicates.push((e, Hash(v.hash)));
        } else {
            remaining.push(e);
        }
    }
    info!("File count: {}", remaining.len());
    info!("Deduplicating diff by hash...");
    // if the diff file hash matches in the old file index, skip its backup
    let mut files_to_backup = Vec::new();
    let remaining_count = remaining.len();
    for (i, e) in remaining.into_iter().enumerate() {
        info!("Hashing: [{}/{}] {}", i, remaining_count, e.path.display());
        let file_hash = compute_file_hash(e.full_path())?;
        if !old_index_hash_set.contains(&&*file_hash) {
            files_to_backup.push((file_hash, e));
        } else {
            duplicates.push((e, file_hash));
        }
    }
    files_to_backup.sort_by(|a, b| a.1.path.cmp(&b.1.path));
    info!("File count: {}", files_to_backup.len());
    assert_eq!(duplicates.len() + files_to_backup.len(), files.len());

    info!("Writing to backup files...");
    let file_splits = write_bak_files(&out_dir, files_to_backup.iter().copied())?;

    info!("Creating index database...");
    let mut db = IndexDb::new(&ctx.index_db, true)?;
    let db_tx = db.transaction()?;
    db_tx.insert_file_split_info(&file_splits)?;
    let new_files_ref_map = files_to_backup
        .iter()
        .map(|x| (x.1 as *const _, x))
        .collect::<HashMap<_, _>>();
    // the current index = files_to_backup ...
    for e in &files {
        let entry = new_files_ref_map.get(&(e as *const _));
        if let Some(e) = entry {
            // this is the new file being backed up
            let row = IndexRow {
                hash: *e.0,
                entry: e.1.clone(),
            };
            db_tx.insert_index_row(&row)?;
        }
    }
    // ... + duplicates
    for x in duplicates {
        db_tx.insert_index_row(&IndexRow {
            entry: x.0.clone(),
            hash: *x.1,
        })?;
    }
    // so if I do db_tx.0.commit()? outside, it doesn't work
    {
        let tx = db_tx;
        tx.0.commit()?;
    }
    assert_eq!(db.query_index_row_count()?, files.len() as u64);
    assert_eq!(
        db.query_chunk_row_count()?,
        file_splits.iter().map(|x| x.chunks.len()).sum::<usize>() as u64
    );

    Ok(())
}

fn initial_backup(ctx: &Context) -> anyhow::Result<()> {
    // Do the first full backup
    info!("Indexing files...");
    let src_dir = mutex_lock!(ARGS).source_dir.clone();
    let files = index_files(&src_dir)?;
    let out_dir = mutex_lock!(ARGS).out_dir.clone();
    let file_count = files.len();
    info!("File count: {file_count}");

    info!("Deduplicating by hash. Please wait...");
    let mut file_hash_list = Vec::new();
    let mut unique_list = HashMap::new();
    for (i, e) in files.iter().enumerate() {
        info!("Hashing: [{}/{}] {}", i, file_count, e.path.display());
        let hash = compute_file_hash(e.full_path())?;
        unique_list.insert(hash, e);
        file_hash_list.push(hash);
    }
    info!("Writing to backup files...");

    let mut files_to_backup = unique_list.iter().map(|x| (*x.0, *x.1)).collect::<Vec<_>>();
    files_to_backup.sort_by(|a, b| a.1.path.cmp(&b.1.path));
    let file_splits = write_bak_files(&out_dir, files_to_backup.into_iter())?;

    info!("Creating index database...");
    let mut db = IndexDb::new(&ctx.index_db, true)?;
    let db_tx = db.transaction()?;
    for x in files.iter().zip(file_hash_list) {
        db_tx.insert_index_row(&IndexRow {
            hash: *x.1,
            entry: x.0.clone(),
        })?;
    }
    db_tx.insert_file_split_info(&file_splits)?;
    db_tx.0.commit()?;
    info!("Done");
    Ok(())
}

fn write_bak_files<'a>(
    out_dir: &Path,
    files: impl ExactSizeIterator<Item = (Hash, &'a FileEntry)>,
) -> anyhow::Result<Vec<SplitInfo>> {
    let file_count = files.len();
    let mut file_chunks_hash = vec![Vec::<Hash>::new(); file_count];

    let mut bak_n = 0;
    let mut bak_total_size = 0_u64;
    // chunk offset of the current 'bak' file in index.txt
    let mut chunk_offset = 0_u64;
    let output_filter = mutex_lock!(ARGS).backup_output_filter.clone();
    let create_bak_file = |bak_n: i32| -> anyhow::Result<_> {
        let bak_file = out_dir.join(format!("bak{bak_n}"));
        let writer = BufWriter::new(File::create(&bak_file)?);
        let writer = BakOutputWriter::new(writer, output_filter.as_ref())?;
        Ok(writer)
    };
    let mut bak_output = create_bak_file(bak_n)?;

    let mut split_info_list = Vec::new();

    for (i, e) in files.into_iter().enumerate() {
        let file_size = e.1.size;
        let file_path = e.1.path.as_path();
        let file_path_full = e.1.full_path();
        split_info_list.push(SplitInfo {
            file_hash: e.0,
            chunks: Default::default(),
        });

        let chunks = chunks_ranges(file_size);
        let mut reader = BufReader::new(File::open(file_path_full)?);
        for (chunk_n, r) in chunks.iter().enumerate() {
            info!(
                "Write file [{i}/{file_count}] {} chunk #{}",
                file_path.display(),
                chunk_n + 1
            );

            // Check if a new 'bak' file is needed, that's, this 'bak' file is not sufficient for
            // storing a new chunk.
            // write to the new 'bak' file; close the old and create a new one
            if bak_total_size + r.size > *BACKUP_SIZE {
                bak_n += 1;
                chunk_offset = 0;
                bak_output.flush()?;
                // directly assign to it; Rust will drop the old one
                bak_output = create_bak_file(bak_n)?;
                bak_total_size = 0;
            }

            let chunk_reader = reader.by_ref().take(r.size);
            let mut hash_wrapper = HashReadWrapper::new(chunk_reader);
            io::copy(&mut hash_wrapper, &mut bak_output)?;
            let chunk_hash = hash_wrapper.finalize();
            file_chunks_hash[i].push(chunk_hash);

            split_info_list[i].chunks.push(ChunkInfo {
                hash: chunk_hash,
                bak_n,
                offset: chunk_offset,
                size: r.size,
            });

            bak_total_size += r.size;
            chunk_offset += r.size;
        }
        debug_assert_eq!(reader.stream_position()?, file_size);
    }
    // flush the last 'bak' file
    bak_output.flush()?;

    Ok(split_info_list)
}
