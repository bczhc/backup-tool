use backup_tool::db::{ChunkRow, IndexDb, IndexRow};
use backup_tool::{
    chunks_ranges, compute_file_hash, configure_log, index_files, mutex_lock, ChunkInfo, CliArgs,
    FileEntry, Hash, HashReadWrapper, SplitInfo, ARGS, BACKUP_SIZE, CHUNK_SIZE,
};
use clap::Parser;
use log::info;
use std::cell::RefCell;
use std::collections::{HashMap, HashSet};
use std::fs::File;
use std::io::{BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::{fs, io, mem};

fn main() -> anyhow::Result<()> {
    let args = CliArgs::parse();
    *mutex_lock!(ARGS) = args.clone();
    configure_log()?;

    if args.ref_index.is_none() {
        initial_backup()?;
    } else {
        differential_backup()?;
    }
    Ok(())
}

fn differential_backup() -> anyhow::Result<()> {
    // full scan is still needed
    info!("Indexing files...");
    let files = index_files(&mutex_lock!(ARGS).source_dir)?;
    let out_dir = mutex_lock!(ARGS).out_dir.clone();
    let db_path = mutex_lock!(ARGS).ref_index.as_ref().unwrap().clone();
    let db = IndexDb::new(db_path)?;

    // find out differential files by path, mtime and size
    info!("Reading old index database...");
    let old_index = db.select_index_all()?;
    // let old_chunk = Vec::new(); todo!();
    info!("Deduplicating by metadata...");
    let metadata_set = old_index
        .iter()
        .map(|x| (x.entry.path.as_path(), x.entry.mtime, x.entry.size))
        .collect::<HashSet<_>>();
    let hash_set = old_index.iter().map(|x| &x.hash).collect::<HashSet<_>>();
    let diff = files
        .iter()
        .filter(|&e| !metadata_set.contains(&(e.path.as_path(), e.mtime, e.size)))
        .collect::<Vec<_>>();
    info!("Deduplicating diff by hash...");
    // move may occur, so compute the hash on the diff files and if they match hashes in old index,
    // remove the entry
    let old_diff = diff;
    let mut diff = Vec::new();
    for e in old_diff {
        let file_hash = compute_file_hash(&e.path)?;
        if hash_set.contains(&&*file_hash) {
            diff.push(e);
        }
    }
    info!("Diff count: {}", diff.len());

    let diff_path_set = diff
        .iter()
        .map(|x| x.path.as_path())
        .collect::<HashSet<_>>();
    // these files are not touched; thus can be copied to the new index.db directly
    let copied_entries = files
        .iter()
        .filter(|x| diff_path_set.contains(x.path.as_path()))
        .collect::<Vec<_>>();

    // back up diff files
    // write_bak_files(&out_dir, todo!(), todo!())?;
    info!("Adding up old index entries...");
    let old_index_map = old_index
        .iter()
        .map(|x| (x.entry.path.as_path(), x))
        .collect::<HashMap<_, _>>();
    // let old_chunk_map = old_chunk
    //     .iter()
    //     .map(|x| (x.file_hash, x))
    //     .collect::<HashMap<_, _>>();
    // reopen the db, append data
    let mut db = IndexDb::new(out_dir.join("index.db"))?;
    let db_tx = db.transaction()?;
    for e in copied_entries {
        // assert: these lookups always succeed
        let index_row = old_index_map[e.path.as_path()];
        // let chunk_row = old_chunk_map[&index_row.hash];
        // db_tx.insert_index_row(index_row)?;
        // db_tx.insert_chunk_row(chunk_row)?;
    }
    db_tx.0.commit()?;

    Ok(())
}

fn initial_backup() -> anyhow::Result<()> {
    // Do the first full backup
    let src_dir = mutex_lock!(ARGS).source_dir.clone();
    let files = index_files(&src_dir)?;
    let out_dir = mutex_lock!(ARGS).out_dir.clone();
    let file_count = files.len();
    info!("File count: {file_count}");

    info!("Deduplicating by hash. Please wait...");
    let mut file_hash_list = Vec::new();
    let mut unique_list = HashMap::new();
    for (i, e) in files.iter().enumerate() {
        info!("[{}/{}] {}", i, file_count, e.path.display());
        let hash = compute_file_hash(&e.path)?;
        unique_list.insert(hash, e);
        file_hash_list.push(hash);
    }
    info!("Writing to backup files...");
    let file_splits =
        write_bak_files(&out_dir, &src_dir, unique_list.iter().map(|x| (*x.0, *x.1)))?;

    info!("Creating index database...");
    let db_path = out_dir.join("index.db");
    if db_path.exists() {
        fs::remove_file(&db_path)?;
    }
    let mut db = IndexDb::new(&db_path)?;
    let db_tx = db.transaction()?;
    for x in files.iter().zip(file_hash_list) {
        db_tx.insert_index_row(&IndexRow {
            hash: *x.1,
            entry: x.0.clone(),
        })?;
    }
    for x in file_splits {
        let file_hash = x.file_hash;
        for x in x.chunks {
            db_tx.insert_chunk_row(&ChunkRow {
                file_hash: *file_hash,
                bak_n: x.bak_n,
                chunk_hash: *x.hash,
                offset: x.offset,
                size: x.size,
            })?;
        }
    }
    db_tx.0.commit()?;
    info!("Done");
    Ok(())
}

fn write_bak_files<'a>(
    out_dir: &Path,
    src_dir: &Path,
    files: impl ExactSizeIterator<Item = (Hash, &'a FileEntry)>,
) -> anyhow::Result<Vec<SplitInfo>> {
    let file_count = files.len();
    let mut file_chunks_hash = vec![Vec::<Hash>::new(); file_count];

    let mut bak_n = 0;
    let mut bak_total_size = 0_u64;
    // chunk offset of the current 'bak' file in index.txt
    let mut chunk_offset = 0_u64;
    let create_bak_file = |bak_n: i32| -> anyhow::Result<BufWriter<File>> {
        let bak_file = out_dir.join(format!("bak{bak_n}"));
        Ok(BufWriter::new(File::create(&bak_file)?))
    };
    let mut bak_output = create_bak_file(bak_n)?;

    let mut split_info_list = Vec::new();

    for (i, e) in files.into_iter().enumerate() {
        let file_size = e.1.size;
        let file_path = e.1.path.as_path();
        split_info_list.push(SplitInfo {
            file_hash: e.0,
            chunks: Default::default(),
        });

        let chunks = chunks_ranges(file_size);
        let mut reader = BufReader::new(File::open(src_dir.join(file_path))?);
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
