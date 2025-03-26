use backup_tool::{chunks_ranges, configure_log, index_files, mutex_lock, CliArgs, Hash, HashReadWrapper, ARGS, CHUNK_SIZE};
use clap::Parser;
use log::info;
use std::fs::File;
use std::{io, mem};
use std::cell::RefCell;
use std::io::{BufReader, BufWriter, Read, Seek, SeekFrom, Write};

fn main() -> anyhow::Result<()> {
    let args = CliArgs::parse();
    *mutex_lock!(ARGS) = args.clone();
    configure_log()?;

    if args.ref_index.is_none() {
        initial_backup()?;
    }
    Ok(())
}

fn initial_backup() -> anyhow::Result<()> {
    let mut files = index_files(&mutex_lock!(ARGS).source_dir)?;
    let out_dir = mutex_lock!(ARGS).out_dir.clone();
    let file_count = files.len();
    println!("{}", files.len());

    let mut file_chunks_hash = vec![Vec::<Hash>::new(); file_count];

    // Do the first full backup
    let mut bak_n = 0;
    let mut bak_total_size = 0_u64;
    // chunk offset of the current 'bak' file in index.txt
    let mut chunk_offset = 0_u64;
    let mut chunks_index = Vec::new();
    let create_bak_file = |bak_n: i32| -> anyhow::Result<BufWriter<File>> {
        let bak_file = out_dir.join(format!("bak{bak_n}"));
        Ok(BufWriter::new(File::create(&bak_file)?))
    };
    let mut bak_output = create_bak_file(bak_n)?;
    
    for (i, e) in files.iter_mut().enumerate() {
        let chunks = chunks_ranges(e.size);
        let mut reader = BufReader::new(File::open(&e.path)?);
        for (chunk_n, r) in chunks.iter().enumerate() {
            info!(
                "Write file [{i}/{file_count}] {} chunk #{}",
                e.path.display(),
                chunk_n + 1
            );
            reader.seek(SeekFrom::Start(r.start))?;
            let chunk_reader = reader.by_ref().take(r.size);
            let mut hash_wrapper = HashReadWrapper::new(chunk_reader);
            io::copy(&mut hash_wrapper, &mut bak_output)?;
            let chunk_hash = hash_wrapper.finalize();
            file_chunks_hash[i].push(chunk_hash);

            chunks_index.push((chunk_hash, format!("bak{bak_n}"), chunk_offset, r.size));
            
            bak_total_size += r.size;
            chunk_offset += r.size;
            // write to the new 'bak' file; close the old and create a new one
            if bak_total_size >= *CHUNK_SIZE {
                bak_n += 1;
                chunk_offset = 0;
                bak_output.flush()?;
                // directly assign to it; Rust will drop the old one
                bak_output = create_bak_file(bak_n)?;
            }
        }
    }
    // flush the last 'bak' file
    bak_output.flush()?;

    info!("Writing index file for chunks...");
    // format: chunk-hash, bak filename, offset, size
    let mut index_output = File::create(out_dir.join("index.txt"))?;
    writeln!(&mut index_output, "chunk-hash,bak-filename,offset,size")?;
    for x in chunks_index {
        use io::Write;
        writeln!(&mut index_output, "{},{},{},{}", x.0, x.1, x.2, x.3)?;
    }
    Ok(())
}
