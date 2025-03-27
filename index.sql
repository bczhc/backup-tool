create table if not exists `index`
(
    path  blob unique,
    size  integer,
    mtime integer,
    hash  blob
);

create table if not exists chunk
(
    file_hash  blob,
    chunk_hash blob,
    bak_n      integer,
    offset     integer,
    size       integer
)