create table if not exists `index`
(
    path  blob,
    size  integer,
    mtime integer,
    hash  blob
);

create table if not exists chunk
(
    file_hash blob,
    -- comma-separated list of chunk digests
    splits    text
)