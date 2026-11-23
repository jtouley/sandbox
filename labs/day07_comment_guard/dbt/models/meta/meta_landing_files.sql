{{ config(location=lake('meta', 'landing_files')) }}

-- Meta: every landed file (raw responses and per-comment lines), with the hash recorded at capture time and the hash of the
-- file as it is on disk now. `file_intact` = false means the file changed after capture.
with recorded as (
    select r.run_id, unnest(r.files) as f
    from read_json(
        '{{ var("data_dir") }}/meta/runs/*.json',
        columns = {
            run_id: 'VARCHAR',
            files: 'STRUCT(stream VARCHAR, path VARCHAR, bytes BIGINT, lines BIGINT, sha256 VARCHAR)[]'
        }
    ) r
),

on_disk as (
    select filename as abs_path, size as bytes_on_disk, sha256(content) as sha256_on_disk
    from read_blob(['{{ landing_glob() }}', '{{ comment_lines_glob() }}'])
)

select
    r.run_id,
    r.f.stream as stream,
    r.f.path as path,
    r.f.bytes as bytes_recorded,
    r.f.lines as lines_recorded,
    r.f.sha256 as sha256_recorded,
    d.abs_path,
    d.bytes_on_disk,
    d.sha256_on_disk,
    d.sha256_on_disk is not null as present,
    coalesce(d.sha256_on_disk = r.f.sha256, false) as file_intact
from recorded r
left join on_disk d on d.abs_path = '{{ var("data_dir") }}/' || r.f.path
