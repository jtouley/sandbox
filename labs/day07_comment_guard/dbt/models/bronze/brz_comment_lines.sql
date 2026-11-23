{{ config(location=lake('bronze', 'comment_lines')) }}

-- Bronze: the per-post comment files written at capture time, one row per comment/reply,
-- in the order they appear in the file (each top-level comment followed by its replies).
select
    run_id,
    captured_at,
    post_id,
    post_shortcode,
    post_permalink,
    post_posted_at,
    post_caption,
    comment_id,
    parent_comment_id,
    is_reply,
    username,
    user_id,
    text,
    commented_at,
    like_count,
    hidden,
    source ->> 'file' as source_file,
    (source ->> 'seq')::bigint as source_seq,
    source ->> 'body_sha256' as source_body_sha256,
    filename as lines_file,
    row_number() over (partition by filename) as line_number,
    schema_version
from read_ndjson(
    '{{ comment_lines_glob() }}',
    filename = true,
    columns = {
        schema_version: 'INTEGER',
        run_id: 'VARCHAR',
        captured_at: 'TIMESTAMPTZ',
        post_id: 'VARCHAR',
        post_shortcode: 'VARCHAR',
        post_permalink: 'VARCHAR',
        post_posted_at: 'TIMESTAMPTZ',
        post_caption: 'VARCHAR',
        comment_id: 'VARCHAR',
        parent_comment_id: 'VARCHAR',
        is_reply: 'BOOLEAN',
        username: 'VARCHAR',
        user_id: 'VARCHAR',
        text: 'VARCHAR',
        commented_at: 'TIMESTAMPTZ',
        like_count: 'INTEGER',
        hidden: 'BOOLEAN',
        source: 'JSON'
    }
)
