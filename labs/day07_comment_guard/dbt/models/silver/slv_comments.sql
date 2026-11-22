{{ config(location=lake('silver', 'comments')) }}

-- Silver: current state of each comment plus its sighting history summary.
select
    comment_id,
    arg_max(media_id, captured_at) as media_id,
    arg_max(parent_comment_id, captured_at) as parent_comment_id,
    bool_or(parent_comment_id is not null) as is_reply,
    arg_max(text, captured_at) as text,
    arg_max(text_sha256, captured_at) as text_sha256,
    arg_max(commenter_username, captured_at) as commenter_username,
    arg_max(commenter_id, captured_at) as commenter_id,
    min(commented_at) as commented_at,
    arg_max(like_count, captured_at) as like_count,
    arg_max(hidden, captured_at) as hidden,
    bool_or(coalesce(hidden, false)) as ever_hidden,
    count(distinct text_sha256) > 1 as text_changed,
    min(captured_at) as first_seen_at,
    max(captured_at) as last_seen_at,
    arg_max(run_id, captured_at) as last_seen_run_id,
    count(*) as observation_count,
    -- where to find the first verbatim copy (for an evidence packet)
    arg_min(evidence_file, captured_at) as first_evidence_file,
    arg_min(evidence_seq, captured_at) as first_evidence_seq,
    arg_min(evidence_body_sha256, captured_at) as first_evidence_body_sha256
from {{ ref('slv_comment_observations') }}
group by comment_id
