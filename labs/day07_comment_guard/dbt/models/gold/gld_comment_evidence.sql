{{ config(location=lake('gold', 'comment_evidence')) }}

-- Gold: one row per comment, ready for review/classification, with post context and
-- everything needed to pull the verbatim original. `possibly_removed` means the post's
-- comments were captured again in a later run and this comment wasn't in it (deleted by
-- its author or the creator, caught by filters, or the author was restricted). The
-- earlier verbatim copy stays in landing/bronze either way.
with last_comment_capture as (
    select media_id, arg_max(run_id, captured_at) as last_run_id, max(captured_at) as last_captured_at
    from {{ ref('brz_api_responses') }}
    where stream = 'comments' and status_code = 200
    group by media_id
),

seen_in_last_run as (
    select distinct o.comment_id
    from {{ ref('slv_comment_observations') }} o
    join last_comment_capture l on o.media_id = l.media_id and o.run_id = l.last_run_id
)

select
    c.comment_id,
    c.media_id,
    m.permalink as post_permalink,
    m.posted_at as post_posted_at,
    left(m.caption, 140) as post_caption_snippet,
    c.parent_comment_id,
    c.is_reply,
    c.commenter_username,
    c.commenter_id,
    c.text,
    c.text_sha256,
    c.commented_at,
    c.like_count,
    c.hidden,
    c.ever_hidden,
    c.text_changed,
    c.first_seen_at,
    c.last_seen_at,
    c.observation_count,
    l.last_captured_at as post_last_captured_at,
    l.last_run_id is not null and s.comment_id is null as possibly_removed,
    c.first_evidence_file,
    c.first_evidence_seq,
    c.first_evidence_body_sha256
from {{ ref('slv_comments') }} c
left join {{ ref('slv_media') }} m using (media_id)
left join last_comment_capture l using (media_id)
left join seen_in_last_run s using (comment_id)
