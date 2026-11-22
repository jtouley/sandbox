{{ config(location=lake('gold', 'commenter_activity')) }}

-- Gold: per-commenter activity on this creator's posts. PRIVATE working data for the
-- creator's own moderation and safety review. Do not publish: labelling people from this
-- table is a defamation and doxxing risk (see README).
select
    coalesce(commenter_id, 'username:' || commenter_username) as commenter_key,
    any_value(commenter_id) as commenter_id,
    arg_max(commenter_username, last_seen_at) as commenter_username,
    count(*) as comments,
    count(distinct media_id) as posts_commented_on,
    count(*) filter (where is_reply) as replies,
    count(*) filter (where ever_hidden) as hidden_comments,
    count(*) filter (where possibly_removed) as possibly_removed_comments,
    min(commented_at) as first_comment_at,
    max(commented_at) as last_comment_at
from {{ ref('gld_comment_evidence') }}
group by 1
