{{ config(location=lake('gold', 'capture_coverage')) }}

-- Gold: how much of each post's comment section we actually hold. Instagram's
-- comments_count includes comments the API won't return (restricted users, some
-- filtered comments), so coverage < 1 is expected; a big gap is worth a look.
select
    m.media_id,
    m.permalink,
    m.posted_at,
    m.comments_count_reported,
    count(c.comment_id) as comments_captured,
    count(c.comment_id) filter (where not c.is_reply) as top_level_captured,
    count(c.comment_id) filter (where c.is_reply) as replies_captured,
    count(c.comment_id) filter (where c.ever_hidden) as hidden_captured,
    round(count(c.comment_id) / nullif(m.comments_count_reported, 0), 3) as coverage_ratio
from {{ ref('slv_media') }} m
left join {{ ref('slv_comments') }} c using (media_id)
group by m.media_id, m.permalink, m.posted_at, m.comments_count_reported
