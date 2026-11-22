{{ config(location=lake('silver', 'comment_observations')) }}

-- Silver: one row per (comment, run) sighting. Comments arrive three ways:
--   top-level items on /{media}/comments pages, the first page of replies nested
--   inside them, and further /{comment}/replies pages. Each keeps its evidence pointer.
with pages as (
    select *
    from {{ ref('brz_api_responses') }}
    where status_code = 200 and stream in ('comments', 'replies')
),

items as (
    select
        p.run_id, p.seq, p.captured_at, p.stream, p.media_id, p.parent_comment_id,
        p.landing_file, p.body_sha256,
        unnest(json_extract(p.body, '$.data[*]')) as c
    from pages p
),

top_level as (
    select
        *,
        case when stream = 'replies' then 'replies_page' else 'comments_page' end as observed_via,
        case when stream = 'replies' then parent_comment_id end as parent_id_ctx
    from items
),

nested as (
    select * exclude (reply), reply as c
    from (
        select
            run_id, seq, captured_at, stream, media_id, parent_comment_id, landing_file,
            body_sha256,
            'nested_reply' as observed_via,
            c ->> 'id' as parent_id_ctx,
            unnest(json_extract(c, '$.replies.data[*]')) as reply
        from items
        where stream = 'comments'
    )
),

all_items as (
    select * from top_level
    union all by name
    select * from nested
),

typed as (
    select
        c ->> 'id' as comment_id,
        media_id,
        coalesce(c ->> 'parent_id', parent_id_ctx) as parent_comment_id,
        c ->> 'text' as text,
        sha256(coalesce(c ->> 'text', '')) as text_sha256,
        coalesce(c ->> '$.from.username', c ->> 'username') as commenter_username,
        c ->> '$.from.id' as commenter_id,
        (c ->> 'timestamp')::timestamptz as commented_at,
        (c ->> 'like_count')::integer as like_count,
        (c ->> 'hidden')::boolean as hidden,
        observed_via,
        run_id,
        captured_at,
        seq as evidence_seq,
        landing_file as evidence_file,
        body_sha256 as evidence_body_sha256,
        c as raw_item
    from all_items
)

-- The same reply can show up nested and on a replies page in one run; keep the first.
select * from typed
qualify row_number() over (
    partition by comment_id, run_id order by captured_at, evidence_seq
) = 1
