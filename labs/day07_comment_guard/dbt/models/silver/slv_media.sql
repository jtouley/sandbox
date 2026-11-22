{{ config(location=lake('silver', 'media')) }}

-- Silver: latest state of each post, from /me/media pages.
with items as (
    select run_id, captured_at, unnest(json_extract(body, '$.data[*]')) as m
    from {{ ref('brz_api_responses') }}
    where status_code = 200 and stream = 'media'
),

typed as (
    select
        m ->> 'id' as media_id,
        m ->> 'permalink' as permalink,
        m ->> 'shortcode' as shortcode,
        m ->> 'media_type' as media_type,
        m ->> 'media_product_type' as media_product_type,
        m ->> 'caption' as caption,
        (m ->> 'timestamp')::timestamptz as posted_at,
        (m ->> 'comments_count')::integer as comments_count,
        (m ->> 'like_count')::integer as like_count,
        m ->> 'username' as owner_username,
        run_id,
        captured_at
    from items
)

select
    media_id,
    arg_max(permalink, captured_at) as permalink,
    arg_max(shortcode, captured_at) as shortcode,
    arg_max(media_type, captured_at) as media_type,
    arg_max(media_product_type, captured_at) as media_product_type,
    arg_max(caption, captured_at) as caption,
    min(posted_at) as posted_at,
    arg_max(comments_count, captured_at) as comments_count_reported,
    arg_max(like_count, captured_at) as like_count,
    arg_max(owner_username, captured_at) as owner_username,
    min(captured_at) as first_seen_at,
    max(captured_at) as last_seen_at
from typed
group by media_id
