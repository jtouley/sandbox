-- The readable per-comment lines and the raw API responses must agree exactly:
-- same comments, same text, same parent, in every run. Rows returned = mismatches.
with lines as (
    select run_id, comment_id, post_id, parent_comment_id, text
    from {{ ref('brz_comment_lines') }}
),

raw as (
    select run_id, comment_id, media_id as post_id, parent_comment_id, text
    from {{ ref('slv_comment_observations') }}
)

select 'missing_from_lines' as problem, * from (select * from raw except select * from lines)
union all
select 'missing_from_raw' as problem, * from (select * from lines except select * from raw)
