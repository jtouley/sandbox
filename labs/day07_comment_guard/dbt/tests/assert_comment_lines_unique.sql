-- Each comment appears once per run in the per-comment lines.
select run_id, comment_id, count(*) as n
from {{ ref('brz_comment_lines') }}
group by all
having count(*) > 1
