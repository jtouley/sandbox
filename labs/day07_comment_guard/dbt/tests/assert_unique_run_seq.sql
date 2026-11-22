-- (run_id, seq) identifies one landed API response.
select run_id, seq, count(*) as n
from {{ ref('brz_api_responses') }}
group by all
having count(*) > 1
