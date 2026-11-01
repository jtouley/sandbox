select popularity_rank, display_name, piece_count, pct_of_haul
from {{ ref('fct_candy_haul') }}
order by piece_count desc
limit 10
