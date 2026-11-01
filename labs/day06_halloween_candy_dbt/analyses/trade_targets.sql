-- Low-value candy we have lots of = what to trade away.
select display_name, piece_count, trade_value
from {{ ref('fct_candy_haul') }}
where is_candy and trade_value <= 2
order by piece_count desc
