-- Pieces and market share by manufacturer.
select
    manufacturer,
    count(*)                                          as candy_types,
    sum(piece_count)                                  as pieces,
    {{ pct('sum(piece_count)', 'sum(sum(piece_count)) over ()') }} as market_share_pct,
    list(display_name order by piece_count desc)      as brands
from {{ ref('fct_candy_haul') }}
group by manufacturer
order by pieces desc
