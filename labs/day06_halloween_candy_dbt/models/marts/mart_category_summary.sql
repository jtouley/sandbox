select
    category,
    count(*)                                          as candy_types,
    sum(piece_count)                                  as pieces,
    {{ pct('sum(piece_count)', 'sum(sum(piece_count)) over ()') }} as pct_of_haul,
    round(avg(trade_value), 2)                        as avg_trade_value,
    sum(est_total_sugar_g)                            as est_sugar_g,
    arg_max(display_name, piece_count)                as top_candy
from {{ ref('fct_candy_haul') }}
group by category
order by pieces desc
