-- A single row of headline numbers for a KPI tile.
select
    any_value(trick_or_treater)                          as trick_or_treater,
    any_value(halloween_year)                            as halloween_year,
    count(*)                                             as candy_types,
    sum(piece_count)                                     as total_pieces,
    sum(piece_count) filter (where is_candy)             as candy_pieces,
    sum(piece_count) filter (where not is_candy)         as non_candy_pieces,
    sum(piece_count) filter (where has_chocolate)        as chocolate_pieces,
    {{ pct('sum(piece_count) filter (where has_chocolate)', 'sum(piece_count)') }} as pct_chocolate,
    sum(parent_tax_pieces)                               as parent_tax_pieces,
    round(sum(est_total_sugar_g) / 1000.0, 2)            as est_total_sugar_kg,
    sum(est_total_calories)::int                         as est_total_calories,
    sum(trade_points)                                    as total_trade_points,
    arg_max(display_name, piece_count)                   as most_common_candy
from {{ ref('fct_candy_haul') }}
