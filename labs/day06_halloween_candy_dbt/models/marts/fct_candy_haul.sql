-- One row per candy type: the main table to slice and chart.
with enriched as (
    select * from {{ ref('int_candy_enriched') }}
)

select
    '{{ var("trick_or_treater") }}'                  as trick_or_treater,
    {{ var('halloween_year') }}                      as halloween_year,
    candy_name,
    display_name,
    manufacturer,
    category,
    texture,
    is_candy,
    has_chocolate,
    has_peanuts,
    has_tree_nuts,
    trade_value,
    piece_count,
    parent_tax_pieces,
    piece_count - parent_tax_pieces                  as pieces_kept,
    est_total_sugar_g,
    est_total_calories,
    {{ pct('piece_count', 'sum(piece_count) over ()') }} as pct_of_haul,
    rank() over (order by piece_count desc)          as popularity_rank,
    piece_count * trade_value                        as trade_points
from enriched
