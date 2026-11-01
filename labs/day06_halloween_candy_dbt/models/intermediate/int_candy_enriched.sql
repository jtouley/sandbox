with haul as (
    select * from {{ ref('stg_candy_haul') }}
),

attrs as (
    select * from {{ ref('stg_candy_attributes') }}
)

select
    haul.candy_name,
    attrs.display_name,
    attrs.manufacturer,
    attrs.category,
    attrs.texture,
    attrs.is_candy,
    attrs.has_chocolate,
    attrs.has_peanuts,
    attrs.has_tree_nuts,
    attrs.trade_value,
    haul.piece_count,
    attrs.est_sugar_g_per_piece,
    attrs.est_calories_per_piece,
    haul.piece_count * attrs.est_sugar_g_per_piece   as est_total_sugar_g,
    haul.piece_count * attrs.est_calories_per_piece  as est_total_calories,

    -- Parents only tax the chocolate. Obviously.
    case
        when attrs.has_chocolate
            then round(haul.piece_count * {{ var('parent_tax_pct') }})::int
        else 0
    end as parent_tax_pieces

from haul
left join attrs using (candy_name)
