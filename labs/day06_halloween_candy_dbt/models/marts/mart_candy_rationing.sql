-- "How long will it last?" Eats the best stuff first, `daily_piece_limit` pieces/day,
-- after the parent tax. Returns one row per piece with the day it gets eaten.
with kept as (
    select display_name, trade_value, pieces_kept
    from {{ ref('fct_candy_haul') }}
    where is_candy and pieces_kept > 0
),

pieces as (
    select
        display_name,
        trade_value,
        unnest(range(pieces_kept)) as piece_n
    from kept
),

ordered as (
    select
        *,
        row_number() over (order by trade_value desc, display_name, piece_n) as eat_order
    from pieces
)

select
    eat_order,
    ceil(eat_order / {{ var('daily_piece_limit') }})::int                  as day_number,
    (date '{{ var("halloween_year") }}-10-31'
        + to_days(ceil(eat_order / {{ var('daily_piece_limit') }})::int))::date as eat_date,
    display_name,
    trade_value
from ordered
order by eat_order
