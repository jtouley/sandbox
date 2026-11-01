select
    trim(candy_name)        as candy_name,
    display_name,
    manufacturer,
    category,
    texture,
    is_candy,
    has_chocolate,
    has_peanuts,
    has_tree_nuts,
    est_sugar_g_per_piece,
    est_calories_per_piece,
    trade_value
from {{ ref('candy_attributes') }}
