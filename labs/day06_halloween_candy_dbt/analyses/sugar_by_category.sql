select category, pieces, est_sugar_g, round(est_sugar_g / pieces, 1) as sugar_g_per_piece
from {{ ref('mart_category_summary') }}
order by est_sugar_g desc
