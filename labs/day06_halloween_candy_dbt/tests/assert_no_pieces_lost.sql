-- Every piece in the raw sheet should make it to the fact table.
select *
from (select sum(total_count) as raw_pieces from {{ ref('raw_candy_haul') }})
cross join (select sum(piece_count) as fct_pieces from {{ ref('fct_candy_haul') }})
where raw_pieces != fct_pieces
