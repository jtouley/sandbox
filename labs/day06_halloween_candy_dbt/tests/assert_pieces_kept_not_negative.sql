-- Fails if parent_tax_pct > 1 takes more pieces than exist.
select * from {{ ref('fct_candy_haul') }} where pieces_kept < 0
