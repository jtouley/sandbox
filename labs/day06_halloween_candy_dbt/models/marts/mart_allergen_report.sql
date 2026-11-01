-- Pieces to set aside for a friend with a nut allergy.
select
    case
        when has_peanuts and has_tree_nuts then 'peanuts + tree nuts'
        when has_peanuts                   then 'peanuts'
        when has_tree_nuts                 then 'tree nuts'
        else 'nut-free (per label, double-check!)'
    end                                               as allergen_group,
    sum(piece_count)                                  as pieces,
    list(display_name order by piece_count desc)      as candies
from {{ ref('fct_candy_haul') }}
where is_candy or category = 'snack'
group by all
order by pieces desc
