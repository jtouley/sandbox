select
    trim(candy_name)          as candy_name,
    cast(total_count as int)  as piece_count
from {{ ref('raw_candy_haul') }}
