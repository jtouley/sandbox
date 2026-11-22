-- Every JSONL file that bronze read must be listed (with a hash) in a run manifest.
select distinct b.landing_file
from {{ ref('brz_api_responses') }} b
left join {{ ref('meta_landing_files') }} f on f.abs_path = b.landing_file
where f.abs_path is null
