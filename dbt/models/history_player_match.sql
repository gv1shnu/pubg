{{ config(materialized='incremental',unique_key='player_match_key',incremental_strategy='delete+insert') }}
-- Re-read all eligible demo matches on every build, so late corrections replace earlier rows.
-- A larger deployment needs an affected-key journal; this bounded demo avoids a cutoff that loses late data.
select f.*,s.status,now() as built_at
from {{ ref('fct_player_match') }} f
join {{ source('curated','match_status') }} s using(namespace,run_id,match_id)
where s.status in ('final','corrected')
