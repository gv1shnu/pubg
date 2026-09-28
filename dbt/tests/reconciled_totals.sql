select f.namespace,f.run_id,f.match_id
from {{ ref('fct_player_match') }} f join {{ source('curated','match_status') }} s using(namespace,run_id,match_id)
where s.status in ('final','corrected') group by 1,2,3
having sum(kills)<>count(*)-1 or sum(damage)<>(count(*)-1)*100 or count(distinct placement)<>count(*)
