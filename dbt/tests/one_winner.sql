select f.namespace,f.run_id,f.match_id
from {{ ref('fct_player_match') }} f join {{ source('curated','match_status') }} s using(namespace,run_id,match_id)
where s.status in ('final','corrected') group by 1,2,3 having count(*) filter(where placement=1)<>1
