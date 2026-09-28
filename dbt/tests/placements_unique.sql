select namespace,run_id,match_id,placement
from {{ ref('fct_player_match') }} where placement is not null
group by 1,2,3,4 having count(*)>1
