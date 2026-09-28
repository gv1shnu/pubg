select namespace,run_id,server_id,player_id,session_id
from {{ ref('fct_sessions') }} group by 1,2,3,4,5 having count(*)>1 or min(sequence)<1
