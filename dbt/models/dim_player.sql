select *,md5(namespace||'/'||run_id||'/'||match_id||'/'||player_id) as player_match_key
from {{ source('curated','dim_players') }}
