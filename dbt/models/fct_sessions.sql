select *,md5(namespace||'/'||run_id||'/'||server_id||'/'||player_id||'/'||session_id) as session_key
from {{ source('curated','sessions') }}
