select *,md5(namespace||'/'||run_id||'/'||server_id) as server_key
from {{ source('curated','dim_servers') }}
