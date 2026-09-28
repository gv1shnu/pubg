select *,md5(namespace||'/'||run_id||'/'||server_id||'/'||window_start::text) as window_key,
md5(namespace||'/'||run_id||'/'||server_id) as server_key
from {{ source('curated','server_windows') }}
