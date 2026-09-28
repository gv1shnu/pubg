select *,md5(namespace||'/'||run_id||'/'||event_id) as event_key
from {{ source('curated','events') }}
