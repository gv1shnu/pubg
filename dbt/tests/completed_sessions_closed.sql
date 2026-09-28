select s.* from {{ ref('fct_sessions') }} s
where s.connected and not exists (
 select 1 from {{ source('curated','match_status') }} m
 where m.namespace=s.namespace and m.run_id=s.run_id and (m.status not in ('final','corrected') or m.processed_at>now()-interval '30 seconds')
)
