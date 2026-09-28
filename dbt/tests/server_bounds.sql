select * from {{ ref('fct_server_windows') }}
where packet_loss_pct not between 0 and 100 or latency_p95_ms<0 or tick_ms<=0 or packets_sent<=0
