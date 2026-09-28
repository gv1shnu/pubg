# BI decision: Apache Superset

2026-09-28: The project owner selected Apache Superset and authorized local Docker testing. The final analytics surface is now Superset. Tableau deliverables remain as an optional historical reference, not a release blocker or the current product.

The Python/Kafka/Flink/PostgreSQL architecture is unchanged. Superset queries the same read-only analytical views over the internal Docker network. The browser reaches a loopback HTTP endpoint; PostgreSQL remains unexposed. No paid cloud or Tableau service is required for this local demo.

This choice replaces a blocked Tableau authoring/private-connectivity dependency with a self-hosted BI application. It does not change the meaning of event time, checkpoints, idempotency, finality or visible refresh. Automatic dashboard refresh is query polling; do not describe it as server-pushed streaming.

The owner authorized commits and pushes to the `dev` branch only. Main is not the development target, and version 0.1.0-dev.2 is not a production release.
