# Superset dashboards

Apache Superset 5.0.0 is the selected BI surface. It runs inside the same private Docker network as PostgreSQL and reads the existing mart views through a read-only role. No Tableau subscription, Bridge installation or public database endpoint is required.

`make bi-up` builds and initializes Superset, creates an administrator using the ignored `.env` password, introspects virtual SQL datasets and idempotently authors four real dashboards with 24 charts:

- `/superset/dashboard/live-leaderboard/`
- `/superset/dashboard/match-operations/`
- `/superset/dashboard/server-health/`
- `/superset/dashboard/pipeline-reliability/`

Open http://localhost:8088 and sign in as `admin`; the password is `SUPERSET_ADMIN_PASSWORD` in the local `.env`. Never commit this file. Dashboards marked published are available to authenticated users, not anonymously shared. HTTP binds only to loopback. Superset metadata has its own PostgreSQL database/role; read-only analytical credentials stay on the server.

The first three dashboards default to one run selected at authoring time. Use the single-select Run ID filter for another run. Re-running the idempotent authoring script updates defaults to the newest run. Pipeline metrics intentionally have installation scope. Refresh is configured at 15 seconds; actual browser/query timing must be measured separately from backend processing delay.

`bootstrap.py` is the reproducible dashboard artifact, executed by the compatible Superset application rather than pretending arbitrary workbook XML is valid. It creates actual dataset, chart and dashboard metadata. `start.sh` migrates the metadata database before authoring. The checked-in configuration disables query-result caching for the small live demo and uses synchronous queries; no extra Redis/Celery services are added.

The dark styling and amber palette are original. Gameplay and server data are explicitly synthetic. Source timestamps and all axes use UTC. Project score is not an official game ranking. The SQL leaderboard grain is one player-match; filter to one match to interpret its precomputed rank. CPU and tick charts summarize exact server/window marks; p95 values are not averaged across windows. Server session rows use DISTINCT to avoid repeating current counts over historical windows.

This is a development deployment. It uses a fixed pinned release, simple in-process filter-state caching, synchronous queries and two web workers. Production security, scaling, SSO, public sharing and high availability are outside this release. See the current acceptance evidence before claiming browser validation or a performance target.

## Native saved assets

`dashboards.zip` is a real export from the verified running Superset instance. `assets/` contains the same YAML for readable diffs. Database passwords were replaced with `XXXXXXXXXX` before inclusion; importing requires supplying the destination database password. Run-specific default filters reference the recorded demo. `make bi-up` is the preferred fresh-install route because bootstrap authors against the current database and newest run automatically.
