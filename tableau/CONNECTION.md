# Tableau feasibility and connection setup

**Current mode: Tableau-ready delivery only. No live connection, authoring session, workbook, published dashboard, or Tableau screen validation is available.** Public CSV snapshot export is implemented but no runtime-generated export is claimed until the cloud stack runs. Specifications are not completed dashboards.

## Mode 1 — existing Cloud/Server access

Required: an existing authorized site, authoring/publishing permissions, a PostgreSQL connector/driver, and a supported private connectivity route. Use a configured Tableau Bridge client/pool for private data where supported. PostgreSQL is reachable as `postgres:5432` only inside the Compose network; that hostname is not a Cloud connection endpoint. Have the site/network administrator supply the private database hostname reachable from the authorized Bridge/Server host, TLS/CA details and a least-privilege `tableau_reader` login. Keep credentials in the Tableau connection's supported credential mechanism, not workbook source, this repository or browser code.

Database `telemetry`; schema `mart`; choose **Live**. Start with base views in `dashboard-spec.md`, or their `tableau_*` dbt aliases after `make dbt`. These are SQL views, so scheduled historical dbt builds are not required for the live path. Do not expose port 5432 publicly. Codespaces HTTP port forwarding is not a PostgreSQL TCP route. This repository does not pretend to configure a private Bridge route automatically.

Author sheets and dashboards exactly as specified. Publish only after connection, permissions and private networking have been verified. For live proof, record a new event ID/player result in PostgreSQL, query-refresh Tableau, and verify the same result and processed timestamp. Then inject `latency-spike`, verify the incident on Server Health, and record backend delay separately from visible refresh delay.

## Mode 2 — Desktop Free Edition (requires explicit acceptance)

Official documentation lists live database access, but this is a desktop viewing application with different publishing capabilities. The user has not accepted this route. It cannot silently fulfill cloud/browser-only delivery. If explicitly accepted later, configure a supported authenticated private connection and use the Tableau PostgreSQL driver. Demonstrate supported manual refresh. Do not call a live connection automatic browser refresh.

## Mode 3 — Tableau Public snapshot

Run `make export-tableau` after a verified cloud run. Download the CSVs from a single timestamped directory. In Tableau Public's supported web authoring or Public Edition, connect to each CSV separately and build the specification. Display `SNAPSHOT — generated <UTC timestamp>` in every dashboard. Gameplay/server data are synthetic; pipeline data are measured. Public publication requires an authenticated account and user authorization. Public does not publish live PostgreSQL database connections. Re-exporting CSV files is not a real-time published dashboard.

## Embedding gate

No embedding app is implemented because an authorized Cloud/Server site and authentication context are absent. If available later, use Embedding API v3 `TableauViz.refreshDataAsync()` to request a query refresh, await its promise, allow one request in flight, start at 15 seconds, back off failures to 60 seconds, pause when the document is hidden, and stop on disposal. Verify the method against the target site's supported API version. Use an authenticated Tableau session or supported connected-app/EAS flow; generate required signed tokens in a trusted backend. Never ship a PAT, signing secret or database password to the browser. Record successful refresh completion separately from the source's processed timestamp; neither proves another viewer rendered the result.

## Official references checked 2026-09-28

- [Edition comparison](https://help.tableau.com/current/pro/desktop/en-us/desktop_comparison.htm)
- [Tableau Public FAQ](https://help.tableau.com/current/pro/desktop/en-us/public_faq.htm)
- [Bridge live connection publishing](https://help.tableau.com/current/online/en-us/to_bridge_livequery.htm)
- [Embedding authentication](https://help.tableau.com/current/api/embedding_api/en-us/docs/embedding_api_auth.html)
- [Embedding API release notes](https://help.tableau.com/current/api/embedding_api/en-us/docs/embedding_api_release_notes.html)

Smallest live setup requirement: provide an existing Tableau Cloud/Server site with authoring rights and a supported authorized private PostgreSQL connectivity route. Snapshot authoring only needs an accessible authenticated Tableau Public authoring session once exports exist.
