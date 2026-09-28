# Snapshot export destination

`make export-tableau` retains its legacy command name and writes generic CSV snapshots to the Docker export volume. Run `make export-artifacts` to copy timestamped directories here. Exports use a repeatable-read PostgreSQL snapshot; every CSV includes `snapshot_generated_at`, and the manifest records run, namespace and row counts. Pipeline metrics have installation scope.

The Airflow acceptance run produced a real snapshot on 2026-09-28. Generated data is ignored by Git. Superset reads live SQL and does not require these CSVs.
