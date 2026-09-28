# Snapshot export destination

`make export-tableau` writes a timestamped directory here from a repeatable-read PostgreSQL snapshot. Every CSV includes `snapshot_generated_at`; the manifest records run, namespace and row counts. The live sources are read-only views. Pipeline metrics are installation-wide and are not joined to player rows.

No exported CSVs are supplied yet: cloud execution was unavailable. Empty placeholder data must not be presented as a validated snapshot. Generated files are ignored by Git.
