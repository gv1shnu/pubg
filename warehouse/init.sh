#!/bin/sh
set -eu
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" -f /warehouse/001_schema.sql -f /warehouse/002_roles.sql
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<'SQL'
\getenv airflow_password AIRFLOW_DB_PASSWORD
CREATE USER airflow_meta PASSWORD :'airflow_password';
CREATE DATABASE airflow OWNER airflow_meta;
\getenv superset_password SUPERSET_DB_PASSWORD
CREATE USER superset_meta PASSWORD :'superset_password';
CREATE DATABASE superset OWNER superset_meta;
SQL
