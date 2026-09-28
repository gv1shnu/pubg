#!/bin/sh
set -eu
superset db upgrade
superset init
python /app/bootstrap.py
exec gunicorn --bind 0.0.0.0:8088 --workers 2 --threads 4 --timeout 120 'superset.app:create_app()'
