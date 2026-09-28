import os
SECRET_KEY = os.environ['SUPERSET_SECRET_KEY']
SQLALCHEMY_DATABASE_URI = 'postgresql+psycopg2://superset_meta:' + os.environ['SUPERSET_DB_PASSWORD'] + '@postgres/superset'
WTF_CSRF_ENABLED = True
SESSION_COOKIE_HTTPONLY = True
SESSION_COOKIE_SAMESITE = 'Lax'
FEATURE_FLAGS = {'DASHBOARD_NATIVE_FILTERS': True}
ENABLE_PROXY_FIX = True
PUBLIC_ROLE_LIKE = None
TALISMAN_ENABLED = False  # Loopback-only HTTP demo; use TLS and a CSP when deploying beyond localhost.
SQLLAB_ASYNC_TIME_LIMIT_SEC = 30
ROW_LIMIT = 10000
WEBSERVER_TIMEOUT = 60
DATA_CACHE_CONFIG = {'CACHE_TYPE': 'NullCache'}
FILTER_STATE_CACHE_CONFIG = {'CACHE_TYPE': 'SimpleCache', 'CACHE_DEFAULT_TIMEOUT': 300}
EXPLORE_FORM_DATA_CACHE_CONFIG = {'CACHE_TYPE': 'SimpleCache', 'CACHE_DEFAULT_TIMEOUT': 300}
APP_NAME = 'Battleground Telemetry'
EXTRA_CATEGORICAL_COLOR_SCHEMES = [{
    'id': 'battleground', 'label': 'Battleground amber',
    'colors': ['#E8B45B', '#69B9C9', '#E36B67', '#8EBE86', '#BAA6D9'],
}]
