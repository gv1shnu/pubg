FROM postgres:16.8-bookworm
COPY warehouse /warehouse
COPY warehouse/init.sh /docker-entrypoint-initdb.d/00-init.sh
