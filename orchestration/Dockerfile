FROM apache/airflow:2.10.5-python3.11
USER root
RUN python -m venv /opt/dbt && /opt/dbt/bin/pip install --no-cache-dir dbt-core==1.9.3 dbt-postgres==1.9.0 dbt-adapters==1.16.3 dbt-common==1.27.1
USER airflow
RUN pip install --no-cache-dir apache-airflow==2.10.5 pyarrow==19.0.1 'psycopg[binary]==3.2.5' confluent-kafka==2.8.2
WORKDIR /app
COPY --chown=airflow:root . /app
ENV PYTHONPATH=/app DBT_PROFILES_DIR=/app/dbt DBT_EXECUTABLE=/opt/dbt/bin/dbt
