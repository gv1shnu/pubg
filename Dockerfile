FROM python:3.11.11-slim-bookworm
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt dbt-core==1.9.3 dbt-postgres==1.9.0
COPY . .
ENV PYTHONPATH=/app PYTHONUNBUFFERED=1 DBT_PROFILES_DIR=/app/dbt
CMD ["python", "-m", "simulator.main"]
