"""Manual bounded maintenance; Airflow never processes individual game events."""
from datetime import datetime, timezone
from airflow import DAG
from airflow.operators.python import PythonOperator
from scripts.ops import batch_step, connect

def step(action, run_id, **context):
    try:
        return batch_step(run_id,action)
    except Exception as error:
        with connect() as con:
            con.execute("UPDATE ops.batch_runs SET status='failed',finished_at=now(),details=details||%s::jsonb WHERE batch_id=%s",(__import__('json').dumps({'failure_type':type(error).__name__}),run_id))
        raise

with DAG('telemetry_maintenance',start_date=datetime(2025,1,1,tzinfo=timezone.utc),schedule=None,catchup=False,max_active_runs=1,
         default_args={'retries':1},tags=['telemetry','manual']) as dag:
    previous=None
    for action in ('start','archive','reconcile','dbt','export','finish'):
        task=PythonOperator(task_id=action,python_callable=step,op_kwargs={'action':action})
        if previous: previous >> task
        previous=task
