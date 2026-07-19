"""
DAG 01 — Hello World: BashOperator, PythonOperator, XCom démonstration.

Objectif pédagogique :
  • Vérifier que l'environnement Airflow fonctionne
  • Pousser / tirer des valeurs via XCom (push explicite + return implicite)
  • Utiliser Jinja templates dans un BashOperator
"""

import random
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

# ── Default args ─────────────────────────────────────────────────────────────
default_args = {
    "owner": "abdelhakim",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


# ── Task callables ───────────────────────────────────────────────────────────
def generate_pipeline_info(ti, **kwargs):
    """Push pipeline metadata to XCom — both explicit and implicit."""
    record_count = random.randint(100, 500)

    ti.xcom_push(key="pipeline_name", value="ecommerce_transactions")
    ti.xcom_push(key="record_count", value=record_count)

    run_timestamp = datetime.utcnow().isoformat()
    print(f"📊 Pipeline info generated — records: {record_count}, ts: {run_timestamp}")

    # Implicit XCom via return → stored under key 'return_value'
    return run_timestamp


def consume_pipeline_info(ti, **kwargs):
    """Pull all 3 XCom values and validate them."""
    pipeline_name = ti.xcom_pull(task_ids="generate_pipeline_info", key="pipeline_name")
    record_count = ti.xcom_pull(task_ids="generate_pipeline_info", key="record_count")
    run_timestamp = ti.xcom_pull(task_ids="generate_pipeline_info", key="return_value")

    print(f"🔗 Pipeline  : {pipeline_name}")
    print(f"🔗 Records   : {record_count}")
    print(f"🔗 Timestamp : {run_timestamp}")

    assert 100 <= record_count <= 500, (
        f"record_count {record_count} is outside the expected range [100, 500]"
    )
    print("✅ All XCom values consumed and validated successfully.")


# ── DAG definition ───────────────────────────────────────────────────────────
with DAG(
    dag_id="dag_01_hello_world",
    default_args=default_args,
    description="BashOperator + PythonOperator + XCom demo",
    start_date=datetime(2026, 6, 1),
    schedule="@daily",
    catchup=False,
    tags=["learning", "basics"],
) as dag:

    check_environment = BashOperator(
        task_id="check_environment",
        bash_command=(
            'echo "=== Environment Check ===" && '
            "python --version && "
            "airflow version && "
            'echo "ENV OK"'
        ),
    )

    gen_info = PythonOperator(
        task_id="generate_pipeline_info",
        python_callable=generate_pipeline_info,
    )

    consume_info = PythonOperator(
        task_id="consume_pipeline_info",
        python_callable=consume_pipeline_info,
    )

    pipeline_summary = BashOperator(
        task_id="pipeline_summary",
        bash_command='echo "DAG run date: {{ ds }} | Run ID: {{ run_id }}"',
    )

    # ── Dependencies ─────────────────────────────────────────────────────────
    check_environment >> gen_info >> consume_info >> pipeline_summary