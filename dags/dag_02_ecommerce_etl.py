"""
DAG 02 — E-commerce ETL Pipeline with Quality Gate.

Pipeline : Extract (generate mock data) → Transform (pandas) →
           Quality Gate → Load Summary → Cleanup
"""

import json
import os
import random
import uuid
from datetime import datetime, timedelta

import pandas as pd
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
def extract(**context):
    """Generate 50 mock e-commerce transactions and save to JSON."""
    ds = context["ds"]
    transactions = []

    for _ in range(50):
        transactions.append(
            {
                "id_transaction": str(uuid.uuid4()),
                "user_id": random.randint(1, 100),
                "montant": round(random.uniform(10.0, 500.0), 2),
                "methode_paiement": random.choice(
                    ["Carte Bancaire", "PayPal", "Virement"]
                ),
                "statut": random.choices(
                    ["SUCCESS", "FAILED"], weights=[0.9, 0.1]
                )[0],
                "event_timestamp": datetime.utcnow().isoformat(),
            }
        )

    output_path = f"/tmp/raw_transactions_{ds}.json"
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(transactions, f, ensure_ascii=False, indent=2)

    print(f"✅ Extracted {len(transactions)} transactions → {output_path}")

    ti = context["ti"]
    ti.xcom_push(key="raw_path", value=output_path)
    return output_path


def transform(**context):
    """Read raw JSON, add computed columns, save as clean CSV."""
    ti = context["ti"]
    ds = context["ds"]
    raw_path = ti.xcom_pull(task_ids="extract", key="raw_path")

    df = pd.read_json(raw_path)

    # ── Transformations ──────────────────────────────────────────────────
    df["montant_ttc"] = (df["montant"] * 1.20).round(2)
    df["is_high_value"] = df["montant"] > 300
    df["processed_at"] = datetime.utcnow().isoformat()

    output_path = f"/tmp/clean_transactions_{ds}.csv"
    df.to_csv(output_path, index=False)

    row_count = len(df)
    print(f"✅ Transformed {row_count} rows → {output_path}")
    print(f"   High-value transactions: {df['is_high_value'].sum()}")

    ti.xcom_push(key="output_path", value=output_path)
    ti.xcom_push(key="row_count", value=row_count)


def quality_gate(**context):
    """Validate the clean dataset against business rules."""
    ti = context["ti"]
    csv_path = ti.xcom_pull(task_ids="transform", key="output_path")
    row_count = ti.xcom_pull(task_ids="transform", key="row_count")

    df = pd.read_csv(csv_path)

    failed_rules = []

    # Rule 1 — Minimum volume
    if row_count < 10:
        failed_rules.append(f"VOLUME: row_count={row_count} < 10")
        print("❌ FAIL — Volume check: too few rows")
    else:
        print(f"✅ PASS — Volume check: {row_count} rows")

    # Rule 2 — No null montant
    null_montant = int(df["montant"].isna().sum())
    if null_montant != 0:
        failed_rules.append(f"NULLS: {null_montant} null values in 'montant'")
        print(f"❌ FAIL — Null check: {null_montant} nulls in montant")
    else:
        print("✅ PASS — Null check: no nulls in montant")

    # Rule 3 — Valid statut values
    valid_statut = df["statut"].isin(["SUCCESS", "FAILED"]).all()
    if not valid_statut:
        invalid = df.loc[~df["statut"].isin(["SUCCESS", "FAILED"]), "statut"].unique()
        failed_rules.append(f"STATUT: invalid values found: {list(invalid)}")
        print(f"❌ FAIL — Statut check: invalid values {list(invalid)}")
    else:
        print("✅ PASS — Statut check: all values are SUCCESS or FAILED")

    if failed_rules:
        raise ValueError(
            f"Quality gate FAILED — {len(failed_rules)} rule(s) violated:\n"
            + "\n".join(f"  • {r}" for r in failed_rules)
        )

    print("🏁 Quality gate PASSED — all rules satisfied.")


def load_summary(**context):
    """Compute and display summary metrics from the clean dataset."""
    ti = context["ti"]
    csv_path = ti.xcom_pull(task_ids="transform", key="output_path")

    df = pd.read_csv(csv_path)

    total_revenue = round(float(df["montant"].sum()), 2)
    avg_montant = round(float(df["montant"].mean()), 2)
    total = len(df)
    success_count = int((df["statut"] == "SUCCESS").sum())
    success_rate = round(success_count / total * 100, 1) if total else 0.0
    top_method = df["methode_paiement"].value_counts().idxmax()

    metrics = {
        "total_revenue": total_revenue,
        "avg_montant": avg_montant,
        "success_rate_pct": success_rate,
        "top_payment_method": top_method,
        "total_transactions": total,
    }

    print("=" * 50)
    print("        📊 E-COMMERCE ETL — LOAD SUMMARY")
    print("=" * 50)
    print(f"  Total revenue      : {total_revenue:>10.2f} €")
    print(f"  Average montant    : {avg_montant:>10.2f} €")
    print(f"  Success rate       : {success_rate:>9.1f} %")
    print(f"  Top payment method : {top_method}")
    print(f"  Total transactions : {total}")
    print("=" * 50)

    ti.xcom_push(key="metrics", value=metrics)


# ── DAG definition ───────────────────────────────────────────────────────────
with DAG(
    dag_id="dag_02_ecommerce_etl",
    default_args=default_args,
    description="E-commerce ETL pipeline with quality gate",
    start_date=datetime(2026, 6, 1),
    schedule="@daily",
    catchup=False,
    tags=["etl", "ecommerce"],
) as dag:

    task_extract = PythonOperator(
        task_id="extract",
        python_callable=extract,
    )

    task_transform = PythonOperator(
        task_id="transform",
        python_callable=transform,
    )

    task_quality = PythonOperator(
        task_id="quality_gate",
        python_callable=quality_gate,
    )

    task_load = PythonOperator(
        task_id="load_summary",
        python_callable=load_summary,
    )

    task_cleanup = BashOperator(
        task_id="cleanup",
        bash_command=(
            'rm -f /tmp/raw_transactions_{{ ds }}.json && '
            'echo "Cleanup done for {{ ds }}"'
        ),
    )

    # ── Dependencies ─────────────────────────────────────────────────────
    task_extract >> task_transform >> task_quality >> task_load >> task_cleanup
