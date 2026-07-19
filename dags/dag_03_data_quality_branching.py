"""
DAG 03 — Data Quality Branching with BranchPythonOperator.

Pipeline : Run quality checks → Branch decision →
           [load_to_warehouse | trigger_alert] → Final report → End

A 40 % simulated failure rate forces the alert branch, demonstrating
how BranchPythonOperator skips downstream tasks dynamically.
"""

import json
import random
import uuid
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.utils.trigger_rule import TriggerRule

# ── Default args ─────────────────────────────────────────────────────────────
default_args = {
    "owner": "abdelhakim",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


# ── Task callables ───────────────────────────────────────────────────────────
def run_quality_checks(ti, **kwargs):
    """Generate mock transactions and produce a quality report."""
    num_rows = random.randint(30, 150)
    transactions = []

    for _ in range(num_rows):
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

    montants = [t["montant"] for t in transactions]
    anomaly_count = sum(1 for m in montants if m > 400)
    success_count = sum(1 for t in transactions if t["statut"] == "SUCCESS")

    # ── Simulate occasional quality failure (40 % chance) ────────────────
    if random.random() < 0.4:
        num_rows = 5  # Forces volume check failure in branching

    report = {
        "total_rows": num_rows,
        "null_pct": 0.0,
        "anomaly_count": anomaly_count,
        "anomaly_rate_pct": round(anomaly_count / len(transactions) * 100, 2),
        "success_rate_pct": round(success_count / len(transactions) * 100, 2),
        "avg_montant": round(sum(montants) / len(montants), 2),
    }

    print(f"📋 Quality report: {json.dumps(report, indent=2)}")
    ti.xcom_push(key="quality_report", value=report)


def decide_branch(ti, **kwargs):
    """Route pipeline based on quality thresholds."""
    report = ti.xcom_pull(task_ids="run_quality_checks", key="quality_report")

    total_rows = report["total_rows"]
    anomaly_rate = report["anomaly_rate_pct"]

    print(f"🔀 Evaluating: total_rows={total_rows}, anomaly_rate={anomaly_rate}%")

    if total_rows >= 20 and anomaly_rate < 20.0:
        print("→ Branch: load_to_warehouse")
        return "load_to_warehouse"

    print("→ Branch: trigger_alert")
    return "trigger_alert"


def load_to_warehouse(ti, **kwargs):
    """Simulate loading validated data into the warehouse."""
    ds = kwargs["ds"]
    report = ti.xcom_pull(task_ids="run_quality_checks", key="quality_report")

    print("=" * 50)
    print("   ✅ LOADING TO WAREHOUSE")
    print("=" * 50)
    print(f"   Rows loaded       : {report['total_rows']}")
    print(f"   Anomaly rate      : {report['anomaly_rate_pct']}%")
    print(f"   Success rate      : {report['success_rate_pct']}%")
    print(f"   Avg montant       : {report['avg_montant']} €")
    print("=" * 50)

    summary = {
        "status": "LOADED",
        "ds": ds,
        "report": report,
        "loaded_at": datetime.utcnow().isoformat(),
    }
    output_path = f"/tmp/warehouse_load_{ds}.json"
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(summary, f, ensure_ascii=False, indent=2)

    print(f"📦 Summary saved → {output_path}")


def trigger_alert(ti, **kwargs):
    """Generate and log a quality alert."""
    ds = kwargs["ds"]
    report = ti.xcom_pull(task_ids="run_quality_checks", key="quality_report")

    severity = "HIGH" if report["total_rows"] < 10 else "MEDIUM"

    alert = {
        "severity": severity,
        "ds": ds,
        "reason": [],
        "report": report,
        "alerted_at": datetime.utcnow().isoformat(),
    }

    if report["total_rows"] < 20:
        alert["reason"].append(f"Low volume: {report['total_rows']} rows (min 20)")
    if report["anomaly_rate_pct"] >= 20.0:
        alert["reason"].append(
            f"High anomaly rate: {report['anomaly_rate_pct']}% (max 20%)"
        )

    print("=" * 50)
    print(f"   🚨 QUALITY ALERT — Severity: {severity}")
    print("=" * 50)
    for reason in alert["reason"]:
        print(f"   • {reason}")
    print(f"   Total rows    : {report['total_rows']}")
    print(f"   Anomaly rate  : {report['anomaly_rate_pct']}%")
    print(f"   Success rate  : {report['success_rate_pct']}%")
    print("=" * 50)

    output_path = f"/tmp/alert_{ds}.json"
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(alert, f, ensure_ascii=False, indent=2)

    print(f"📄 Alert saved → {output_path}")


def final_report(ti, **kwargs):
    """Print a final summary regardless of which branch was taken."""
    report = ti.xcom_pull(task_ids="run_quality_checks", key="quality_report")

    # Determine which branch ran by checking if its XCom exists
    warehouse_result = ti.xcom_pull(task_ids="load_to_warehouse", key="return_value")
    alert_result = ti.xcom_pull(task_ids="trigger_alert", key="return_value")

    if warehouse_result is not None:
        branch_taken = "load_to_warehouse ✅"
    elif alert_result is not None:
        branch_taken = "trigger_alert 🚨"
    else:
        # Fallback: infer from report thresholds
        if report["total_rows"] >= 20 and report["anomaly_rate_pct"] < 20.0:
            branch_taken = "load_to_warehouse ✅"
        else:
            branch_taken = "trigger_alert 🚨"

    print("=" * 50)
    print("   📋 FINAL REPORT")
    print("=" * 50)
    print(f"   Branch taken  : {branch_taken}")
    print(f"   Total rows    : {report['total_rows']}")
    print(f"   Anomaly rate  : {report['anomaly_rate_pct']}%")
    print(f"   Success rate  : {report['success_rate_pct']}%")
    print(f"   Avg montant   : {report['avg_montant']} €")
    print("=" * 50)


# ── DAG definition ───────────────────────────────────────────────────────────
with DAG(
    dag_id="dag_03_branching_quality",
    default_args=default_args,
    description="Data quality branching with BranchPythonOperator",
    start_date=datetime(2026, 6, 1),
    schedule="@daily",
    catchup=False,
    tags=["data-quality", "branching"],
) as dag:

    task_checks = PythonOperator(
        task_id="run_quality_checks",
        python_callable=run_quality_checks,
    )

    task_branch = BranchPythonOperator(
        task_id="decide_branch",
        python_callable=decide_branch,
    )

    task_load = PythonOperator(
        task_id="load_to_warehouse",
        python_callable=load_to_warehouse,
    )

    task_alert = PythonOperator(
        task_id="trigger_alert",
        python_callable=trigger_alert,
    )

    task_final = PythonOperator(
        task_id="final_report",
        python_callable=final_report,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    task_end = EmptyOperator(
        task_id="end",
        trigger_rule=TriggerRule.ALL_DONE,
    )

    # ── Dependencies ─────────────────────────────────────────────────────
    task_checks >> task_branch
    task_branch >> [task_load, task_alert]
    [task_load, task_alert] >> task_final >> task_end
