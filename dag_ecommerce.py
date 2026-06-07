"""
E-Commerce Data Pipeline — Airflow DAG
=======================================
Orchestrates ingestion → cleaning → dbt → DQ checks with Slack alerting.

Schedule: daily at 06:00 BRT (09:00 UTC).
SLA: Silver layer ready by 07:30 BRT.
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator

# ── Default args ──────────────────────────────────────────────────────────────
DEFAULT_ARGS = {
    "owner": "li-barros",
    "depends_on_past": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=60),
    "email_on_failure": True,
    "email": ["data-alerts@company.io"],
    "sla": timedelta(hours=1, minutes=30),
}

SLACK_CONN_ID = "slack_data_alerts"


# ── Callable helpers ──────────────────────────────────────────────────────────
def _run_ingestion(**ctx) -> dict:
    """Trigger ApiIngestionClient for yesterday's data."""
    from ingestion.api_ingestion import ApiIngestionClient, IngestionConfig

    ds = ctx["ds"]  # YYYY-MM-DD from Airflow execution date
    config = IngestionConfig()
    client = ApiIngestionClient(config)
    result = client.run_full_load(date_from=ds, date_to=ds)

    # Push stats to XCom for downstream tasks
    ctx["ti"].xcom_push(key="ingestion_stats", value=result["stats"])
    return result


def _run_cleaning(**ctx) -> dict:
    """Clean all bronze files produced in this run."""
    import json
    from pathlib import Path
    from cleaning.data_cleaning import OrdersCleaner, CleaningConfig

    ds = ctx["ds"]
    config = CleaningConfig()
    cleaner = OrdersCleaner(config)

    bronze_files = sorted(
        Path(config.bronze_path / "orders").rglob(f"*{ds}*.ndjson")
    )
    reports = []
    for f in bronze_files:
        _, report = cleaner.clean(f)
        reports.append(report.to_dict())

    ctx["ti"].xcom_push(key="dq_reports", value=reports)
    return {"cleaned_files": len(reports), "reports": reports}


def _check_dq_pass(**ctx) -> str:
    """Branch: proceed to Gold or route to alert."""
    reports = ctx["ti"].xcom_pull(key="dq_reports") or []
    all_pass = all(r.get("pass_rate_pct", 0) >= 90 for r in reports)
    return "run_dbt_models" if all_pass else "slack_dq_alert"


def _run_dbt(**ctx) -> None:
    """Execute dbt models via subprocess. Replace with DbtTaskGroup in prod."""
    import subprocess
    result = subprocess.run(
        ["dbt", "run", "--select", "silver+ gold+", "--profiles-dir", "/opt/dbt"],
        capture_output=True, text=True, check=True,
    )
    print(result.stdout)


def _run_dbt_tests(**ctx) -> None:
    import subprocess
    subprocess.run(
        ["dbt", "test", "--select", "silver+ gold+"],
        capture_output=True, text=True, check=True,
    )


def _build_slack_summary(**ctx) -> str:
    stats = ctx["ti"].xcom_pull(key="ingestion_stats") or {}
    reports = ctx["ti"].xcom_pull(key="dq_reports") or []
    pass_rates = [r.get("pass_rate_pct", 0) for r in reports]
    avg_pass = round(sum(pass_rates) / len(pass_rates), 1) if pass_rates else 0
    return (
        f":white_check_mark: *Pipeline OK* — `{ctx['ds']}`\n"
        f"• Ingested: {stats.get('records', '?')} records "
        f"({stats.get('dlq', 0)} DLQ)\n"
        f"• DQ pass rate: {avg_pass}%\n"
        f"• Retries: {stats.get('retried', 0)}"
    )


# ── DAG ────────────────────────────────────────────────────────────────────────
with DAG(
    dag_id="ecommerce_data_pipeline",
    description="Daily e-commerce ingestion → Bronze → Silver → Gold",
    schedule_interval="0 9 * * *",  # 09:00 UTC = 06:00 BRT
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["ecommerce", "ingestion", "dbt", "silver", "gold"],
    doc_md=__doc__,
) as dag:

    start = EmptyOperator(task_id="start")

    ingest = PythonOperator(
        task_id="run_ingestion",
        python_callable=_run_ingestion,
    )

    clean = PythonOperator(
        task_id="run_cleaning",
        python_callable=_run_cleaning,
    )

    dq_branch = BranchPythonOperator(
        task_id="check_dq_pass",
        python_callable=_check_dq_pass,
    )

    dbt_run = PythonOperator(
        task_id="run_dbt_models",
        python_callable=_run_dbt,
    )

    dbt_test = PythonOperator(
        task_id="run_dbt_tests",
        python_callable=_run_dbt_tests,
    )

    slack_dq_alert = SlackWebhookOperator(
        task_id="slack_dq_alert",
        slack_webhook_conn_id=SLACK_CONN_ID,
        message=":warning: *DQ check failed* — Silver pass rate below 90%. Pipeline halted.",
        trigger_rule="none_failed_min_one_success",
    )

    slack_success = SlackWebhookOperator(
        task_id="slack_success",
        slack_webhook_conn_id=SLACK_CONN_ID,
        message="{{ task_instance.xcom_pull(task_ids='check_dq_pass') }}",
        trigger_rule="none_failed_min_one_success",
    )

    end = EmptyOperator(task_id="end", trigger_rule="none_failed_min_one_success")

    # ── Dependencies ──────────────────────────────────────────────────────────
    start >> ingest >> clean >> dq_branch
    dq_branch >> dbt_run >> dbt_test >> slack_success >> end
    dq_branch >> slack_dq_alert >> end
