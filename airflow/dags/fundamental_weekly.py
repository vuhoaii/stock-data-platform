from datetime import timedelta

import pendulum

from airflow.sdk import DAG

from airflow.providers.standard.operators.bash import (
    BashOperator,
)


PROJECT_DIR = (
    "/opt/airflow/project"
)

VN_TIMEZONE = (
    "Asia/Ho_Chi_Minh"
)


default_args = {

    "owner":
        "stock-data-platform",

    "retries":
        2,

    "retry_delay":
        timedelta(
            minutes=3
        ),
}


def bash_task(
    task_id,
    command,
    trigger_rule="all_success",
):

    return BashOperator(

        task_id=task_id,

        cwd=PROJECT_DIR,

        bash_command=(
            "set -e\n"
            + command
        ),

        trigger_rule=trigger_rule,
    )


with DAG(

    dag_id="fundamental_weekly",

    description=(
        "Weekly fundamental "
        "financial data pipeline"
    ),

    start_date=pendulum.datetime(
        2026,
        9,
        21,
        tz=VN_TIMEZONE,
    ),

    schedule="0 9 * * 0",

    catchup=False,

    max_active_runs=1,

    default_args=default_args,

    tags=[
        "stock",
        "fundamental",
        "financial",
        "audit",
    ],

) as dag:

    audit_start = bash_task(

        "audit_start",

        """
        python src/utils/pipeline_audit.py \
        start \
        --pipeline fundamental_weekly \
        --run-id "{{ run_id }}" \
        --message "Fundamental pipeline started"
        """,
    )


    ingest = bash_task(

        "ingest_fundamental_finance",

        """
        python \
        src/ingestion/fundamental_finance.py
        """,
    )


    staging = bash_task(

        "build_fundamental_staging",

        """
        python \
        src/transformation/build_fundamental_staging.py
        """,
    )


    load = bash_task(

        "load_fundamental_warehouse",

        """
        python \
        src/transformation/load_fundamental_warehouse.py
        """,
    )


    quality = bash_task(

        "fundamental_data_quality",

        """
        python \
        src/quality/check_fundamental_warehouse.py
        """,
    )


    refresh = bash_task(

        "refresh_financial_mart",

        """
        python \
        src/transformation/refresh_financial_mart.py
        """,
    )


    audit_success = bash_task(

        "audit_success",

        """
        python src/utils/pipeline_audit.py \
        success \
        --pipeline fundamental_weekly \
        --run-id "{{ run_id }}" \
        --message "Fundamental pipeline completed successfully"
        """,
    )


    audit_failure = bash_task(

        "audit_failure",

        """
        python src/utils/pipeline_audit.py \
        fail \
        --pipeline fundamental_weekly \
        --run-id "{{ run_id }}" \
        --message "One or more fundamental tasks failed"
        """,

        trigger_rule="one_failed",
    )


    (
        audit_start
        >> ingest
        >> staging
        >> load
        >> quality
        >> refresh
        >> audit_success
    )


    watched_tasks = [

        audit_start,
        ingest,
        staging,
        load,
        quality,
        refresh,
    ]


    for task in watched_tasks:

        task >> audit_failure