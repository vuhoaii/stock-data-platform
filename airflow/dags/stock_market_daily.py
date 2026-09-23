from datetime import timedelta

import pendulum

from airflow.sdk import DAG

from airflow.providers.standard.operators.bash import (
    BashOperator,
)


PROJECT_DIR = "/opt/airflow/project"

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
            minutes=2
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

    dag_id="stock_market_daily",

    description=(
        "Daily multi-source Vietnam "
        "stock market pipeline"
    ),

    start_date=pendulum.datetime(
        2026,
        9,
        21,
        tz=VN_TIMEZONE,
    ),

    schedule=(
        "30 17 * * 1-5"
    ),

    catchup=False,

    max_active_runs=1,

    default_args=default_args,

    tags=[
        "stock",
        "data-engineering",
        "multi-source",
        "monitoring",
        "audit",
    ],

) as dag:

    # ========================================================
    # AUDIT START
    # ========================================================

    audit_start = bash_task(

        "audit_start",

        """
        python src/utils/pipeline_audit.py \
        start \
        --pipeline stock_market_daily \
        --run-id "{{ run_id }}" \
        --message "Airflow market pipeline started"
        """,
    )


    # ========================================================
    # CLEAN STAGING
    # ========================================================

    clean_old_staging = bash_task(

        "clean_old_incremental_staging",

        """
        rm -f \
        data/staging/multisource_ohlcv_incremental.parquet

        echo "Old incremental staging removed."
        """,
    )


    # ========================================================
    # VCI
    # ========================================================

    ingest_vci = bash_task(

        "ingest_vci_incremental",

        """
        python \
        src/ingestion/incremental_ohlcv.py
        """,
    )


    # ========================================================
    # KBS
    # ========================================================

    ingest_kbs = bash_task(

        "ingest_kbs_incremental",

        """
        python \
        src/ingestion/incremental_kbs_ohlcv.py
        """,
    )


    # ========================================================
    # INCREMENTAL STAGING
    # ========================================================

    build_incremental = bash_task(

        "build_multisource_incremental_staging",

        """
        python \
        src/transformation/build_multisource_incremental_staging.py
        """,
    )


    # ========================================================
    # LOAD WAREHOUSE
    # ========================================================

    load_warehouse = bash_task(

        "load_incremental_warehouse",

        """
        FILE="data/staging/multisource_ohlcv_incremental.parquet"

        if [ -f "$FILE" ]; then

            python \
            src/transformation/load_multisource_warehouse.py \
            "$FILE"

        else

            echo \
            "No incremental data. Load skipped."

        fi
        """,
    )


    # ========================================================
    # FRESHNESS GATE
    # ========================================================

    source_freshness = bash_task(

        "check_source_freshness",

        """
        python \
        src/quality/check_source_freshness.py
        """,
    )


    # ========================================================
    # FULL STAGING
    # ========================================================

    rebuild_full_staging = bash_task(

        "rebuild_full_multisource_staging",

        """
        python \
        src/transformation/build_multisource_staging.py
        """,
    )


    # ========================================================
    # RECONCILIATION
    # ========================================================

    reconcile_sources = bash_task(

        "reconcile_vci_vs_kbs",

        """
        python \
        src/quality/compare_sources.py
        """,
    )


    # ========================================================
    # QUALITY
    # ========================================================

    warehouse_quality = bash_task(

        "warehouse_data_quality",

        """
        python \
        src/quality/check_warehouse.py
        """,
    )


    # ========================================================
    # MARTS
    # ========================================================

    refresh_marts = bash_task(

        "refresh_analytics_marts",

        """
        python \
        src/transformation/refresh_marts.py
        """,
    )


    # ========================================================
    # AUDIT SUCCESS
    # ========================================================

    audit_success = bash_task(

        "audit_success",

        """
        python src/utils/pipeline_audit.py \
        success \
        --pipeline stock_market_daily \
        --run-id "{{ run_id }}" \
        --message "Market pipeline completed successfully"
        """,
    )


    # ========================================================
    # AUDIT FAILURE
    # ========================================================

    audit_failure = bash_task(

        "audit_failure",

        """
        python src/utils/pipeline_audit.py \
        fail \
        --pipeline stock_market_daily \
        --run-id "{{ run_id }}" \
        --message "One or more Airflow tasks failed"
        """,

        trigger_rule="one_failed",
    )


    # ========================================================
    # NORMAL PIPELINE
    # ========================================================

    (
        audit_start
        >> clean_old_staging
    )

    clean_old_staging >> [
        ingest_vci,
        ingest_kbs,
    ]

    [
        ingest_vci,
        ingest_kbs,
    ] >> build_incremental

    (
        build_incremental
        >> load_warehouse
        >> source_freshness
        >> rebuild_full_staging
        >> reconcile_sources
        >> warehouse_quality
        >> refresh_marts
        >> audit_success
    )


    # ========================================================
    # FAILURE WATCH
    # ========================================================

    watched_tasks = [

        audit_start,

        clean_old_staging,

        ingest_vci,

        ingest_kbs,

        build_incremental,

        load_warehouse,

        source_freshness,

        rebuild_full_staging,

        reconcile_sources,

        warehouse_quality,

        refresh_marts,
    ]

    for task in watched_tasks:

        task >> audit_failure