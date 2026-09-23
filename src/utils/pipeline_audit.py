import argparse
import os

import psycopg2
from dotenv import load_dotenv


load_dotenv(
    override=True
)


# ============================================================
# DATABASE
# ============================================================

def get_connection():

    return psycopg2.connect(
        host=os.getenv("DB_HOST"),
        port=os.getenv("DB_PORT"),
        database=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
    )


# ============================================================
# SNAPSHOT
# ============================================================

def get_pipeline_snapshot(
    cursor,
    pipeline_name,
):

    # ========================================================
    # MARKET PIPELINE
    # ========================================================

    if pipeline_name == "stock_market_daily":

        cursor.execute(
            """
            SELECT
                COUNT(*),
                MAX(d.full_date)

            FROM fact_stock_price f

            JOIN dim_date d
                ON f.date_key =
                   d.date_key;
            """
        )

        row_count, latest_date = (
            cursor.fetchone()
        )

        marker = (
            str(latest_date)
            if latest_date
            else None
        )

        return (
            row_count,
            marker,
        )

    # ========================================================
    # FUNDAMENTAL PIPELINE
    # ========================================================

    if pipeline_name == "fundamental_weekly":

        cursor.execute(
            """
            SELECT COUNT(*)
            FROM fact_financial_metric;
            """
        )

        row_count = (
            cursor.fetchone()[0]
        )

        cursor.execute(
            """
            SELECT
                p.period_label

            FROM dim_period p

            WHERE EXISTS (

                SELECT 1

                FROM fact_financial_metric f

                WHERE
                    f.period_key =
                    p.period_key
            )

            ORDER BY
                p.year DESC NULLS LAST,
                p.quarter DESC NULLS LAST,
                p.period_key DESC

            LIMIT 1;
            """
        )

        row = cursor.fetchone()

        marker = (
            row[0]
            if row
            else None
        )

        return (
            row_count,
            marker,
        )

    return (
        None,
        None,
    )


# ============================================================
# START RUN
# ============================================================

def start_run(
    pipeline_name,
    run_id,
    message,
):

    conn = get_connection()

    try:

        cursor = conn.cursor()

        cursor.execute(
            """
            INSERT INTO pipeline_run_audit (
                pipeline_name,
                run_id,
                started_at,
                finished_at,
                status,
                row_count,
                latest_data_marker,
                message
            )

            VALUES (
                %s,
                %s,
                CURRENT_TIMESTAMP,
                NULL,
                'RUNNING',
                NULL,
                NULL,
                %s
            )

            ON CONFLICT (
                pipeline_name,
                run_id
            )

            DO UPDATE SET

                started_at =
                    CURRENT_TIMESTAMP,

                finished_at =
                    NULL,

                status =
                    'RUNNING',

                row_count =
                    NULL,

                latest_data_marker =
                    NULL,

                message =
                    EXCLUDED.message;
            """,
            (
                pipeline_name,
                run_id,
                message,
            )
        )

        conn.commit()

        print(
            "========================================"
        )

        print(
            "      PIPELINE AUDIT START"
        )

        print(
            "========================================"
        )

        print(
            f"Pipeline: {pipeline_name}"
        )

        print(
            f"Run ID: {run_id}"
        )

        print(
            "Status: RUNNING"
        )

        cursor.close()

    except Exception:

        conn.rollback()

        raise

    finally:

        conn.close()


# ============================================================
# FINISH RUN
# ============================================================

def finish_run(
    pipeline_name,
    run_id,
    status,
    message,
):

    conn = get_connection()

    try:

        cursor = conn.cursor()

        row_count, marker = (
            get_pipeline_snapshot(
                cursor,
                pipeline_name,
            )
        )

        cursor.execute(
            """
            INSERT INTO pipeline_run_audit (
                pipeline_name,
                run_id,
                started_at,
                finished_at,
                status,
                row_count,
                latest_data_marker,
                message
            )

            VALUES (
                %s,
                %s,
                CURRENT_TIMESTAMP,
                CURRENT_TIMESTAMP,
                %s,
                %s,
                %s,
                %s
            )

            ON CONFLICT (
                pipeline_name,
                run_id
            )

            DO UPDATE SET

                finished_at =
                    CURRENT_TIMESTAMP,

                status =
                    EXCLUDED.status,

                row_count =
                    EXCLUDED.row_count,

                latest_data_marker =
                    EXCLUDED.latest_data_marker,

                message =
                    EXCLUDED.message;
            """,
            (
                pipeline_name,
                run_id,
                status,
                row_count,
                marker,
                message,
            )
        )

        conn.commit()

        print(
            "========================================"
        )

        print(
            "      PIPELINE AUDIT FINISH"
        )

        print(
            "========================================"
        )

        print(
            f"Pipeline: {pipeline_name}"
        )

        print(
            f"Run ID: {run_id}"
        )

        print(
            f"Status: {status}"
        )

        print(
            f"Warehouse rows: {row_count}"
        )

        print(
            f"Latest marker: {marker}"
        )

        cursor.close()

    except Exception:

        conn.rollback()

        raise

    finally:

        conn.close()


# ============================================================
# SHOW HISTORY
# ============================================================

def show_history(
    limit,
):

    conn = get_connection()

    try:

        cursor = conn.cursor()

        cursor.execute(
            """
            SELECT
                audit_id,
                pipeline_name,
                run_id,
                started_at,
                finished_at,
                status,
                row_count,
                latest_data_marker,
                message

            FROM pipeline_run_audit

            ORDER BY
                audit_id DESC

            LIMIT %s;
            """,
            (
                limit,
            )
        )

        rows = cursor.fetchall()

        print(
            "========================================"
        )

        print(
            "       PIPELINE AUDIT HISTORY"
        )

        print(
            "========================================"
        )

        if not rows:

            print(
                "No audit records found."
            )

        for row in rows:

            print()

            print(
                f"Audit ID: {row[0]}"
            )

            print(
                f"Pipeline: {row[1]}"
            )

            print(
                f"Run ID: {row[2]}"
            )

            print(
                f"Started: {row[3]}"
            )

            print(
                f"Finished: {row[4]}"
            )

            print(
                f"Status: {row[5]}"
            )

            print(
                f"Rows: {row[6]}"
            )

            print(
                f"Latest: {row[7]}"
            )

            print(
                f"Message: {row[8]}"
            )

        cursor.close()

    finally:

        conn.close()


# ============================================================
# ARGUMENTS
# ============================================================

def parse_args():

    parser = argparse.ArgumentParser(
        description="Pipeline audit utility"
    )

    parser.add_argument(
        "action",
        choices=[
            "start",
            "success",
            "fail",
            "show",
        ],
    )

    parser.add_argument(
        "--pipeline",
    )

    parser.add_argument(
        "--run-id",
    )

    parser.add_argument(
        "--message",
        default="",
    )

    parser.add_argument(
        "--limit",
        type=int,
        default=20,
    )

    return parser.parse_args()


# ============================================================
# MAIN
# ============================================================

def main():

    args = parse_args()

    if args.action == "show":

        show_history(
            args.limit
        )

        return

    if not args.pipeline:

        raise ValueError(
            "--pipeline is required"
        )

    if not args.run_id:

        raise ValueError(
            "--run-id is required"
        )

    if args.action == "start":

        start_run(
            args.pipeline,
            args.run_id,
            args.message,
        )

    elif args.action == "success":

        finish_run(
            args.pipeline,
            args.run_id,
            "SUCCESS",
            args.message,
        )

    elif args.action == "fail":

        finish_run(
            args.pipeline,
            args.run_id,
            "FAILED",
            args.message,
        )


if __name__ == "__main__":
    main()