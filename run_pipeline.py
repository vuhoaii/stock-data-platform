import os
import sys
import subprocess

from pathlib import Path

import psycopg2
from dotenv import load_dotenv


# ============================================================
# CONFIG
# ============================================================

ROOT = Path(__file__).resolve().parent

load_dotenv(
    ROOT / ".env",
    override=True,
)

PYTHON = sys.executable

INCREMENTAL_STAGING = (
    ROOT
    / "data"
    / "staging"
    / "multisource_ohlcv_incremental.parquet"
)


# ============================================================
# RUN PYTHON SCRIPT
# ============================================================

def run_step(
    title,
    script,
    arguments=None,
):

    print()
    print(
        "=" * 70
    )

    print(
        f"STEP: {title}"
    )

    print(
        "=" * 70
    )

    command = [
        PYTHON,
        str(
            ROOT / script
        ),
    ]

    if arguments:

        command.extend(
            arguments
        )

    result = subprocess.run(
        command,
        cwd=ROOT,
    )

    if result.returncode != 0:

        raise RuntimeError(
            f"FAILED: {title}"
        )


# ============================================================
# DATABASE
# ============================================================

def get_connection():

    return psycopg2.connect(
        host=os.getenv(
            "DB_HOST"
        ),
        port=os.getenv(
            "DB_PORT"
        ),
        database=os.getenv(
            "DB_NAME"
        ),
        user=os.getenv(
            "DB_USER"
        ),
        password=os.getenv(
            "DB_PASSWORD"
        ),
    )


# ============================================================
# REFRESH MARTS
# ============================================================

def refresh_marts():

    print()
    print(
        "=" * 70
    )

    print(
        "STEP: Refresh materialized views"
    )

    print(
        "=" * 70
    )

    views = [
        "mart_stock_price_canonical",
        "mart_stock_quality_summary",
        "mart_stock_overview",
    ]

    conn = get_connection()

    try:

        cursor = conn.cursor()

        for view in views:

            print(
                f"Refreshing {view}..."
            )

            cursor.execute(
                f"""
                REFRESH MATERIALIZED VIEW
                {view};
                """
            )

        conn.commit()

        cursor.close()

        print(
            "Materialized views refreshed."
        )

    except Exception:

        conn.rollback()

        raise

    finally:

        conn.close()


# ============================================================
# FINAL SUMMARY
# ============================================================

def show_summary():

    print()
    print(
        "=" * 70
    )

    print(
        "FINAL PIPELINE SUMMARY"
    )

    print(
        "=" * 70
    )

    conn = get_connection()

    try:

        cursor = conn.cursor()

        # ----------------------------------------------------
        # FACT MARKET
        # ----------------------------------------------------

        cursor.execute(
            """
            SELECT COUNT(*)
            FROM fact_stock_price;
            """
        )

        total_fact = (
            cursor.fetchone()[0]
        )

        print(
            f"Market fact rows: "
            f"{total_fact}"
        )

        # ----------------------------------------------------
        # SOURCE
        # ----------------------------------------------------

        cursor.execute(
            """
            SELECT
                source,
                COUNT(*)

            FROM fact_stock_price

            GROUP BY source

            ORDER BY source;
            """
        )

        print()
        print(
            "ROWS BY SOURCE"
        )

        for source, count in (
            cursor.fetchall()
        ):

            print(
                f"{source}: {count}"
            )

        # ----------------------------------------------------
        # LATEST DATE
        # ----------------------------------------------------

        cursor.execute(
            """
            SELECT
                f.source,
                MAX(
                    d.full_date
                )

            FROM fact_stock_price f

            JOIN dim_date d
                ON f.date_key =
                   d.date_key

            GROUP BY
                f.source

            ORDER BY
                f.source;
            """
        )

        print()
        print(
            "LATEST DATE BY SOURCE"
        )

        for (
            source,
            latest_date,
        ) in cursor.fetchall():

            print(
                f"{source}: "
                f"{latest_date}"
            )

        # ----------------------------------------------------
        # CANONICAL
        # ----------------------------------------------------

        cursor.execute(
            """
            SELECT COUNT(*)
            FROM mart_stock_price_canonical;
            """
        )

        canonical_rows = (
            cursor.fetchone()[0]
        )

        print()
        print(
            f"Canonical rows: "
            f"{canonical_rows}"
        )

        # ----------------------------------------------------
        # OVERVIEW
        # ----------------------------------------------------

        cursor.execute(
            """
            SELECT
                symbol,
                latest_trade_date,
                close_vci,
                close_kbs,
                has_discrepancy

            FROM mart_stock_overview

            ORDER BY symbol;
            """
        )

        print()
        print(
            "LATEST STOCK OVERVIEW"
        )

        for (
            symbol,
            trade_date,
            close_vci,
            close_kbs,
            discrepancy,
        ) in cursor.fetchall():

            print(
                f"{symbol:<5} "
                f"{trade_date} | "
                f"VCI={close_vci} | "
                f"KBS={close_kbs} | "
                f"discrepancy={discrepancy}"
            )

        cursor.close()

    finally:

        conn.close()


# ============================================================
# MAIN
# ============================================================

def main():

    print()
    print(
        "############################################################"
    )

    print(
        "             STOCK DATA PLATFORM PIPELINE"
    )

    print(
        "############################################################"
    )

    print(
        f"Python: {PYTHON}"
    )

    print(
        f"Database: "
        f"{os.getenv('DB_HOST')}:"
        f"{os.getenv('DB_PORT')}/"
        f"{os.getenv('DB_NAME')}"
    )

    # ========================================================
    # 1. VCI INCREMENTAL
    # ========================================================

    run_step(
        "VCI incremental ingestion",
        "src/ingestion/incremental_ohlcv.py",
    )

    # ========================================================
    # 2. KBS INCREMENTAL
    # ========================================================

    run_step(
        "KBS incremental ingestion",
        "src/ingestion/incremental_kbs_ohlcv.py",
    )

    # ========================================================
    # 3. REMOVE OLD INCREMENTAL STAGING
    #
    # Rất quan trọng:
    # tránh load nhầm staging của lần chạy trước
    # khi hôm nay không có phiên giao dịch.
    # ========================================================

    if INCREMENTAL_STAGING.exists():

        print()
        print(
            "Removing previous incremental staging..."
        )

        INCREMENTAL_STAGING.unlink()

    # ========================================================
    # 4. BUILD MULTI-SOURCE INCREMENTAL
    # ========================================================

    run_step(
        "Build multi-source incremental staging",
        (
            "src/transformation/"
            "build_multisource_incremental_staging.py"
        ),
    )

    # ========================================================
    # 5. LOAD INCREMENTAL
    # ========================================================

    if INCREMENTAL_STAGING.exists():

        run_step(
            "Load multi-source incremental warehouse",
            (
                "src/transformation/"
                "load_multisource_warehouse.py"
            ),
            [
                str(
                    INCREMENTAL_STAGING
                )
            ],
        )

    else:

        print()
        print(
            "No new multi-source incremental "
            "staging found."
        )

        print(
            "Warehouse load skipped."
        )

    # ========================================================
    # 6. REBUILD FULL MULTI-SOURCE STAGING
    #
    # Raw historical + latest incremental
    # -> latest clean representation
    # ========================================================

    run_step(
        "Rebuild full multi-source staging",
        (
            "src/transformation/"
            "build_multisource_staging.py"
        ),
    )

    # ========================================================
    # 7. RECONCILIATION
    # ========================================================

    run_step(
        "VCI vs KBS reconciliation",
        "src/quality/compare_sources.py",
    )

    # ========================================================
    # 8. DATA QUALITY
    # ========================================================

    run_step(
        "Warehouse data quality",
        "src/quality/check_warehouse.py",
    )

    # ========================================================
    # 9. REFRESH MARTS
    # ========================================================

    refresh_marts()

    # ========================================================
    # 10. SUMMARY
    # ========================================================

    show_summary()

    print()
    print(
        "############################################################"
    )

    print(
        "       PIPELINE COMPLETED SUCCESSFULLY"
    )

    print(
        "############################################################"
    )


# ============================================================
# ENTRY
# ============================================================

if __name__ == "__main__":
    main()