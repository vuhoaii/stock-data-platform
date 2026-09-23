import os
import sys

import psycopg2

from dotenv import load_dotenv


# ============================================================
# ENV
# ============================================================

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
# MAIN
# ============================================================

def main():

    print(
        "========================================"
    )

    print(
        " FUNDAMENTAL WAREHOUSE QUALITY CHECK"
    )

    print(
        "========================================"
    )

    conn = get_connection()

    failed = False

    try:

        cursor = conn.cursor()

        # ====================================================
        # TOTAL FACT ROWS
        # ====================================================

        cursor.execute(
            """
            SELECT COUNT(*)
            FROM fact_financial_metric;
            """
        )

        total_rows = (
            cursor.fetchone()[0]
        )

        print()
        print(
            f"Financial fact rows: "
            f"{total_rows}"
        )

        if total_rows == 0:

            print(
                "[FAIL] No financial metrics found"
            )

            failed = True

        else:

            print(
                "[PASS] Financial fact contains data"
            )


        # ====================================================
        # STOCK COVERAGE
        # ====================================================

        cursor.execute(
            """
            SELECT
                COUNT(
                    DISTINCT stock_key
                )

            FROM fact_financial_metric;
            """
        )

        stocks = (
            cursor.fetchone()[0]
        )

        print(
            f"Stocks covered: {stocks}"
        )

        if stocks < 6:

            print(
                "[FAIL] Less than 6 stocks covered"
            )

            failed = True

        else:

            print(
                "[PASS] Stock coverage"
            )


        # ====================================================
        # PERIOD COVERAGE
        # ====================================================

        cursor.execute(
            """
            SELECT
                COUNT(
                    DISTINCT period_key
                )

            FROM fact_financial_metric;
            """
        )

        periods = (
            cursor.fetchone()[0]
        )

        print(
            f"Periods covered: {periods}"
        )

        if periods == 0:

            print(
                "[FAIL] No financial periods"
            )

            failed = True

        else:

            print(
                "[PASS] Period coverage"
            )


        # ====================================================
        # REQUIRED KEY NULLS
        # ====================================================

        cursor.execute(
            """
            SELECT COUNT(*)

            FROM fact_financial_metric

            WHERE
                stock_key IS NULL
                OR period_key IS NULL
                OR statement_type IS NULL
                OR metric IS NULL
                OR source IS NULL;
            """
        )

        null_keys = (
            cursor.fetchone()[0]
        )

        if null_keys == 0:

            print(
                "[PASS] Required financial keys"
            )

        else:

            print(
                f"[FAIL] Required key NULLs: "
                f"{null_keys}"
            )

            failed = True


        # ====================================================
        # DUPLICATES
        # ====================================================

        cursor.execute(
            """
            SELECT COUNT(*)

            FROM (

                SELECT
                    stock_key,
                    period_key,
                    statement_type,
                    metric,
                    source,
                    COUNT(*) AS count_rows

                FROM fact_financial_metric

                GROUP BY
                    stock_key,
                    period_key,
                    statement_type,
                    metric,
                    source

                HAVING COUNT(*) > 1

            ) x;
            """
        )

        duplicates = (
            cursor.fetchone()[0]
        )

        if duplicates == 0:

            print(
                "[PASS] Duplicate financial metrics"
            )

        else:

            print(
                f"[FAIL] Duplicate financial metrics: "
                f"{duplicates}"
            )

            failed = True


        # ====================================================
        # STATEMENT COVERAGE
        # ====================================================

        cursor.execute(
            """
            SELECT
                statement_type,
                COUNT(*)

            FROM fact_financial_metric

            GROUP BY
                statement_type

            ORDER BY
                statement_type;
            """
        )

        print()
        print(
            "ROWS BY STATEMENT TYPE"
        )

        statement_rows = (
            cursor.fetchall()
        )

        for (
            statement_type,
            count,
        ) in statement_rows:

            print(
                f"{statement_type}: "
                f"{count}"
            )


        # ====================================================
        # SOURCE COVERAGE
        # ====================================================

        cursor.execute(
            """
            SELECT
                source,
                COUNT(*)

            FROM fact_financial_metric

            GROUP BY source

            ORDER BY source;
            """
        )

        print()
        print(
            "ROWS BY SOURCE"
        )

        for (
            source,
            count,
        ) in cursor.fetchall():

            print(
                f"{source}: "
                f"{count}"
            )


        # ====================================================
        # SYMBOL COVERAGE
        # ====================================================

        cursor.execute(
            """
            SELECT
                s.symbol,
                COUNT(*) AS metrics

            FROM fact_financial_metric f

            JOIN dim_stock s
                ON f.stock_key =
                   s.stock_key

            GROUP BY
                s.symbol

            ORDER BY
                s.symbol;
            """
        )

        print()
        print(
            "ROWS BY SYMBOL"
        )

        for (
            symbol,
            count,
        ) in cursor.fetchall():

            print(
                f"{symbol}: "
                f"{count}"
            )


        # ====================================================
        # PERIOD LIST
        # ====================================================

        cursor.execute(
            """
            SELECT
                period_label

            FROM dim_period

            ORDER BY
                year DESC,
                quarter DESC NULLS LAST;
            """
        )

        print()
        print(
            "AVAILABLE PERIODS"
        )

        for row in cursor.fetchall():

            print(
                row[0]
            )


        cursor.close()

    finally:

        conn.close()


    # ========================================================
    # RESULT
    # ========================================================

    print()

    if failed:

        print(
            "FUNDAMENTAL DATA QUALITY FAILED"
        )

        sys.exit(1)

    print(
        "FUNDAMENTAL DATA QUALITY PASSED"
    )


if __name__ == "__main__":
    main()