import os
import sys

from datetime import datetime
from zoneinfo import ZoneInfo

import psycopg2

from dotenv import load_dotenv


load_dotenv(
    override=True
)


EXPECTED_SOURCES = [
    "VCI",
    "KBS",
]


EXPECTED_SYMBOLS = {
    "FPT",
    "VCB",
    "HPG",
    "VNM",
    "MWG",
    "SSI",
}


MAX_STALE_DAYS = int(
    os.getenv(
        "MARKET_MAX_STALE_DAYS",
        "10",
    )
)


def get_connection():

    return psycopg2.connect(
        host=os.getenv("DB_HOST"),
        port=os.getenv("DB_PORT"),
        database=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
    )


def main():

    print(
        "========================================"
    )

    print(
        "       SOURCE FRESHNESS CHECK"
    )

    print(
        "========================================"
    )

    conn = get_connection()

    failed = False

    try:

        cursor = conn.cursor()

        # ====================================================
        # LATEST DATE BY SOURCE
        # ====================================================

        cursor.execute(
            """
            SELECT
                f.source,
                MAX(d.full_date)

            FROM fact_stock_price f

            JOIN dim_date d
                ON f.date_key =
                   d.date_key

            WHERE
                f.source IN (
                    'VCI',
                    'KBS'
                )

            GROUP BY
                f.source

            ORDER BY
                f.source;
            """
        )

        latest_dates = {
            source: latest_date
            for source, latest_date
            in cursor.fetchall()
        }

        print()
        print(
            "LATEST DATE BY SOURCE"
        )

        for source in EXPECTED_SOURCES:

            print(
                f"{source}: "
                f"{latest_dates.get(source)}"
            )

        # ====================================================
        # SOURCE EXISTENCE
        # ====================================================

        for source in EXPECTED_SOURCES:

            if (
                source
                not in latest_dates
            ):

                print(
                    f"[FAIL] Missing source: "
                    f"{source}"
                )

                failed = True


        if failed:

            cursor.close()

            print()
            print(
                "SOURCE FRESHNESS FAILED"
            )

            sys.exit(1)


        # ====================================================
        # SOURCE SYNCHRONIZATION
        # ====================================================

        vci_date = (
            latest_dates["VCI"]
        )

        kbs_date = (
            latest_dates["KBS"]
        )

        if vci_date != kbs_date:

            print()
            print(
                "[FAIL] Sources are not synchronized"
            )

            print(
                f"VCI latest: {vci_date}"
            )

            print(
                f"KBS latest: {kbs_date}"
            )

            failed = True

        else:

            print()
            print(
                "[PASS] VCI and KBS "
                "latest dates match"
            )


        # ====================================================
        # SYMBOL COVERAGE AT LATEST DATE
        # ====================================================

        for source in EXPECTED_SOURCES:

            latest_date = (
                latest_dates[
                    source
                ]
            )

            cursor.execute(
                """
                SELECT
                    DISTINCT s.symbol

                FROM fact_stock_price f

                JOIN dim_stock s
                    ON f.stock_key =
                       s.stock_key

                JOIN dim_date d
                    ON f.date_key =
                       d.date_key

                WHERE
                    f.source = %s
                    AND d.full_date = %s

                ORDER BY
                    s.symbol;
                """,
                (
                    source,
                    latest_date,
                )
            )

            actual_symbols = {
                row[0]
                for row in cursor.fetchall()
            }

            print()
            print(
                f"{source} SYMBOL COVERAGE"
            )

            print(
                f"Expected: "
                f"{sorted(EXPECTED_SYMBOLS)}"
            )

            print(
                f"Actual: "
                f"{sorted(actual_symbols)}"
            )

            missing = (
                EXPECTED_SYMBOLS
                - actual_symbols
            )

            extra = (
                actual_symbols
                - EXPECTED_SYMBOLS
            )

            if missing:

                print(
                    f"[FAIL] Missing symbols: "
                    f"{sorted(missing)}"
                )

                failed = True

            if extra:

                print(
                    f"[WARN] Extra symbols: "
                    f"{sorted(extra)}"
                )

            if (
                not missing
                and not extra
            ):

                print(
                    "[PASS] Symbol coverage"
                )


        # ====================================================
        # STALENESS
        #
        # Không yêu cầu latest_date == today
        # vì cuối tuần / ngày nghỉ thị trường.
        #
        # Chỉ fail nếu dữ liệu cũ quá lâu.
        # ====================================================

        latest_market_date = max(
            latest_dates.values()
        )

        today = datetime.now(
            ZoneInfo(
                "Asia/Ho_Chi_Minh"
            )
        ).date()

        stale_days = (
            today
            - latest_market_date
        ).days

        print()
        print(
            "STALENESS"
        )

        print(
            f"Latest market date: "
            f"{latest_market_date}"
        )

        print(
            f"Today: {today}"
        )

        print(
            f"Age: {stale_days} days"
        )

        print(
            f"Allowed: "
            f"{MAX_STALE_DAYS} days"
        )

        if (
            stale_days
            > MAX_STALE_DAYS
        ):

            print(
                "[FAIL] Market data is stale"
            )

            failed = True

        else:

            print(
                "[PASS] Market data freshness"
            )


        cursor.close()

    finally:

        conn.close()


    print()

    if failed:

        print(
            "SOURCE FRESHNESS CHECK FAILED"
        )

        sys.exit(1)

    print(
        "SOURCE FRESHNESS CHECK PASSED"
    )


if __name__ == "__main__":
    main()