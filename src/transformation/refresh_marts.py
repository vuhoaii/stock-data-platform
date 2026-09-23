import os

import psycopg2
from dotenv import load_dotenv


# ============================================================
# ENV
# ============================================================

load_dotenv(
    override=True
)


# ============================================================
# MATERIALIZED VIEWS
#
# Thứ tự rất quan trọng:
#
# canonical
#     ↓
# quality / overview / signals
# ============================================================

MATERIALIZED_VIEWS = [

    "mart_stock_price_canonical",

    "mart_stock_quality_summary",

    "mart_stock_overview",

    "mart_stock_signals",
]


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
# REFRESH
# ============================================================

def refresh_views(
    cursor
):

    for view in MATERIALIZED_VIEWS:

        print(
            f"Refreshing "
            f"{view}..."
        )

        cursor.execute(
            f"""
            REFRESH MATERIALIZED VIEW
            {view};
            """
        )

        print(
            f"[OK] {view}"
        )


# ============================================================
# SUMMARY
# ============================================================

def show_summary(
    cursor
):

    print()
    print(
        "========================================"
    )

    print(
        "       ANALYTICS MART SUMMARY"
    )

    print(
        "========================================"
    )

    # --------------------------------------------------------
    # CANONICAL
    # --------------------------------------------------------

    cursor.execute(
        """
        SELECT COUNT(*)
        FROM mart_stock_price_canonical;
        """
    )

    canonical_rows = (
        cursor.fetchone()[0]
    )

    print(
        f"Canonical rows: "
        f"{canonical_rows}"
    )

    # --------------------------------------------------------
    # SIGNAL
    # --------------------------------------------------------

    cursor.execute(
        """
        SELECT COUNT(*)
        FROM mart_stock_signals;
        """
    )

    signal_rows = (
        cursor.fetchone()[0]
    )

    print(
        f"Signal rows: "
        f"{signal_rows}"
    )

    # --------------------------------------------------------
    # LATEST DATE
    # --------------------------------------------------------

    cursor.execute(
        """
        SELECT MAX(full_date)
        FROM mart_stock_signals;
        """
    )

    latest_date = (
        cursor.fetchone()[0]
    )

    print(
        f"Latest signal date: "
        f"{latest_date}"
    )

    # --------------------------------------------------------
    # LATEST SIGNALS
    # --------------------------------------------------------

    cursor.execute(
        """
        SELECT
            symbol,
            close_price,
            price_trend,
            volume_signal,
            trading_signal

        FROM mart_stock_signals

        WHERE full_date = (
            SELECT MAX(full_date)
            FROM mart_stock_signals
        )

        ORDER BY symbol;
        """
    )

    print()
    print(
        "LATEST STOCK SIGNALS"
    )

    for (
        symbol,
        close_price,
        price_trend,
        volume_signal,
        trading_signal,
    ) in cursor.fetchall():

        print(
            f"{symbol:<5} | "
            f"close={close_price} | "
            f"{price_trend} | "
            f"{volume_signal} | "
            f"{trading_signal}"
        )


# ============================================================
# MAIN
# ============================================================

def main():

    print(
        "========================================"
    )

    print(
        "       REFRESH ANALYTICS MARTS"
    )

    print(
        "========================================"
    )

    conn = get_connection()

    try:

        cursor = (
            conn.cursor()
        )

        refresh_views(
            cursor
        )

        conn.commit()

        show_summary(
            cursor
        )

        cursor.close()

        print()
        print(
            "ALL ANALYTICS MARTS "
            "REFRESHED SUCCESSFULLY"
        )

    except Exception as e:

        conn.rollback()

        print(
            f"ERROR: {e}"
        )

        raise

    finally:

        conn.close()


if __name__ == "__main__":
    main()