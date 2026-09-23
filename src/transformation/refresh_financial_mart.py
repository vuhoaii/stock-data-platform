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
        "     REFRESH FINANCIAL ANALYTICS"
    )

    print(
        "========================================"
    )

    conn = get_connection()

    try:

        cursor = conn.cursor()

        print(
            "Refreshing mart_financial_latest..."
        )

        cursor.execute(
            """
            REFRESH MATERIALIZED VIEW
            mart_financial_latest;
            """
        )

        conn.commit()

        # ----------------------------------------------------
        # SUMMARY
        # ----------------------------------------------------

        cursor.execute(
            """
            SELECT COUNT(*)
            FROM mart_financial_latest;
            """
        )

        rows = (
            cursor.fetchone()[0]
        )

        print(
            f"mart_financial_latest rows: "
            f"{rows}"
        )

        print()
        print(
            "FINANCIAL MART REFRESHED "
            "SUCCESSFULLY"
        )

        cursor.close()

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