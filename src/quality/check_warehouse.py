import os
import psycopg2
from dotenv import load_dotenv


load_dotenv()


def get_connection():
    return psycopg2.connect(
        host=os.getenv("DB_HOST"),
        port=os.getenv("DB_PORT"),
        database=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
    )


def run_check(cursor, name, query):
    cursor.execute(query)

    result = cursor.fetchone()[0]

    if result == 0:
        print(f"[PASS] {name}")
    else:
        print(
            f"[FAIL] {name}: {result} bad rows"
        )


def main():

    print(
        "===== DATA QUALITY CHECK ====="
    )

    conn = get_connection()
    cursor = conn.cursor()

    # NULL
    run_check(
        cursor,
        "NULL prices",
        """
        SELECT COUNT(*)
        FROM fact_stock_price
        WHERE open_price IS NULL
           OR high_price IS NULL
           OR low_price IS NULL
           OR close_price IS NULL;
        """
    )

    # Giá âm
    run_check(
        cursor,
        "Negative prices",
        """
        SELECT COUNT(*)
        FROM fact_stock_price
        WHERE open_price < 0
           OR high_price < 0
           OR low_price < 0
           OR close_price < 0;
        """
    )

    # Volume âm
    run_check(
        cursor,
        "Negative volume",
        """
        SELECT COUNT(*)
        FROM fact_stock_price
        WHERE volume < 0;
        """
    )

    # High < Low
    run_check(
        cursor,
        "High lower than Low",
        """
        SELECT COUNT(*)
        FROM fact_stock_price
        WHERE high_price < low_price;
        """
    )

    # Open nằm ngoài High-Low
    run_check(
        cursor,
        "Open outside range",
        """
        SELECT COUNT(*)
        FROM fact_stock_price
        WHERE open_price > high_price
           OR open_price < low_price;
        """
    )

    # Close nằm ngoài High-Low
    run_check(
        cursor,
        "Close outside range",
        """
        SELECT COUNT(*)
        FROM fact_stock_price
        WHERE close_price > high_price
           OR close_price < low_price;
        """
    )

    # Duplicate business key
    cursor.execute(
        """
        SELECT COUNT(*)
        FROM (
            SELECT
                date_key,
                stock_key,
                source,
                COUNT(*)
            FROM fact_stock_price
            GROUP BY
                date_key,
                stock_key,
                source
            HAVING COUNT(*) > 1
        ) x;
        """
    )

    duplicates = cursor.fetchone()[0]

    if duplicates == 0:
        print(
            "[PASS] Duplicate fact records"
        )
    else:
        print(
            f"[FAIL] Duplicate groups: {duplicates}"
        )

    print()
    print(
        "===== WAREHOUSE SUMMARY ====="
    )

    cursor.execute(
        """
        SELECT COUNT(*)
        FROM fact_stock_price;
        """
    )

    print(
        "Fact rows:",
        cursor.fetchone()[0]
    )

    cursor.execute(
        """
        SELECT COUNT(*)
        FROM dim_stock;
        """
    )

    print(
        "Stocks:",
        cursor.fetchone()[0]
    )

    cursor.execute(
        """
        SELECT MIN(full_date), MAX(full_date)
        FROM dim_date;
        """
    )

    date_min, date_max = cursor.fetchone()

    print(
        "Date range:",
        date_min,
        "->",
        date_max
    )

    cursor.close()
    conn.close()


if __name__ == "__main__":
    main()