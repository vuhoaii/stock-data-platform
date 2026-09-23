import os

import pandas as pd
import psycopg2

from dotenv import load_dotenv


load_dotenv()

STAGING_FILE = "data/staging/stock_ohlcv.parquet"


def get_connection():
    return psycopg2.connect(
        host=os.getenv("DB_HOST"),
        port=os.getenv("DB_PORT"),
        database=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
    )


def load_dim_stock(cursor, df):
    symbols = sorted(df["symbol"].drop_duplicates())

    for symbol in symbols:
        cursor.execute(
            """
            INSERT INTO dim_stock (symbol)
            VALUES (%s)
            ON CONFLICT (symbol)
            DO NOTHING;
            """,
            (symbol,)
        )

    print(f"dim_stock: {len(symbols)} symbols")


def load_dim_date(cursor, df):
    dates = (
        pd.to_datetime(df["time"])
        .drop_duplicates()
        .sort_values()
    )

    for date in dates:
        date_key = int(date.strftime("%Y%m%d"))
        quarter = ((date.month - 1) // 3) + 1

        cursor.execute(
            """
            INSERT INTO dim_date (
                date_key,
                full_date,
                day,
                month,
                quarter,
                year,
                day_of_week
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (date_key)
            DO NOTHING;
            """,
            (
                date_key,
                date.date(),
                date.day,
                date.month,
                quarter,
                date.year,
                date.day_name(),
            )
        )

    print(f"dim_date: {len(dates)} dates")


def get_stock_keys(cursor):
    cursor.execute(
        """
        SELECT stock_key, symbol
        FROM dim_stock;
        """
    )

    rows = cursor.fetchall()

    return {
        symbol: stock_key
        for stock_key, symbol in rows
    }


def load_fact_stock_price(cursor, df):
    stock_keys = get_stock_keys(cursor)

    count = 0

    for _, row in df.iterrows():
        date = pd.to_datetime(row["time"])

        date_key = int(
            date.strftime("%Y%m%d")
        )

        stock_key = stock_keys[
            row["symbol"]
        ]

        ingestion_time = pd.to_datetime(
            row["ingestion_timestamp"]
        ).to_pydatetime()

        cursor.execute(
            """
            INSERT INTO fact_stock_price (
                date_key,
                stock_key,
                open_price,
                high_price,
                low_price,
                close_price,
                volume,
                source,
                ingestion_timestamp,
                batch_id
            )
            VALUES (
                %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s
            )
            ON CONFLICT (
                date_key,
                stock_key,
                source
            )
            DO UPDATE SET
                open_price = EXCLUDED.open_price,
                high_price = EXCLUDED.high_price,
                low_price = EXCLUDED.low_price,
                close_price = EXCLUDED.close_price,
                volume = EXCLUDED.volume,
                ingestion_timestamp =
                    EXCLUDED.ingestion_timestamp,
                batch_id = EXCLUDED.batch_id;
            """,
            (
                date_key,
                stock_key,
                float(row["open"]),
                float(row["high"]),
                float(row["low"]),
                float(row["close"]),
                int(row["volume"]),
                row["source"],
                ingestion_time,
                row["batch_id"],
            )
        )

        count += 1

    print(
        f"fact_stock_price: {count} rows"
    )


def main():
    print("Reading staging data...")

    df = pd.read_parquet(
        STAGING_FILE
    )

    print(
        f"Staging rows: {len(df)}"
    )

    conn = get_connection()

    try:
        cursor = conn.cursor()

        load_dim_stock(
            cursor,
            df
        )

        load_dim_date(
            cursor,
            df
        )

        load_fact_stock_price(
            cursor,
            df
        )

        conn.commit()

        cursor.close()

        print(
            "\nWarehouse loaded successfully!"
        )

    except Exception as e:
        conn.rollback()

        print(
            f"\nERROR: {e}"
        )

        raise

    finally:
        conn.close()


if __name__ == "__main__":
    main()