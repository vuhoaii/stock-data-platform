import os
import re

import pandas as pd
import psycopg2

from dotenv import load_dotenv


load_dotenv()


STAGING_FILE = (
    "data/staging/fundamental_staging.parquet"
)


def get_connection():

    return psycopg2.connect(
        host=os.getenv("DB_HOST"),
        port=os.getenv("DB_PORT"),
        database=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
    )


# ============================================================
# PERIOD PARSING
# ============================================================

def parse_period(period):

    period = str(period).strip()

    year = None
    quarter = None
    period_type = "unknown"

    # Ví dụ:
    # 2026-Q2
    match = re.search(
        r"(\d{4}).*?[Qq](\d)",
        period
    )

    if match:

        year = int(
            match.group(1)
        )

        quarter = int(
            match.group(2)
        )

        period_type = "quarter"

        return (
            year,
            quarter,
            period_type
        )

    # Ví dụ:
    # Q2-2026
    match = re.search(
        r"[Qq](\d).*?(\d{4})",
        period
    )

    if match:

        quarter = int(
            match.group(1)
        )

        year = int(
            match.group(2)
        )

        period_type = "quarter"

        return (
            year,
            quarter,
            period_type
        )

    # Nếu chỉ có năm
    match = re.fullmatch(
        r"\d{4}",
        period
    )

    if match:

        year = int(period)

        period_type = "year"

    return (
        year,
        quarter,
        period_type
    )


# ============================================================
# DIM STOCK
# ============================================================

def load_dim_stock(
    cursor,
    df
):

    symbols = (
        df["symbol"]
        .drop_duplicates()
        .sort_values()
    )

    for symbol in symbols:

        cursor.execute(
            """
            INSERT INTO dim_stock (
                symbol
            )

            VALUES (%s)

            ON CONFLICT (
                symbol
            )

            DO NOTHING;
            """,
            (
                symbol,
            )
        )

    print(
        f"dim_stock: {len(symbols)} symbols processed"
    )


# ============================================================
# DIM PERIOD
# ============================================================

def load_dim_period(
    cursor,
    df
):

    periods = (
        df["period"]
        .astype(str)
        .drop_duplicates()
        .sort_values()
    )

    for period in periods:

        year, quarter, period_type = (
            parse_period(
                period
            )
        )

        cursor.execute(
            """
            INSERT INTO dim_period (
                period_label,
                year,
                quarter,
                period_type
            )

            VALUES (
                %s,
                %s,
                %s,
                %s
            )

            ON CONFLICT (
                period_label
            )

            DO UPDATE SET

                year =
                    EXCLUDED.year,

                quarter =
                    EXCLUDED.quarter,

                period_type =
                    EXCLUDED.period_type;
            """,
            (
                period,
                year,
                quarter,
                period_type,
            )
        )

    print(
        f"dim_period: {len(periods)} periods processed"
    )


# ============================================================
# KEYS
# ============================================================

def get_stock_keys(
    cursor
):

    cursor.execute(
        """
        SELECT
            stock_key,
            symbol

        FROM dim_stock;
        """
    )

    rows = cursor.fetchall()

    return {
        symbol: stock_key
        for stock_key, symbol
        in rows
    }


def get_period_keys(
    cursor
):

    cursor.execute(
        """
        SELECT
            period_key,
            period_label

        FROM dim_period;
        """
    )

    rows = cursor.fetchall()

    return {
        period_label: period_key
        for period_key, period_label
        in rows
    }


# ============================================================
# FACT
# ============================================================

def load_fact_financial_metric(
    cursor,
    df
):

    stock_keys = get_stock_keys(
        cursor
    )

    period_keys = get_period_keys(
        cursor
    )

    count = 0

    for _, row in df.iterrows():

        symbol = row["symbol"]

        period = str(
            row["period"]
        )

        stock_key = stock_keys[
            symbol
        ]

        period_key = period_keys[
            period
        ]

        ingestion_timestamp = None

        if (
            "ingestion_timestamp"
            in row.index
            and pd.notna(
                row["ingestion_timestamp"]
            )
        ):

            ingestion_timestamp = (
                pd.to_datetime(
                    row[
                        "ingestion_timestamp"
                    ]
                )
                .to_pydatetime()
            )

        batch_id = None

        if (
            "batch_id"
            in row.index
            and pd.notna(
                row["batch_id"]
            )
        ):

            batch_id = str(
                row["batch_id"]
            )

        source = (
            row["source"]
            if "source" in row.index
            else "VCI_FUNDAMENTAL"
        )

        cursor.execute(
            """
            INSERT INTO fact_financial_metric (
                stock_key,
                period_key,
                statement_type,
                metric,
                value,
                source,
                ingestion_timestamp,
                batch_id
            )

            VALUES (
                %s,
                %s,
                %s,
                %s,
                %s,
                %s,
                %s,
                %s
            )

            ON CONFLICT (
                stock_key,
                period_key,
                statement_type,
                metric,
                source
            )

            DO UPDATE SET

                value =
                    EXCLUDED.value,

                ingestion_timestamp =
                    EXCLUDED.ingestion_timestamp,

                batch_id =
                    EXCLUDED.batch_id;
            """,
            (
                stock_key,
                period_key,
                row["statement_type"],
                row["metric"],
                float(row["value"]),
                source,
                ingestion_timestamp,
                batch_id,
            )
        )

        count += 1

    print(
        f"fact_financial_metric: {count} rows processed"
    )


# ============================================================
# MAIN
# ============================================================

def main():

    print(
        "========================================"
    )

    print(
        "      LOAD FUNDAMENTAL WAREHOUSE"
    )

    print(
        "========================================"
    )

    print(
        "Reading staging data..."
    )

    df = pd.read_parquet(
        STAGING_FILE
    )

    print(
        f"Rows: {len(df)}"
    )

    if df.empty:

        print(
            "No fundamental data."
        )

        return

    conn = get_connection()

    try:

        cursor = conn.cursor()

        print()

        print(
            "Loading dim_stock..."
        )

        load_dim_stock(
            cursor,
            df
        )

        print()

        print(
            "Loading dim_period..."
        )

        load_dim_period(
            cursor,
            df
        )

        print()

        print(
            "Loading fact_financial_metric..."
        )

        load_fact_financial_metric(
            cursor,
            df
        )

        conn.commit()

        cursor.close()

        print()
        print(
            "Fundamental warehouse loaded successfully!"
        )

    except Exception as e:

        conn.rollback()

        print()
        print(
            f"ERROR: {e}"
        )

        raise

    finally:

        conn.close()


if __name__ == "__main__":
    main()