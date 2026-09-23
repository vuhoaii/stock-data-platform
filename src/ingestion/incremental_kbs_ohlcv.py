import os
import time
import uuid

from pathlib import Path
from datetime import datetime, timedelta, timezone

import pandas as pd
import psycopg2

from dotenv import load_dotenv
from vnstock import Quote


# ============================================================
# CONFIG
# ============================================================

load_dotenv(override=True)

SYMBOLS = [
    "FPT",
    "VCB",
    "HPG",
    "VNM",
    "MWG",
    "SSI",
]

SOURCE = "KBS"

INITIAL_START_DATE = "2024-01-01"


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
# LATEST DATE
# ============================================================

def get_latest_date(symbol):

    conn = get_connection()

    try:

        cursor = conn.cursor()

        cursor.execute(
            """
            SELECT
                MAX(d.full_date)

            FROM fact_stock_price f

            JOIN dim_stock s
                ON f.stock_key = s.stock_key

            JOIN dim_date d
                ON f.date_key = d.date_key

            WHERE
                s.symbol = %s
                AND f.source = %s;
            """,
            (
                symbol,
                SOURCE,
            )
        )

        result = cursor.fetchone()[0]

        cursor.close()

        return result

    finally:

        conn.close()


# ============================================================
# EXTRACT
# ============================================================

def extract_data(
    symbol,
    start_date,
    end_date,
):

    quote = Quote(
        symbol=symbol,
        source=SOURCE,
    )

    return quote.history(
        start=start_date,
        end=end_date,
        interval="1D",
    )


# ============================================================
# FILTER
# ============================================================

def filter_new_data(
    df,
    latest_date,
    end_date,
):

    if df is None or df.empty:

        return pd.DataFrame()

    df = df.copy()

    df["time"] = pd.to_datetime(
        df["time"],
        errors="coerce",
    )

    df = df.dropna(
        subset=[
            "time"
        ]
    )

    # KBS daily thường trả 07:00.
    # Business key là ngày giao dịch.
    df["trade_date"] = (
        df["time"]
        .dt.date
    )

    end_date_value = (
        pd.to_datetime(
            end_date
        )
        .date()
    )

    df = df[
        df["trade_date"]
        <= end_date_value
    ]

    if latest_date is not None:

        df = df[
            df["trade_date"]
            > latest_date
        ]

    df = (
        df
        .drop_duplicates(
            subset=[
                "trade_date"
            ],
            keep="last",
        )
        .sort_values(
            "time"
        )
        .reset_index(
            drop=True
        )
    )

    df = df.drop(
        columns=[
            "trade_date"
        ]
    )

    return df


# ============================================================
# METADATA
# ============================================================

def add_metadata(
    df,
    symbol,
    batch_id,
):

    df = df.copy()

    df["symbol"] = symbol
    df["source"] = SOURCE

    df["ingestion_timestamp"] = (
        datetime.now(
            timezone.utc
        )
    )

    df["batch_id"] = (
        batch_id
    )

    return df


# ============================================================
# SAVE
# ============================================================

def save_raw(
    df,
    symbol,
    batch_id,
):

    ingestion_date = (
        datetime.now()
        .strftime(
            "%Y-%m-%d"
        )
    )

    output_dir = (
        Path("data")
        / "raw"
        / "kbs"
        / "ohlcv"
        / symbol
        / ingestion_date
    )

    output_dir.mkdir(
        parents=True,
        exist_ok=True,
    )

    output_file = (
        output_dir
        / f"{symbol}_incremental_"
          f"{batch_id[:8]}.parquet"
    )

    df.to_parquet(
        output_file,
        index=False,
    )

    print(
        f"Saved: {output_file}"
    )


# ============================================================
# SYMBOL
# ============================================================

def process_symbol(
    symbol,
    batch_id,
):

    print()
    print(
        f"Processing {symbol}..."
    )

    latest_date = get_latest_date(
        symbol
    )

    print(
        f"KBS warehouse latest: "
        f"{latest_date}"
    )

    if latest_date:

        start_date = (
            latest_date
            + timedelta(
                days=1
            )
        ).strftime(
            "%Y-%m-%d"
        )

    else:

        start_date = (
            INITIAL_START_DATE
        )

    end_date = (
        datetime.now()
        .strftime(
            "%Y-%m-%d"
        )
    )

    print(
        f"Request: "
        f"{start_date} -> {end_date}"
    )

    df = extract_data(
        symbol,
        start_date,
        end_date,
    )

    print(
        f"API returned: "
        f"{len(df)} rows"
    )

    df = filter_new_data(
        df,
        latest_date,
        end_date,
    )

    if df.empty:

        print(
            f"{symbol}: "
            f"no new KBS trading data."
        )

        return 0

    df = add_metadata(
        df,
        symbol,
        batch_id,
    )

    print(
        "New data range: "
        f"{df['time'].min()} "
        "-> "
        f"{df['time'].max()}"
    )

    save_raw(
        df,
        symbol,
        batch_id,
    )

    print(
        f"{symbol}: "
        f"{len(df)} new rows"
    )

    return len(df)


# ============================================================
# MAIN
# ============================================================

def main():

    print(
        "========================================"
    )

    print(
        "       KBS INCREMENTAL INGESTION"
    )

    print(
        "========================================"
    )

    batch_id = str(
        uuid.uuid4()
    )

    total_rows = 0
    successful = 0
    failed = 0

    for symbol in SYMBOLS:

        try:

            rows = process_symbol(
                symbol,
                batch_id,
            )

            total_rows += rows
            successful += 1

        except Exception as e:

            failed += 1

            print(
                f"{symbol} ERROR: {e}"
            )

        time.sleep(1)

    print()
    print(
        "========================================"
    )

    print(
        "KBS incremental completed."
    )

    print(
        f"Successful: {successful}"
    )

    print(
        f"Failed: {failed}"
    )

    print(
        f"New rows: {total_rows}"
    )

    print(
        "========================================"
    )


if __name__ == "__main__":
    main()