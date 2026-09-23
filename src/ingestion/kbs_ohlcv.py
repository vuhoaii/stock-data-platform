import time
import uuid

from pathlib import Path
from datetime import datetime, timezone

import pandas as pd
from vnstock import Quote


SYMBOLS = [
    "FPT",
    "VCB",
    "HPG",
    "VNM",
    "MWG",
    "SSI",
]

SOURCE = "KBS"

START_DATE = "2024-01-01"
END_DATE = datetime.now().strftime("%Y-%m-%d")


def extract_ohlcv(symbol):
    quote = Quote(
        symbol=symbol,
        source=SOURCE
    )

    df = quote.history(
        start=START_DATE,
        end=END_DATE,
        interval="1D"
    )

    if df.empty:
        return df

    df = df.copy()

    df["time"] = pd.to_datetime(
        df["time"],
        errors="coerce"
    )

    df = df.dropna(
        subset=["time"]
    )

    start_date = pd.Timestamp(
        START_DATE
    )

    end_date = pd.Timestamp(
        END_DATE
    )

    # Không tin hoàn toàn khoảng ngày API trả về
    df = df[
        (df["time"] >= start_date)
        & (df["time"] <= end_date)
    ].copy()

    df = df.drop_duplicates(
        subset=["time"],
        keep="last"
    )

    df = df.sort_values(
        "time"
    )

    return df.reset_index(
        drop=True
    )


def add_metadata(
    df,
    symbol,
    batch_id
):
    df = df.copy()

    df["symbol"] = symbol
    df["source"] = SOURCE

    df["ingestion_timestamp"] = datetime.now(
        timezone.utc
    )

    df["batch_id"] = batch_id

    return df


def save_raw(
    df,
    symbol
):
    ingestion_date = datetime.now().strftime(
        "%Y-%m-%d"
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
        exist_ok=True
    )

    output_file = (
        output_dir
        / f"{symbol}_{START_DATE}_{END_DATE}.parquet"
    )

    df.to_parquet(
        output_file,
        index=False
    )

    print(
        f"Saved: {output_file}"
    )


def process_symbol(
    symbol,
    batch_id
):
    print()
    print(
        f"Processing {symbol}..."
    )

    df = extract_ohlcv(
        symbol
    )

    if df.empty:
        print(
            f"{symbol}: no data."
        )
        return False

    df = add_metadata(
        df,
        symbol,
        batch_id
    )

    print(
        "Date range:",
        df["time"].min().date(),
        "->",
        df["time"].max().date()
    )

    print(
        f"Rows: {len(df)}"
    )

    save_raw(
        df,
        symbol
    )

    print(
        f"{symbol}: completed."
    )

    return True


def main():
    print(
        "========================================"
    )

    print(
        "          KBS INITIAL INGESTION"
    )

    print(
        "========================================"
    )

    batch_id = str(
        uuid.uuid4()
    )

    print(
        f"Batch ID: {batch_id}"
    )

    successful = 0
    failed = 0

    for symbol in SYMBOLS:
        try:
            result = process_symbol(
                symbol,
                batch_id
            )

            if result:
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
        f"Successful: {successful}"
    )

    print(
        f"Failed: {failed}"
    )

    print(
        "========================================"
    )


if __name__ == "__main__":
    main()