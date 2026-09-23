from pathlib import Path
from datetime import datetime

import pandas as pd


RAW_PATH = Path("data/raw/vnstock/ohlcv")
STAGING_PATH = Path("data/staging")

REQUIRED_COLUMNS = [
    "time",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "symbol",
    "source",
    "ingestion_timestamp",
    "batch_id",
]


def find_incremental_files():

    today = datetime.now().strftime("%Y-%m-%d")

    files = list(
        RAW_PATH.glob(
            f"*/{today}/*_incremental_*.parquet"
        )
    )

    return files


def load_files(files):

    dataframes = []

    for file in files:

        print(f"Reading: {file}")

        df = pd.read_parquet(file)

        dataframes.append(df)

    if not dataframes:
        return pd.DataFrame()

    return pd.concat(
        dataframes,
        ignore_index=True
    )


def validate_schema(df):

    missing_columns = [
        column
        for column in REQUIRED_COLUMNS
        if column not in df.columns
    ]

    if missing_columns:

        raise ValueError(
            f"Missing columns: {missing_columns}"
        )


def clean_data(df):

    df = df.copy()

    df["time"] = pd.to_datetime(
        df["time"],
        errors="coerce"
    )

    df["symbol"] = (
        df["symbol"]
        .astype(str)
        .str.upper()
        .str.strip()
    )

    numeric_columns = [
        "open",
        "high",
        "low",
        "close",
        "volume"
    ]

    for column in numeric_columns:

        df[column] = pd.to_numeric(
            df[column],
            errors="coerce"
        )

    # bỏ record thiếu dữ liệu quan trọng
    df = df.dropna(
        subset=[
            "time",
            "symbol",
            "open",
            "high",
            "low",
            "close",
            "volume"
        ]
    )

    # Giá và volume không được âm
    df = df[
        (df["open"] >= 0)
        & (df["high"] >= 0)
        & (df["low"] >= 0)
        & (df["close"] >= 0)
        & (df["volume"] >= 0)
    ]

    # High phải >= Low
    df = df[
        df["high"] >= df["low"]
    ]

    return df


def remove_duplicates(df):

    before = len(df)

    df = df.drop_duplicates(
        subset=[
            "time",
            "symbol",
            "source"
        ],
        keep="last"
    )

    after = len(df)

    print(
        f"Duplicates removed: {before - after}"
    )

    return df


def show_report(df):

    print()
    print(
        "===== INCREMENTAL STAGING REPORT ====="
    )

    print(
        f"Rows: {len(df)}"
    )

    print(
        f"Symbols: {df['symbol'].nunique()}"
    )

    print(
        f"Date min: {df['time'].min()}"
    )

    print(
        f"Date max: {df['time'].max()}"
    )

    print()

    print(
        "Rows per symbol:"
    )

    print(
        df.groupby("symbol").size()
    )


def save_staging(df):

    STAGING_PATH.mkdir(
        parents=True,
        exist_ok=True
    )

    output_file = (
        STAGING_PATH
        / "stock_ohlcv_incremental.parquet"
    )

    df.to_parquet(
        output_file,
        index=False
    )

    print()
    print(
        f"Saved: {output_file}"
    )


def main():

    print(
        "===== BUILD INCREMENTAL STAGING ====="
    )

    files = find_incremental_files()

    print(
        f"Incremental files found: {len(files)}"
    )

    if not files:

        print(
            "No new incremental files."
        )

        print(
            "Nothing to stage."
        )

        return

    df = load_files(files)

    validate_schema(df)

    df = clean_data(df)

    df = remove_duplicates(df)

    if df.empty:

        print(
            "No valid new rows after cleaning."
        )

        return

    show_report(df)

    save_staging(df)

    print()
    print(
        "Incremental staging completed."
    )


if __name__ == "__main__":
    main()