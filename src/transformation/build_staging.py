from pathlib import Path
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


def load_raw_data():

    files = list(
        RAW_PATH.glob("*/*/*.parquet")
    )

    print(f"Found {len(files)} parquet files.")

    dataframes = []

    for file in files:

        print(f"Reading: {file}")

        df = pd.read_parquet(file)

        dataframes.append(df)

    if not dataframes:
        raise ValueError("No raw parquet files found.")

    return pd.concat(
        dataframes,
        ignore_index=True
    )


def validate_schema(df):

    missing_columns = [
        col
        for col in REQUIRED_COLUMNS
        if col not in df.columns
    ]

    if missing_columns:
        raise ValueError(
            f"Missing columns: {missing_columns}"
        )


def clean_data(df):

    # chuẩn hóa tên mã
    df["symbol"] = (
        df["symbol"]
        .str.upper()
        .str.strip()
    )

    # chuẩn hóa thời gian
    df["time"] = pd.to_datetime(
        df["time"]
    )

    # đảm bảo volume >= 0
    df = df[
        df["volume"] >= 0
    ]

    # bỏ dòng thiếu dữ liệu quan trọng
    df = df.dropna(
        subset=[
            "time",
            "symbol",
            "open",
            "high",
            "low",
            "close",
        ]
    )

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


def data_quality_report(df):

    print("\n===== DATA QUALITY REPORT =====")

    print(f"Rows: {len(df)}")

    print(
        f"Symbols: {df['symbol'].nunique()}"
    )

    print(
        f"Date min: {df['time'].min()}"
    )

    print(
        f"Date max: {df['time'].max()}"
    )

    print("\nNull values:")

    print(
        df.isnull().sum()
    )

    print("\nRows per symbol:")

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
        / "stock_ohlcv.parquet"
    )

    df.to_parquet(
        output_file,
        index=False
    )

    print(
        f"\nSaved staging: {output_file}"
    )


def main():

    df = load_raw_data()

    validate_schema(df)

    df = clean_data(df)

    df = remove_duplicates(df)

    data_quality_report(df)

    save_staging(df)


if __name__ == "__main__":
    main()