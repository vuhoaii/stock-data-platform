from pathlib import Path
import pandas as pd


VCI_ROOT = Path(
    "data/raw/vnstock/ohlcv"
)

KBS_ROOT = Path(
    "data/raw/kbs/ohlcv"
)

OUTPUT_FILE = Path(
    "data/staging/multisource_ohlcv.parquet"
)


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


# ============================================================
# LOAD SOURCE
# ============================================================

def load_source(
    root_path,
    source_name,
):

    files = list(
        root_path.glob(
            "*/*/*.parquet"
        )
    )

    print()
    print(
        f"{source_name} files: "
        f"{len(files)}"
    )

    if not files:

        return pd.DataFrame()

    frames = []

    for file in files:

        try:

            df = pd.read_parquet(
                file
            )

            if df.empty:
                continue

            missing = [
                column
                for column
                in REQUIRED_COLUMNS
                if column not in df.columns
            ]

            if missing:

                print(
                    f"SKIP {file}"
                )

                print(
                    f"Missing: {missing}"
                )

                continue

            df = df.copy()

            # ép source rõ ràng
            df["source"] = (
                source_name
            )

            frames.append(
                df
            )

        except Exception as e:

            print(
                f"ERROR {file}: {e}"
            )

    if not frames:

        return pd.DataFrame()

    return pd.concat(
        frames,
        ignore_index=True,
    )


# ============================================================
# CLEAN
# ============================================================

def clean_data(df):

    df = df.copy()

    # --------------------------------------------------------
    # Datetime
    # --------------------------------------------------------

    df["time"] = pd.to_datetime(
        df["time"],
        errors="coerce",
    )

    df[
        "ingestion_timestamp"
    ] = pd.to_datetime(
        df[
            "ingestion_timestamp"
        ],
        errors="coerce",
        utc=True,
    )

    # --------------------------------------------------------
    # Symbol
    # --------------------------------------------------------

    df["symbol"] = (
        df["symbol"]
        .astype(str)
        .str.upper()
        .str.strip()
    )

    # --------------------------------------------------------
    # Numeric
    # --------------------------------------------------------

    numeric_columns = [
        "open",
        "high",
        "low",
        "close",
        "volume",
    ]

    for column in numeric_columns:

        df[column] = pd.to_numeric(
            df[column],
            errors="coerce",
        )

    # --------------------------------------------------------
    # Drop null
    # --------------------------------------------------------

    df = df.dropna(
        subset=[
            "time",
            "symbol",
            "source",
            "open",
            "high",
            "low",
            "close",
            "volume",
        ]
    )

    # --------------------------------------------------------
    # Business rules
    # --------------------------------------------------------

    df = df[
        (df["open"] >= 0)
        & (df["high"] >= 0)
        & (df["low"] >= 0)
        & (df["close"] >= 0)
        & (df["volume"] >= 0)
    ]

    df = df[
        df["high"]
        >= df["low"]
    ]

    df = df[
        (df["open"] >= df["low"])
        & (df["open"] <= df["high"])
    ]

    df = df[
        (df["close"] >= df["low"])
        & (df["close"] <= df["high"])
    ]

    # --------------------------------------------------------
    # Deduplicate
    # --------------------------------------------------------

    before = len(df)

    df = df.sort_values(
        "ingestion_timestamp"
    )

    df = df.drop_duplicates(
        subset=[
            "time",
            "symbol",
            "source",
        ],
        keep="last",
    )

    print(
        f"Duplicates removed: "
        f"{before - len(df)}"
    )

    return df.reset_index(
        drop=True
    )


# ============================================================
# REPORT
# ============================================================

def show_report(df):

    print()
    print(
        "========================================"
    )

    print(
        "     MULTI-SOURCE STAGING REPORT"
    )

    print(
        "========================================"
    )

    print(
        f"Total rows: {len(df)}"
    )

    print(
        f"Sources: "
        f"{df['source'].nunique()}"
    )

    print(
        f"Symbols: "
        f"{df['symbol'].nunique()}"
    )

    print(
        f"Date range: "
        f"{df['time'].min().date()} "
        f"-> "
        f"{df['time'].max().date()}"
    )

    print()
    print(
        "ROWS BY SOURCE"
    )

    print(
        df.groupby(
            "source"
        ).size()
    )

    print()
    print(
        "ROWS BY SYMBOL / SOURCE"
    )

    print(
        df.groupby(
            [
                "symbol",
                "source",
            ]
        ).size()
    )


# ============================================================
# MAIN
# ============================================================

def main():

    print(
        "========================================"
    )

    print(
        "      BUILD MULTI-SOURCE STAGING"
    )

    print(
        "========================================"
    )

    vci = load_source(
        VCI_ROOT,
        "VCI",
    )

    kbs = load_source(
        KBS_ROOT,
        "KBS",
    )

    if vci.empty:

        print(
            "WARNING: VCI data not found."
        )

    if kbs.empty:

        print(
            "WARNING: KBS data not found."
        )

    frames = [
        df
        for df in [
            vci,
            kbs,
        ]
        if not df.empty
    ]

    if not frames:

        print(
            "No market data found."
        )

        return

    combined = pd.concat(
        frames,
        ignore_index=True,
    )

    combined = clean_data(
        combined
    )

    OUTPUT_FILE.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    combined.to_parquet(
        OUTPUT_FILE,
        index=False,
    )

    show_report(
        combined
    )

    print()
    print(
        f"Saved: {OUTPUT_FILE}"
    )

    print()
    print(
        "MULTI-SOURCE STAGING "
        "COMPLETED SUCCESSFULLY"
    )


if __name__ == "__main__":
    main()