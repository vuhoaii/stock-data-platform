from pathlib import Path
from datetime import datetime

import pandas as pd


TODAY = datetime.now().strftime("%Y-%m-%d")

VCI_ROOT = Path("data/raw/vnstock/ohlcv")
KBS_ROOT = Path("data/raw/kbs/ohlcv")

OUTPUT_FILE = Path(
    "data/staging/multisource_ohlcv_incremental.parquet"
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


def find_incremental_files(
    root,
    source,
):

    files = list(
        root.glob(
            f"*/{TODAY}/*_incremental_*.parquet"
        )
    )

    print(
        f"{source} incremental files: "
        f"{len(files)}"
    )

    return files


def load_files(
    files,
    source,
):

    frames = []

    for file in files:

        print(
            f"Reading: {file}"
        )

        try:

            df = pd.read_parquet(
                file
            )

            if df.empty:
                continue

            missing = [
                col
                for col in REQUIRED_COLUMNS
                if col not in df.columns
            ]

            if missing:

                print(
                    f"SKIP {file}"
                )

                print(
                    f"Missing columns: {missing}"
                )

                continue

            df = df.copy()

            df["source"] = source

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


def clean(df):

    df = df.copy()

    df["time"] = pd.to_datetime(
        df["time"],
        errors="coerce",
    )

    df["ingestion_timestamp"] = pd.to_datetime(
        df["ingestion_timestamp"],
        errors="coerce",
        utc=True,
    )

    df["symbol"] = (
        df["symbol"]
        .astype(str)
        .str.upper()
        .str.strip()
    )

    df["source"] = (
        df["source"]
        .astype(str)
        .str.upper()
        .str.strip()
    )

    for col in [
        "open",
        "high",
        "low",
        "close",
        "volume",
    ]:

        df[col] = pd.to_numeric(
            df[col],
            errors="coerce",
        )

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

    df = df[
        (df["open"] >= 0)
        & (df["high"] >= 0)
        & (df["low"] >= 0)
        & (df["close"] >= 0)
        & (df["volume"] >= 0)
    ]

    df = df[
        df["high"] >= df["low"]
    ]

    # Daily business key.
    # VCI = 00:00
    # KBS = 07:00
    df["trade_date"] = (
        df["time"]
        .dt.normalize()
    )

    before = len(df)

    df = (
        df
        .sort_values(
            "ingestion_timestamp"
        )
        .drop_duplicates(
            subset=[
                "trade_date",
                "symbol",
                "source",
            ],
            keep="last",
        )
    )

    print(
        f"Duplicates removed: "
        f"{before - len(df)}"
    )

    # trade_date chỉ dùng để deduplicate
    df = df.drop(
        columns=[
            "trade_date"
        ]
    )

    return df.reset_index(
        drop=True
    )


def main():

    print(
        "========================================"
    )
    print(
        " BUILD MULTI-SOURCE INCREMENTAL STAGING"
    )
    print(
        "========================================"
    )

    vci_files = find_incremental_files(
        VCI_ROOT,
        "VCI",
    )

    kbs_files = find_incremental_files(
        KBS_ROOT,
        "KBS",
    )

    vci = load_files(
        vci_files,
        "VCI",
    )

    kbs = load_files(
        kbs_files,
        "KBS",
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
            "No incremental data found."
        )
        return

    df = pd.concat(
        frames,
        ignore_index=True,
    )

    df = clean(
        df
    )

    OUTPUT_FILE.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    df.to_parquet(
        OUTPUT_FILE,
        index=False,
    )

    print()
    print(
        "===== REPORT ====="
    )

    print(
        f"Rows: {len(df)}"
    )

    print(
        df.groupby(
            [
                "symbol",
                "source",
            ]
        ).size()
    )

    print()

    print(
        f"Saved: {OUTPUT_FILE}"
    )


if __name__ == "__main__":
    main()