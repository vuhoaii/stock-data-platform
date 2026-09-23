from pathlib import Path

import pandas as pd


# ============================================================
# CONFIG
# ============================================================

RAW_ROOT = Path(
    "data/raw/fundamental"
)

OUTPUT_FILE = Path(
    "data/staging/fundamental_staging.parquet"
)

STATEMENT_TYPES = [
    "income_statement",
    "balance_sheet",
    "cash_flow",
]


# ============================================================
# PERIOD NORMALIZATION
# ============================================================

def normalize_period(df):

    df = df.copy()

    # --------------------------------------------------------
    # Trường hợp chuẩn: đã có period
    # --------------------------------------------------------

    if "period" in df.columns:

        df["period"] = (
            df["period"]
            .astype(str)
            .str.strip()
        )

        return df

    # --------------------------------------------------------
    # yearReport + lengthReport
    # --------------------------------------------------------

    if (
        "yearReport" in df.columns
        and "lengthReport" in df.columns
    ):

        years = pd.to_numeric(
            df["yearReport"],
            errors="coerce",
        )

        lengths = pd.to_numeric(
            df["lengthReport"],
            errors="coerce",
        )

        periods = []

        for year, length in zip(
            years,
            lengths,
        ):

            if pd.isna(year):

                periods.append(
                    None
                )

                continue

            year = int(year)

            if pd.isna(length):

                periods.append(
                    str(year)
                )

                continue

            length = int(length)

            # quarter dạng 1/2/3/4
            if length in [
                1,
                2,
                3,
                4,
            ]:

                periods.append(
                    f"{year}-Q{length}"
                )

            # quarter dạng tháng 3/6/9/12
            elif length in [
                3,
                6,
                9,
                12,
            ]:

                quarter = (
                    length // 3
                )

                periods.append(
                    f"{year}-Q{quarter}"
                )

            else:

                periods.append(
                    f"{year}-P{length}"
                )

        df["period"] = (
            periods
        )

        return df

    # --------------------------------------------------------
    # year + quarter
    # --------------------------------------------------------

    if (
        "year" in df.columns
        and "quarter" in df.columns
    ):

        df["period"] = (
            df["year"]
            .astype(str)
            + "-Q"
            + df["quarter"]
            .astype(str)
        )

        return df

    # --------------------------------------------------------
    # Chỉ có year
    # --------------------------------------------------------

    if "year" in df.columns:

        df["period"] = (
            df["year"]
            .astype(str)
        )

        return df

    return None


# ============================================================
# READ ONE RAW FILE
# ============================================================

def transform_file(
    statement_type,
    file,
):

    print()
    print(
        f"Reading: {file}"
    )

    df = pd.read_parquet(
        file
    )

    if df.empty:

        print(
            "  EMPTY"
        )

        return pd.DataFrame()

    df = df.copy()

    # Folder:
    #
    # fundamental/
    #   income_statement/
    #       FPT/
    #           date/
    #               file.parquet

    symbol = (
        file.parts[-3]
        .upper()
        .strip()
    )

    # ========================================================
    # PERIOD
    # ========================================================

    df = normalize_period(
        df
    )

    if df is None:

        print(
            "  SKIP: cannot identify period"
        )

        return pd.DataFrame()

    # ========================================================
    # STANDARD METADATA
    # ========================================================

    df["symbol"] = symbol

    df["statement_type"] = (
        statement_type
    )

    if "source" not in df.columns:

        df["source"] = (
            "VNSTOCK_FUNDAMENTAL"
        )

    if (
        "report_period_type"
        not in df.columns
    ):

        df["report_period_type"] = (
            "unknown"
        )

    if (
        "orientation"
        not in df.columns
    ):

        df["orientation"] = (
            "unknown"
        )

    # ========================================================
    # COLUMNS NOT CONSIDERED METRICS
    # ========================================================

    metadata_columns = {
        "period",
        "ticker",
        "symbol",
        "statement_type",
        "source",
        "report_period_type",
        "orientation",
        "ingestion_timestamp",
        "batch_id",
        "yearReport",
        "lengthReport",
        "year",
        "quarter",
    }

    metric_columns = [
        column
        for column in df.columns
        if column not in metadata_columns
    ]

    print(
        f"  Symbol: {symbol}"
    )

    print(
        f"  Raw rows: {len(df)}"
    )

    print(
        f"  Metrics detected: "
        f"{len(metric_columns)}"
    )

    # ========================================================
    # WIDE -> LONG
    # ========================================================

    records = []

    for index, row in df.iterrows():

        period = row[
            "period"
        ]

        if (
            pd.isna(period)
            or str(period).strip() == ""
        ):

            continue

        for metric in metric_columns:

            value = pd.to_numeric(
                row[metric],
                errors="coerce",
            )

            # Không lưu metric null
            if pd.isna(value):

                continue

            record = {
                "symbol":
                    symbol,

                "period":
                    str(period).strip(),

                "statement_type":
                    statement_type,

                # QUAN TRỌNG:
                # giữ revenue_2,
                # total_assets_2...
                # Không merge.
                "metric":
                    str(metric).strip(),

                "value":
                    float(value),

                "source":
                    str(
                        row["source"]
                    ),

                "report_period_type":
                    str(
                        row[
                            "report_period_type"
                        ]
                    ),

                "orientation":
                    str(
                        row[
                            "orientation"
                        ]
                    ),
            }

            if (
                "ingestion_timestamp"
                in df.columns
            ):

                record[
                    "ingestion_timestamp"
                ] = row[
                    "ingestion_timestamp"
                ]

            if (
                "batch_id"
                in df.columns
            ):

                record[
                    "batch_id"
                ] = row[
                    "batch_id"
                ]

            records.append(
                record
            )

    result = pd.DataFrame(
        records
    )

    print(
        f"  Generated rows: "
        f"{len(result)}"
    )

    return result


# ============================================================
# CLEAN STAGING
# ============================================================

def clean_staging(df):

    df = df.copy()

    # --------------------------------------------------------
    # Standardize
    # --------------------------------------------------------

    df["symbol"] = (
        df["symbol"]
        .astype(str)
        .str.upper()
        .str.strip()
    )

    df["period"] = (
        df["period"]
        .astype(str)
        .str.strip()
    )

    df["metric"] = (
        df["metric"]
        .astype(str)
        .str.strip()
    )

    df["statement_type"] = (
        df["statement_type"]
        .astype(str)
        .str.strip()
    )

    df["source"] = (
        df["source"]
        .astype(str)
        .str.strip()
    )

    # --------------------------------------------------------
    # Timestamp
    # --------------------------------------------------------

    if (
        "ingestion_timestamp"
        in df.columns
    ):

        df[
            "ingestion_timestamp"
        ] = pd.to_datetime(
            df[
                "ingestion_timestamp"
            ],
            errors="coerce",
            utc=True,
        )

        df = df.sort_values(
            "ingestion_timestamp"
        )

    # --------------------------------------------------------
    # Duplicates
    # --------------------------------------------------------

    business_key = [
        "symbol",
        "period",
        "statement_type",
        "metric",
        "source",
    ]

    before = len(df)

    df = df.drop_duplicates(
        subset=business_key,
        keep="last",
    )

    removed = (
        before - len(df)
    )

    print()
    print(
        f"Duplicates removed: "
        f"{removed}"
    )

    return df.reset_index(
        drop=True
    )


# ============================================================
# QUALITY REPORT
# ============================================================

def show_report(df):

    print()
    print(
        "========================================"
    )

    print(
        "     FUNDAMENTAL STAGING REPORT"
    )

    print(
        "========================================"
    )

    print(
        f"Total rows: {len(df)}"
    )

    print(
        f"Symbols: "
        f"{df['symbol'].nunique()}"
    )

    print(
        f"Periods: "
        f"{df['period'].nunique()}"
    )

    print(
        f"Metrics: "
        f"{df['metric'].nunique()}"
    )

    print()
    print(
        "ROWS BY SYMBOL"
    )

    print(
        df.groupby(
            "symbol"
        ).size()
    )

    print()
    print(
        "ROWS BY STATEMENT"
    )

    print(
        df.groupby(
            "statement_type"
        ).size()
    )

    print()
    print(
        "COVERAGE"
    )

    coverage = (
        df.groupby(
            [
                "symbol",
                "statement_type",
            ]
        )
        .size()
        .unstack(
            fill_value=0
        )
    )

    print(
        coverage
    )

    print()
    print(
        "PERIOD TYPES"
    )

    print(
        df.groupby(
            "report_period_type"
        ).size()
    )

    print()
    print(
        "ORIENTATIONS"
    )

    print(
        df.groupby(
            "orientation"
        ).size()
    )

    print()
    print(
        "SAMPLE"
    )

    columns = [
        "symbol",
        "period",
        "statement_type",
        "metric",
        "value",
        "source",
        "report_period_type",
    ]

    print(
        df[
            columns
        ]
        .head(20)
        .to_string(
            index=False
        )
    )


# ============================================================
# MAIN
# ============================================================

def main():

    print(
        "========================================"
    )

    print(
        "      BUILD FUNDAMENTAL STAGING"
    )

    print(
        "========================================"
    )

    frames = []

    total_files = 0

    # ========================================================
    # READ 18 RAW FILES
    # ========================================================

    for statement_type in STATEMENT_TYPES:

        root = (
            RAW_ROOT
            / statement_type
        )

        files = list(
            root.glob(
                "*/*/*.parquet"
            )
        )

        print()
        print(
            f"{statement_type}: "
            f"{len(files)} files"
        )

        total_files += (
            len(files)
        )

        for file in files:

            try:

                transformed = (
                    transform_file(
                        statement_type,
                        file,
                    )
                )

                if not transformed.empty:

                    frames.append(
                        transformed
                    )

            except Exception as e:

                print(
                    f"  ERROR: {e}"
                )

    print()
    print(
        f"Total Raw files: "
        f"{total_files}"
    )

    if not frames:

        print(
            "No fundamental data "
            "was transformed."
        )

        return

    # ========================================================
    # COMBINE
    # ========================================================

    final_df = pd.concat(
        frames,
        ignore_index=True,
    )

    final_df = clean_staging(
        final_df
    )

    # ========================================================
    # SAVE
    # ========================================================

    OUTPUT_FILE.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    final_df.to_parquet(
        OUTPUT_FILE,
        index=False,
    )

    show_report(
        final_df
    )

    print()
    print(
        f"Saved: "
        f"{OUTPUT_FILE}"
    )

    print()
    print(
        "FUNDAMENTAL STAGING "
        "COMPLETED SUCCESSFULLY"
    )


if __name__ == "__main__":
    main()