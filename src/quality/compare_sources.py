from pathlib import Path
import pandas as pd


INPUT_FILE = Path(
    "data/staging/multisource_ohlcv.parquet"
)

OUTPUT_FILE = Path(
    "data/staging/source_comparison.parquet"
)

PRICE_THRESHOLD_PCT = 0.1
VOLUME_THRESHOLD_PCT = 1.0


# ============================================================
# LOAD
# ============================================================

def load_data():

    df = pd.read_parquet(
        INPUT_FILE
    )

    df = df.copy()

    df["time"] = pd.to_datetime(
        df["time"],
        errors="coerce",
    )

    # --------------------------------------------------------
    # QUAN TRỌNG
    #
    # VCI:
    # 2024-01-02 00:00:00
    #
    # KBS:
    # 2024-01-02 07:00:00
    #
    # Daily OHLCV -> cùng ngày giao dịch
    # nên bỏ phần giờ.
    # --------------------------------------------------------

    df["trade_date"] = (
        df["time"]
        .dt.normalize()
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

    return df


# ============================================================
# PREPARE SOURCE
# ============================================================

def prepare_source(
    df,
    source_name,
):

    source = (
        df[
            df["source"]
            == source_name
        ]
        .copy()
    )

    source = source[
        [
            "trade_date",
            "symbol",
            "open",
            "high",
            "low",
            "close",
            "volume",
        ]
    ]

    # Phòng duplicate cùng ngày/source
    source = (
        source
        .drop_duplicates(
            subset=[
                "trade_date",
                "symbol",
            ],
            keep="last",
        )
    )

    suffix = (
        source_name.lower()
    )

    source = source.rename(
        columns={
            "open":
                f"open_{suffix}",

            "high":
                f"high_{suffix}",

            "low":
                f"low_{suffix}",

            "close":
                f"close_{suffix}",

            "volume":
                f"volume_{suffix}",
        }
    )

    return source


# ============================================================
# SAFE PERCENT DIFFERENCE
# ============================================================

def calculate_diff_pct(
    left,
    right,
):

    diff = (
        left - right
    ).abs()

    denominator = (
        pd.concat(
            [
                left.abs(),
                right.abs(),
            ],
            axis=1,
        )
        .mean(
            axis=1
        )
    )

    pct = (
        diff
        / denominator
        * 100
    )

    pct = pct.where(
        denominator != 0,
        0,
    )

    return (
        diff,
        pct,
    )


# ============================================================
# COMPARISON
# ============================================================

def build_comparison(df):

    vci = prepare_source(
        df,
        "VCI",
    )

    kbs = prepare_source(
        df,
        "KBS",
    )

    print()
    print(
        f"VCI prepared rows: {len(vci)}"
    )

    print(
        f"KBS prepared rows: {len(kbs)}"
    )

    comparison = pd.merge(
        vci,
        kbs,
        on=[
            "trade_date",
            "symbol",
        ],
        how="outer",
        indicator=True,
    )

    comparison[
        "coverage_status"
    ] = comparison[
        "_merge"
    ].map(
        {
            "both":
                "MATCHED",

            "left_only":
                "VCI_ONLY",

            "right_only":
                "KBS_ONLY",
        }
    )

    comparison = comparison.drop(
        columns=[
            "_merge"
        ]
    )

    # ========================================================
    # OHLC DIFFERENCE
    # ========================================================

    for column in [
        "open",
        "high",
        "low",
        "close",
    ]:

        (
            comparison[
                f"{column}_diff"
            ],
            comparison[
                f"{column}_diff_pct"
            ],
        ) = calculate_diff_pct(
            comparison[
                f"{column}_vci"
            ],
            comparison[
                f"{column}_kbs"
            ],
        )

    # ========================================================
    # VOLUME DIFFERENCE
    # ========================================================

    (
        comparison[
            "volume_diff"
        ],
        comparison[
            "volume_diff_pct"
        ],
    ) = calculate_diff_pct(
        comparison[
            "volume_vci"
        ],
        comparison[
            "volume_kbs"
        ],
    )

    # ========================================================
    # FLAGS
    # ========================================================

    comparison[
        "price_discrepancy"
    ] = (
        comparison[
            "close_diff_pct"
        ]
        > PRICE_THRESHOLD_PCT
    )

    comparison[
        "volume_discrepancy"
    ] = (
        comparison[
            "volume_diff_pct"
        ]
        > VOLUME_THRESHOLD_PCT
    )

    comparison[
        "has_discrepancy"
    ] = (
        comparison[
            "price_discrepancy"
        ]
        |
        comparison[
            "volume_discrepancy"
        ]
    )

    comparison = comparison.sort_values(
        [
            "symbol",
            "trade_date",
        ]
    ).reset_index(
        drop=True
    )

    return comparison


# ============================================================
# REPORT
# ============================================================

def show_report(df):

    print()
    print(
        "========================================"
    )

    print(
        "        SOURCE COMPARISON REPORT"
    )

    print(
        "========================================"
    )

    print(
        f"Comparison rows: {len(df)}"
    )

    print()
    print(
        "COVERAGE"
    )

    print(
        df[
            "coverage_status"
        ]
        .value_counts(
            dropna=False
        )
    )

    matched = df[
        df[
            "coverage_status"
        ]
        == "MATCHED"
    ].copy()

    print()
    print(
        f"Matched rows: "
        f"{len(matched)}"
    )

    if matched.empty:

        print(
            "No matched VCI/KBS rows."
        )

        return

    # ========================================================
    # CLOSE
    # ========================================================

    exact_close = (
        matched[
            "close_diff"
        ]
        == 0
    ).sum()

    print()
    print(
        "===== CLOSE PRICE ====="
    )

    print(
        f"Exact matches: "
        f"{exact_close}"
    )

    print(
        f"Exact match rate: "
        f"{exact_close / len(matched) * 100:.2f}%"
    )

    print(
        f"Mean diff %: "
        f"{matched['close_diff_pct'].mean():.6f}%"
    )

    print(
        f"Median diff %: "
        f"{matched['close_diff_pct'].median():.6f}%"
    )

    print(
        f"Max diff %: "
        f"{matched['close_diff_pct'].max():.6f}%"
    )

    # ========================================================
    # VOLUME
    # ========================================================

    exact_volume = (
        matched[
            "volume_diff"
        ]
        == 0
    ).sum()

    print()
    print(
        "===== VOLUME ====="
    )

    print(
        f"Exact matches: "
        f"{exact_volume}"
    )

    print(
        f"Exact match rate: "
        f"{exact_volume / len(matched) * 100:.2f}%"
    )

    print(
        f"Mean diff %: "
        f"{matched['volume_diff_pct'].mean():.6f}%"
    )

    print(
        f"Median diff %: "
        f"{matched['volume_diff_pct'].median():.6f}%"
    )

    print(
        f"Max diff %: "
        f"{matched['volume_diff_pct'].max():.6f}%"
    )

    # ========================================================
    # DISCREPANCIES
    # ========================================================

    discrepancy = matched[
        matched[
            "has_discrepancy"
        ]
    ]

    print()
    print(
        "===== DISCREPANCIES ====="
    )

    print(
        f"Rows flagged: "
        f"{len(discrepancy)}"
    )

    print(
        f"Rate: "
        f"{len(discrepancy) / len(matched) * 100:.2f}%"
    )

    # ========================================================
    # BY SYMBOL
    # ========================================================

    print()
    print(
        "===== BY SYMBOL ====="
    )

    summary = (
        matched
        .groupby(
            "symbol"
        )
        .agg(
            rows=(
                "symbol",
                "size",
            ),

            avg_close_diff_pct=(
                "close_diff_pct",
                "mean",
            ),

            max_close_diff_pct=(
                "close_diff_pct",
                "max",
            ),

            avg_volume_diff_pct=(
                "volume_diff_pct",
                "mean",
            ),

            discrepancies=(
                "has_discrepancy",
                "sum",
            ),
        )
    )

    print(
        summary.to_string()
    )

    # ========================================================
    # TOP DIFFERENCES
    # ========================================================

    print()
    print(
        "===== TOP 20 CLOSE DIFFERENCES ====="
    )

    columns = [
        "trade_date",
        "symbol",
        "close_vci",
        "close_kbs",
        "close_diff",
        "close_diff_pct",
        "volume_vci",
        "volume_kbs",
        "volume_diff_pct",
    ]

    print(
        matched
        .sort_values(
            "close_diff_pct",
            ascending=False,
        )
        [columns]
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
        "       VCI VS KBS RECONCILIATION"
    )

    print(
        "========================================"
    )

    if not INPUT_FILE.exists():

        print(
            f"File not found: "
            f"{INPUT_FILE}"
        )

        return

    df = load_data()

    print(
        f"Input rows: "
        f"{len(df)}"
    )

    print(
        f"Sources: "
        f"{df['source'].unique().tolist()}"
    )

    print()
    print(
        "TIME CHECK"
    )

    print(
        df.groupby(
            "source"
        )[
            "trade_date"
        ].agg(
            [
                "min",
                "max",
                "count",
            ]
        )
    )

    comparison = build_comparison(
        df
    )

    OUTPUT_FILE.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    comparison.to_parquet(
        OUTPUT_FILE,
        index=False,
    )

    show_report(
        comparison
    )

    print()
    print(
        f"Saved: "
        f"{OUTPUT_FILE}"
    )

    print()
    print(
        "SOURCE RECONCILIATION "
        "COMPLETED SUCCESSFULLY"
    )


if __name__ == "__main__":
    main()