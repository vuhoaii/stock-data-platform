import time
import uuid

from pathlib import Path
from datetime import datetime, timezone

import pandas as pd
from vnstock import Fundamental


# ============================================================
# CONFIG
# ============================================================

SYMBOLS = [
    "FPT",
    "VCB",
    "HPG",
    "VNM",
    "MWG",
    "SSI",
]

STATEMENTS = [
    "income_statement",
    "balance_sheet",
    "cash_flow",
]


# ============================================================
# FIX DUPLICATE COLUMN NAMES
# ============================================================

def make_unique_columns(columns):
    """
    Chuyển tên cột bị trùng:

    revenue
    revenue
    total_assets
    total_assets

    thành:

    revenue
    revenue_2
    total_assets
    total_assets_2
    """

    counts = {}
    result = []

    for column in columns:

        column = str(column).strip()

        if column not in counts:

            counts[column] = 1

            result.append(
                column
            )

        else:

            counts[column] += 1

            new_column = (
                f"{column}_"
                f"{counts[column]}"
            )

            # Phòng trường hợp tên mới cũng đã tồn tại
            while new_column in counts:

                counts[column] += 1

                new_column = (
                    f"{column}_"
                    f"{counts[column]}"
                )

            counts[new_column] = 1

            result.append(
                new_column
            )

    return result


# ============================================================
# NORMALIZE DATAFRAME
# ============================================================

def normalize_dataframe(df):

    if df is None:
        return pd.DataFrame()

    if df.empty:
        return pd.DataFrame()

    df = df.copy()

    # Một số response trả period ở index
    if not isinstance(
        df.index,
        pd.RangeIndex
    ):

        df = df.reset_index()

    # Tất cả tên cột -> string
    df.columns = [
        str(column).strip()
        for column in df.columns
    ]

    # Fix duplicate columns
    df.columns = make_unique_columns(
        df.columns
    )

    return df


# ============================================================
# FETCH ONE FINANCIAL STATEMENT
# ============================================================

def fetch_statement(
    equity,
    statement_type,
):

    method = getattr(
        equity,
        statement_type
    )

    """
    Thử theo thứ tự:

    1. quarterly time_series
    2. quarterly report
    3. yearly time_series
    4. yearly report

    Nếu một kiểu lỗi thì tự fallback.
    """

    attempts = [
        (
            "quarter",
            "time_series",
        ),
        (
            "quarter",
            "report",
        ),
        (
            "year",
            "time_series",
        ),
        (
            "year",
            "report",
        ),
    ]

    errors = []

    for period_type, orientation in attempts:

        try:

            print(
                f"      trying "
                f"period={period_type}, "
                f"orient={orientation}"
            )

            df = method(
                period=period_type,
                orient=orientation,
            )

            df = normalize_dataframe(
                df
            )

            if not df.empty:

                print(
                    f"      SUCCESS "
                    f"({period_type}/"
                    f"{orientation})"
                )

                print(
                    f"      API rows: "
                    f"{len(df)}"
                )

                print(
                    f"      API columns: "
                    f"{len(df.columns)}"
                )

                return (
                    df,
                    period_type,
                    orientation,
                )

            error_message = (
                f"{period_type}/"
                f"{orientation}: EMPTY"
            )

            errors.append(
                error_message
            )

            print(
                f"      EMPTY "
                f"({period_type}/"
                f"{orientation})"
            )

        except Exception as e:

            error_message = (
                f"{period_type}/"
                f"{orientation}: {e}"
            )

            errors.append(
                error_message
            )

            print(
                f"      FAILED "
                f"({period_type}/"
                f"{orientation}): {e}"
            )

    print(
        "      ALL ATTEMPTS FAILED"
    )

    for error in errors:

        print(
            f"        {error}"
        )

    return (
        pd.DataFrame(),
        None,
        None,
    )


# ============================================================
# SAVE RAW PARQUET
# ============================================================

def save_raw(
    df,
    symbol,
    statement_type,
    period_type,
    orientation,
    batch_id,
):

    df = df.copy()

    # --------------------------------------------------------
    # Fix duplicate một lần nữa trước khi add metadata
    # --------------------------------------------------------

    df.columns = make_unique_columns(
        df.columns
    )

    # --------------------------------------------------------
    # Metadata
    # --------------------------------------------------------

    df["symbol"] = symbol

    df["statement_type"] = (
        statement_type
    )

    df["source"] = (
        "VNSTOCK_FUNDAMENTAL"
    )

    df["report_period_type"] = (
        period_type
    )

    df["orientation"] = (
        orientation
    )

    df["ingestion_timestamp"] = (
        datetime.now(
            timezone.utc
        )
    )

    df["batch_id"] = (
        batch_id
    )

    # --------------------------------------------------------
    # Kiểm tra duplicate lần cuối
    # --------------------------------------------------------

    if df.columns.duplicated().any():

        print(
            "      WARNING: "
            "duplicate columns remained."
        )

        df.columns = (
            make_unique_columns(
                df.columns
            )
        )

    # --------------------------------------------------------
    # Output folder
    # --------------------------------------------------------

    ingestion_date = (
        datetime.now()
        .strftime(
            "%Y-%m-%d"
        )
    )

    output_dir = (
        Path("data")
        / "raw"
        / "fundamental"
        / statement_type
        / symbol
        / ingestion_date
    )

    output_dir.mkdir(
        parents=True,
        exist_ok=True,
    )

    output_file = (
        output_dir
        / f"{symbol}_"
          f"{statement_type}.parquet"
    )

    # --------------------------------------------------------
    # Save
    # --------------------------------------------------------

    df.to_parquet(
        output_file,
        index=False,
    )

    print(
        f"      Saved: "
        f"{output_file}"
    )

    print(
        f"      Rows: "
        f"{len(df)}"
    )

    print(
        f"      Columns: "
        f"{len(df.columns)}"
    )

    print(
        f"      Period type: "
        f"{period_type}"
    )

    print(
        f"      Orientation: "
        f"{orientation}"
    )

    return True


# ============================================================
# PROCESS ONE SYMBOL
# ============================================================

def process_symbol(
    symbol,
    batch_id,
):

    print()
    print(
        "=" * 60
    )

    print(
        f"PROCESSING {symbol}"
    )

    print(
        "=" * 60
    )

    # --------------------------------------------------------
    # Initialize Fundamental
    # --------------------------------------------------------

    try:

        fundamental = Fundamental()

        equity = (
            fundamental
            .equity(
                symbol
            )
        )

    except Exception as e:

        print(
            f"{symbol} INIT ERROR: "
            f"{e}"
        )

        return (
            0,
            len(STATEMENTS),
        )

    successful = 0
    failed = 0

    # --------------------------------------------------------
    # Statements
    # --------------------------------------------------------

    for statement_type in STATEMENTS:

        print()
        print(
            f"  -> {statement_type}"
        )

        try:

            (
                df,
                period_type,
                orientation,
            ) = fetch_statement(
                equity,
                statement_type,
            )

            if df.empty:

                print(
                    f"      NO DATA: "
                    f"{symbol} / "
                    f"{statement_type}"
                )

                failed += 1

                continue

            save_success = save_raw(
                df=df,
                symbol=symbol,
                statement_type=statement_type,
                period_type=period_type,
                orientation=orientation,
                batch_id=batch_id,
            )

            if save_success:

                successful += 1

            else:

                failed += 1

        except Exception as e:

            failed += 1

            print(
                f"      ERROR: {e}"
            )

        time.sleep(1)

    return (
        successful,
        failed,
    )


# ============================================================
# COVERAGE REPORT
# ============================================================

def show_coverage():

    print()
    print(
        "========================================"
    )

    print(
        "           RAW COVERAGE"
    )

    print(
        "========================================"
    )

    root = Path(
        "data/raw/fundamental"
    )

    existing = 0

    for symbol in SYMBOLS:

        for statement_type in STATEMENTS:

            path = (
                root
                / statement_type
                / symbol
            )

            files = list(
                path.rglob(
                    "*.parquet"
                )
            )

            status = (
                "OK"
                if files
                else "MISSING"
            )

            if files:
                existing += 1

            print(
                f"{symbol:<5} | "
                f"{statement_type:<18} | "
                f"{status}"
            )

    print()

    print(
        f"Coverage: "
        f"{existing}/"
        f"{len(SYMBOLS) * len(STATEMENTS)}"
    )


# ============================================================
# MAIN
# ============================================================

def main():

    print(
        "========================================"
    )

    print(
        "     FUNDAMENTAL DATA INGESTION"
    )

    print(
        "========================================"
    )

    batch_id = str(
        uuid.uuid4()
    )

    print(
        f"Batch ID: "
        f"{batch_id}"
    )

    total_success = 0
    total_failed = 0

    # --------------------------------------------------------
    # Run all symbols
    # --------------------------------------------------------

    for symbol in SYMBOLS:

        success, failed = process_symbol(
            symbol,
            batch_id,
        )

        total_success += (
            success
        )

        total_failed += (
            failed
        )

        time.sleep(1)

    # --------------------------------------------------------
    # Result
    # --------------------------------------------------------

    print()
    print(
        "========================================"
    )

    print(
        "INGESTION FINISHED"
    )

    print(
        f"Statements successful: "
        f"{total_success}"
    )

    print(
        f"Statements failed: "
        f"{total_failed}"
    )

    print(
        "========================================"
    )

    show_coverage()


if __name__ == "__main__":
    main()