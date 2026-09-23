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

load_dotenv()

SYMBOLS = [
    "FPT",
    "VCB",
    "HPG",
    "VNM",
    "MWG",
    "SSI"
]

SOURCE = "VCI"

# Nếu một mã chưa tồn tại trong Warehouse
# thì sẽ lấy dữ liệu từ ngày này
INITIAL_START_DATE = "2024-01-01"


# ============================================================
# DATABASE
# ============================================================

def get_connection():
    """
    Kết nối PostgreSQL.
    """

    return psycopg2.connect(
        host=os.getenv("DB_HOST"),
        port=os.getenv("DB_PORT"),
        database=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
    )


def get_latest_date(symbol):
    """
    Lấy ngày mới nhất hiện có trong Warehouse
    của từng mã cổ phiếu.
    """

    conn = get_connection()

    try:
        cursor = conn.cursor()

        cursor.execute(
            """
            SELECT MAX(d.full_date)

            FROM fact_stock_price f

            JOIN dim_stock s
                ON f.stock_key = s.stock_key

            JOIN dim_date d
                ON f.date_key = d.date_key

            WHERE s.symbol = %s;
            """,
            (symbol,)
        )

        latest_date = cursor.fetchone()[0]

        cursor.close()

        return latest_date

    finally:
        conn.close()


# ============================================================
# EXTRACT
# ============================================================

def extract_incremental(
    symbol,
    start_date,
    end_date
):
    """
    Gọi VNStock lấy OHLCV.
    """

    quote = Quote(
        symbol=symbol,
        source=SOURCE
    )

    df = quote.history(
        start=start_date,
        end=end_date,
        interval="1D"
    )

    return df


# ============================================================
# VALIDATION
# ============================================================

def filter_new_data(
    df,
    latest_date,
    end_date
):
    """
    Không tin hoàn toàn khoảng ngày API trả về.

    Chỉ giữ record:
        time > latest_date
        time <= end_date
    """

    if df.empty:
        return df

    df = df.copy()

    df["time"] = pd.to_datetime(
        df["time"],
        errors="coerce"
    )

    # bỏ record không parse được time
    df = df.dropna(
        subset=["time"]
    )

    # Nếu Warehouse đã có dữ liệu
    if latest_date is not None:

        df = df[
            df["time"].dt.date
            > latest_date
        ]

    # Không cho dữ liệu vượt quá ngày hiện tại
    df = df[
        df["time"].dt.date
        <= end_date
    ]

    # Loại duplicate nếu API trả trùng ngày
    df = df.drop_duplicates(
        subset=["time"],
        keep="last"
    )

    df = df.sort_values(
        "time"
    )

    df = df.reset_index(
        drop=True
    )

    return df


# ============================================================
# METADATA
# ============================================================

def add_metadata(
    df,
    symbol,
    batch_id
):
    """
    Thêm metadata để tracking dữ liệu.
    """

    df = df.copy()

    df["symbol"] = symbol

    df["source"] = SOURCE

    df["ingestion_timestamp"] = datetime.now(
        timezone.utc
    )

    df["batch_id"] = batch_id

    return df


# ============================================================
# SAVE RAW
# ============================================================

def save_raw(
    df,
    symbol,
    batch_id
):
    """
    Lưu incremental raw data thành Parquet.
    """

    ingestion_date = datetime.now().strftime(
        "%Y-%m-%d"
    )

    output_dir = Path(
        f"data/raw/vnstock/ohlcv/"
        f"{symbol}/{ingestion_date}"
    )

    output_dir.mkdir(
        parents=True,
        exist_ok=True
    )

    short_batch = batch_id[:8]

    filename = (
        output_dir
        / f"{symbol}_incremental_{short_batch}.parquet"
    )

    df.to_parquet(
        filename,
        index=False
    )

    print(
        f"Saved: {filename}"
    )


# ============================================================
# PROCESS ONE SYMBOL
# ============================================================

def process_symbol(
    symbol,
    batch_id
):
    """
    Incremental ingestion cho một mã.
    """

    print()
    print(
        f"Processing {symbol}..."
    )

    # --------------------------------------------------------
    # 1. Xem Warehouse hiện có đến ngày nào
    # --------------------------------------------------------

    latest_date = get_latest_date(
        symbol
    )

    end_date = datetime.now().date()

    # --------------------------------------------------------
    # 2. Xác định ngày bắt đầu
    # --------------------------------------------------------

    if latest_date is None:

        start_date = datetime.strptime(
            INITIAL_START_DATE,
            "%Y-%m-%d"
        ).date()

        print(
            f"{symbol}: not found in Warehouse."
        )

    else:

        start_date = (
            latest_date
            + timedelta(days=1)
        )

        print(
            f"Warehouse latest: {latest_date}"
        )

    print(
        f"Request: {start_date} -> {end_date}"
    )

    # --------------------------------------------------------
    # 3. Warehouse đã cập nhật đến tương lai/current date
    # --------------------------------------------------------

    if start_date > end_date:

        print(
            f"{symbol}: already up to date."
        )

        return

    # --------------------------------------------------------
    # 4. Extract API
    # --------------------------------------------------------

    df = extract_incremental(
        symbol=symbol,
        start_date=start_date.strftime(
            "%Y-%m-%d"
        ),
        end_date=end_date.strftime(
            "%Y-%m-%d"
        )
    )

    print(
        f"API returned: {len(df)} rows"
    )

    # --------------------------------------------------------
    # 5. Defensive filtering
    # --------------------------------------------------------

    df = filter_new_data(
        df=df,
        latest_date=latest_date,
        end_date=end_date
    )

    # --------------------------------------------------------
    # 6. Không có record thực sự mới
    # --------------------------------------------------------

    if df.empty:

        print(
            f"{symbol}: no new trading data."
        )

        return

    # --------------------------------------------------------
    # 7. Metadata
    # --------------------------------------------------------

    df = add_metadata(
        df=df,
        symbol=symbol,
        batch_id=batch_id
    )

    # --------------------------------------------------------
    # 8. Hiển thị khoảng dữ liệu mới
    # --------------------------------------------------------

    print(
        "New data range:",
        df["time"].min().date(),
        "->",
        df["time"].max().date()
    )

    # --------------------------------------------------------
    # 9. Save Raw
    # --------------------------------------------------------

    save_raw(
        df=df,
        symbol=symbol,
        batch_id=batch_id
    )

    print(
        f"{symbol}: {len(df)} new rows"
    )


# ============================================================
# MAIN
# ============================================================

def main():

    print(
        "========================================"
    )

    print(
        "     INCREMENTAL STOCK INGESTION"
    )

    print(
        "========================================"
    )

    # Một lần chạy pipeline = một batch_id
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

            process_symbol(
                symbol=symbol,
                batch_id=batch_id
            )

            successful += 1

        except Exception as e:

            failed += 1

            print(
                f"{symbol} ERROR: {e}"
            )

        # Tránh request API quá nhanh
        time.sleep(1)

    print()
    print(
        "========================================"
    )

    print(
        "Incremental ingestion completed."
    )

    print(
        f"Processed successfully: {successful}"
    )

    print(
        f"Failed: {failed}"
    )

    print(
        "========================================"
    )


if __name__ == "__main__":
    main()