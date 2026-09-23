import time
import uuid

from pathlib import Path
from datetime import datetime, timezone

import pandas as pd
from vnstock import Quote


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

SOURCE = "VCI"

START_DATE = "2024-01-01"

# Lấy đến ngày hiện tại
END_DATE = datetime.now().strftime("%Y-%m-%d")


# ============================================================
# EXTRACT
# ============================================================

def extract_ohlcv(symbol):
    """
    Lấy dữ liệu OHLCV lịch sử từ VNStock.
    Sau đó tự kiểm tra khoảng ngày vì API
    có thể trả dữ liệu ngoài khoảng yêu cầu.
    """

    quote = Quote(
        symbol=symbol,
        source=SOURCE,
    )

    df = quote.history(
        start=START_DATE,
        end=END_DATE,
        interval="1D",
    )

    if df.empty:
        return df

    df = df.copy()

    # --------------------------------------------------------
    # Chuẩn hóa thời gian
    # --------------------------------------------------------

    df["time"] = pd.to_datetime(
        df["time"],
        errors="coerce",
    )

    # Loại record lỗi datetime
    df = df.dropna(
        subset=["time"]
    )

    # --------------------------------------------------------
    # DEFENSIVE FILTER
    #
    # Không tin hoàn toàn khoảng ngày API trả về.
    # Chỉ giữ đúng dữ liệu nằm trong khoảng mình yêu cầu.
    # --------------------------------------------------------

    start_date = pd.to_datetime(
        START_DATE
    )

    end_date = pd.to_datetime(
        END_DATE
    )

    df = df[
        (df["time"] >= start_date)
        & (df["time"] <= end_date)
    ].copy()

    # --------------------------------------------------------
    # Loại duplicate theo ngày
    # --------------------------------------------------------

    df = df.drop_duplicates(
        subset=["time"],
        keep="last",
    )

    # Sort lại theo ngày
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
    batch_id,
):
    """
    Thêm metadata phục vụ lineage/tracking.
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
):
    """
    Lưu dữ liệu Raw thành Parquet.
    """

    ingestion_date = datetime.now().strftime(
        "%Y-%m-%d"
    )

    output_dir = Path(
        "data"
    ) / "raw" / "vnstock" / "ohlcv" / symbol / ingestion_date

    output_dir.mkdir(
        parents=True,
        exist_ok=True,
    )

    filename = (
        output_dir
        / f"{symbol}_{START_DATE}_{END_DATE}.parquet"
    )

    df.to_parquet(
        filename,
        index=False,
    )

    print(
        f"Saved: {filename}"
    )


# ============================================================
# PROCESS ONE SYMBOL
# ============================================================

def process_symbol(
    symbol,
    batch_id,
):
    """
    Extract + validate + metadata + save
    cho một mã cổ phiếu.
    """

    print()
    print(
        f"Processing {symbol}..."
    )

    # --------------------------------------------------------
    # Extract
    # --------------------------------------------------------

    df = extract_ohlcv(
        symbol
    )

    if df.empty:
        print(
            f"{symbol}: no data."
        )
        return

    # --------------------------------------------------------
    # Metadata
    # --------------------------------------------------------

    df = add_metadata(
        df=df,
        symbol=symbol,
        batch_id=batch_id,
    )

    # --------------------------------------------------------
    # Report
    # --------------------------------------------------------

    print(
        f"Date range: "
        f"{df['time'].min().date()} "
        f"-> "
        f"{df['time'].max().date()}"
    )

    print(
        f"Rows: {len(df)}"
    )

    # --------------------------------------------------------
    # Save Raw
    # --------------------------------------------------------

    save_raw(
        df=df,
        symbol=symbol,
    )

    print(
        f"{symbol}: completed."
    )


# ============================================================
# MAIN
# ============================================================

def main():
    print(
        "========================================"
    )

    print(
        "       INITIAL STOCK INGESTION"
    )

    print(
        "========================================"
    )

    print(
        f"Requested range: "
        f"{START_DATE} -> {END_DATE}"
    )

    # Một lần chạy pipeline dùng chung một batch ID
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
                batch_id=batch_id,
            )

            successful += 1

        except Exception as e:
            failed += 1

            print(
                f"{symbol} ERROR: {e}"
            )

        # tránh request API quá nhanh
        time.sleep(1)

    print()
    print(
        "========================================"
    )

    print(
        "Initial ingestion completed."
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