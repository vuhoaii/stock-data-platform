import os
import argparse

import pandas as pd
import psycopg2

from dotenv import load_dotenv


# ============================================================
# ENV
# ============================================================

load_dotenv(
    override=True
)


# ============================================================
# ARGUMENTS
# ============================================================

def parse_args():

    parser = argparse.ArgumentParser(
        description=(
            "Load multi-source stock staging "
            "data into PostgreSQL warehouse."
        )
    )

    parser.add_argument(
        "staging_file",
        nargs="?",
        default=(
            "data/staging/"
            "multisource_ohlcv.parquet"
        ),
        help=(
            "Path to staging parquet file. "
            "Default: "
            "data/staging/"
            "multisource_ohlcv.parquet"
        ),
    )

    return parser.parse_args()


# ============================================================
# DATABASE
# ============================================================

def get_connection():

    return psycopg2.connect(
        host=os.getenv(
            "DB_HOST"
        ),
        port=os.getenv(
            "DB_PORT"
        ),
        database=os.getenv(
            "DB_NAME"
        ),
        user=os.getenv(
            "DB_USER"
        ),
        password=os.getenv(
            "DB_PASSWORD"
        ),
    )


# ============================================================
# DATE KEY
# ============================================================

def build_date_key(
    date_value
):

    date_value = pd.to_datetime(
        date_value,
        errors="raise",
    )

    return int(
        date_value.strftime(
            "%Y%m%d"
        )
    )


# ============================================================
# PREPARE STAGING
# ============================================================

def prepare_staging(
    df
):

    df = df.copy()

    # --------------------------------------------------------
    # TIME
    # --------------------------------------------------------

    df["time"] = pd.to_datetime(
        df["time"],
        errors="coerce",
    )

    # --------------------------------------------------------
    # SYMBOL / SOURCE
    # --------------------------------------------------------

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

    # --------------------------------------------------------
    # NUMERIC
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
    # INGESTION TIMESTAMP
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

    # --------------------------------------------------------
    # DROP INVALID
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
    # DAILY BUSINESS KEY
    #
    # VCI = 00:00
    # KBS = 07:00
    #
    # Cùng ngày giao dịch.
    # --------------------------------------------------------

    df["trade_date"] = (
        df["time"]
        .dt.normalize()
    )

    # --------------------------------------------------------
    # DEDUP
    # --------------------------------------------------------

    sort_columns = [
        "trade_date",
        "symbol",
        "source",
    ]

    if (
        "ingestion_timestamp"
        in df.columns
    ):

        sort_columns.append(
            "ingestion_timestamp"
        )

    df = df.sort_values(
        sort_columns
    )

    before = len(df)

    df = df.drop_duplicates(
        subset=[
            "trade_date",
            "symbol",
            "source",
        ],
        keep="last",
    )

    removed = (
        before - len(df)
    )

    print(
        f"Duplicates removed: "
        f"{removed}"
    )

    return df.reset_index(
        drop=True
    )


# ============================================================
# DIM STOCK
# ============================================================

def load_dim_stock(
    cursor,
    df
):

    symbols = (
        df["symbol"]
        .drop_duplicates()
        .sort_values()
    )

    for symbol in symbols:

        cursor.execute(
            """
            INSERT INTO dim_stock (
                symbol
            )

            VALUES (%s)

            ON CONFLICT (
                symbol
            )

            DO NOTHING;
            """,
            (
                symbol,
            )
        )

    print(
        f"dim_stock: "
        f"{len(symbols)} "
        f"symbols processed"
    )


# ============================================================
# DIM DATE
# ============================================================

def load_dim_date(
    cursor,
    df
):

    dates = (
        df["trade_date"]
        .drop_duplicates()
        .sort_values()
    )

    for date_value in dates:

        date_key = build_date_key(
            date_value
        )

        cursor.execute(
            """
            INSERT INTO dim_date (
                date_key,
                full_date,
                day,
                month,
                quarter,
                year,
                day_of_week
            )

            VALUES (
                %s,
                %s,
                %s,
                %s,
                %s,
                %s,
                %s
            )

            ON CONFLICT (
                date_key
            )

            DO NOTHING;
            """,
            (
                date_key,
                date_value.date(),
                date_value.day,
                date_value.month,
                date_value.quarter,
                date_value.year,
                date_value.day_name(),
            )
        )

    print(
        f"dim_date: "
        f"{len(dates)} "
        f"dates processed"
    )


# ============================================================
# STOCK KEY MAP
# ============================================================

def get_stock_keys(
    cursor
):

    cursor.execute(
        """
        SELECT
            stock_key,
            symbol

        FROM dim_stock;
        """
    )

    rows = cursor.fetchall()

    return {
        symbol: stock_key
        for stock_key, symbol
        in rows
    }


# ============================================================
# LOAD FACT
# ============================================================

def load_fact_stock_price(
    cursor,
    df
):

    stock_keys = get_stock_keys(
        cursor
    )

    processed = 0
    skipped = 0

    for _, row in df.iterrows():

        symbol = (
            str(
                row["symbol"]
            )
            .upper()
            .strip()
        )

        source = (
            str(
                row["source"]
            )
            .upper()
            .strip()
        )

        # ----------------------------------------------------
        # STOCK KEY
        # ----------------------------------------------------

        if (
            symbol
            not in stock_keys
        ):

            print(
                f"SKIP unknown symbol: "
                f"{symbol}"
            )

            skipped += 1

            continue

        stock_key = (
            stock_keys[
                symbol
            ]
        )

        # ----------------------------------------------------
        # DATE KEY
        # ----------------------------------------------------

        trade_date = (
            row[
                "trade_date"
            ]
        )

        if pd.isna(
            trade_date
        ):

            skipped += 1

            continue

        date_key = build_date_key(
            trade_date
        )

        # ----------------------------------------------------
        # INGESTION TIMESTAMP
        # ----------------------------------------------------

        ingestion_timestamp = None

        if (
            "ingestion_timestamp"
            in row.index
            and pd.notna(
                row[
                    "ingestion_timestamp"
                ]
            )
        ):

            ingestion_timestamp = (
                pd.to_datetime(
                    row[
                        "ingestion_timestamp"
                    ],
                    utc=True,
                )
                .to_pydatetime()
            )

        # ----------------------------------------------------
        # BATCH ID
        # ----------------------------------------------------

        batch_id = None

        if (
            "batch_id"
            in row.index
            and pd.notna(
                row[
                    "batch_id"
                ]
            )
        ):

            batch_id = str(
                row[
                    "batch_id"
                ]
            )

        # ----------------------------------------------------
        # UPSERT FACT
        # ----------------------------------------------------

        cursor.execute(
            """
            INSERT INTO fact_stock_price (
                date_key,
                stock_key,
                open_price,
                high_price,
                low_price,
                close_price,
                volume,
                source,
                ingestion_timestamp,
                batch_id
            )

            VALUES (
                %s,
                %s,
                %s,
                %s,
                %s,
                %s,
                %s,
                %s,
                %s,
                %s
            )

            ON CONFLICT (
                date_key,
                stock_key,
                source
            )

            DO UPDATE SET

                open_price =
                    EXCLUDED.open_price,

                high_price =
                    EXCLUDED.high_price,

                low_price =
                    EXCLUDED.low_price,

                close_price =
                    EXCLUDED.close_price,

                volume =
                    EXCLUDED.volume,

                ingestion_timestamp =
                    EXCLUDED.ingestion_timestamp,

                batch_id =
                    EXCLUDED.batch_id;
            """,
            (
                date_key,
                stock_key,
                float(
                    row["open"]
                ),
                float(
                    row["high"]
                ),
                float(
                    row["low"]
                ),
                float(
                    row["close"]
                ),
                int(
                    row["volume"]
                ),
                source,
                ingestion_timestamp,
                batch_id,
            )
        )

        processed += 1

    print(
        f"fact_stock_price: "
        f"{processed} "
        f"rows processed"
    )

    print(
        f"Skipped: "
        f"{skipped}"
    )


# ============================================================
# SUMMARY
# ============================================================

def show_summary(
    cursor
):

    print()
    print(
        "========================================"
    )

    print(
        "    MULTI-SOURCE WAREHOUSE SUMMARY"
    )

    print(
        "========================================"
    )

    # --------------------------------------------------------
    # TOTAL
    # --------------------------------------------------------

    cursor.execute(
        """
        SELECT
            COUNT(*)

        FROM fact_stock_price;
        """
    )

    total_rows = (
        cursor.fetchone()[0]
    )

    print(
        f"Total fact rows: "
        f"{total_rows}"
    )

    # --------------------------------------------------------
    # SOURCE
    # --------------------------------------------------------

    cursor.execute(
        """
        SELECT
            source,
            COUNT(*)

        FROM fact_stock_price

        GROUP BY
            source

        ORDER BY
            source;
        """
    )

    print()
    print(
        "ROWS BY SOURCE"
    )

    for source, count in (
        cursor.fetchall()
    ):

        print(
            f"{source}: {count}"
        )

    # --------------------------------------------------------
    # SYMBOL / SOURCE
    # --------------------------------------------------------

    cursor.execute(
        """
        SELECT
            s.symbol,
            f.source,
            COUNT(*)

        FROM fact_stock_price f

        JOIN dim_stock s
            ON f.stock_key =
               s.stock_key

        GROUP BY
            s.symbol,
            f.source

        ORDER BY
            s.symbol,
            f.source;
        """
    )

    print()
    print(
        "ROWS BY SYMBOL / SOURCE"
    )

    for (
        symbol,
        source,
        count,
    ) in cursor.fetchall():

        print(
            f"{symbol:<5} "
            f"{source:<5} "
            f"{count}"
        )

    # --------------------------------------------------------
    # LATEST DATE BY SOURCE
    # --------------------------------------------------------

    cursor.execute(
        """
        SELECT
            f.source,
            MAX(
                d.full_date
            )

        FROM fact_stock_price f

        JOIN dim_date d
            ON f.date_key =
               d.date_key

        GROUP BY
            f.source

        ORDER BY
            f.source;
        """
    )

    print()
    print(
        "LATEST DATE BY SOURCE"
    )

    for (
        source,
        latest_date,
    ) in cursor.fetchall():

        print(
            f"{source}: "
            f"{latest_date}"
        )


# ============================================================
# MAIN
# ============================================================

def main():

    args = parse_args()

    staging_file = (
        args.staging_file
    )

    print(
        "========================================"
    )

    print(
        "   LOAD MULTI-SOURCE STOCK WAREHOUSE"
    )

    print(
        "========================================"
    )

    print(
        f"Reading staging file: "
        f"{staging_file}"
    )

    # --------------------------------------------------------
    # LOAD STAGING
    # --------------------------------------------------------

    try:

        df = pd.read_parquet(
            staging_file
        )

    except FileNotFoundError:

        print(
            f"ERROR: staging file "
            f"not found:"
        )

        print(
            staging_file
        )

        return

    print(
        f"Raw staging rows: "
        f"{len(df)}"
    )

    if df.empty:

        print(
            "No staging data."
        )

        return

    # --------------------------------------------------------
    # PREPARE
    # --------------------------------------------------------

    df = prepare_staging(
        df
    )

    print(
        f"Prepared rows: "
        f"{len(df)}"
    )

    print(
        f"Sources: "
        f"{df['source'].unique().tolist()}"
    )

    print(
        f"Symbols: "
        f"{df['symbol'].nunique()}"
    )

    print()

    print(
        "STAGING COVERAGE"
    )

    print(
        df.groupby(
            [
                "symbol",
                "source",
            ]
        ).size()
    )

    # --------------------------------------------------------
    # CONNECT
    # --------------------------------------------------------

    print()
    print(
        "DATABASE"
    )

    print(
        f"Host: "
        f"{os.getenv('DB_HOST')}"
    )

    print(
        f"Port: "
        f"{os.getenv('DB_PORT')}"
    )

    print(
        f"Database: "
        f"{os.getenv('DB_NAME')}"
    )

    conn = get_connection()

    try:

        cursor = (
            conn.cursor()
        )

        # ----------------------------------------------------
        # DIM STOCK
        # ----------------------------------------------------

        print()
        print(
            "Loading dim_stock..."
        )

        load_dim_stock(
            cursor,
            df,
        )

        # ----------------------------------------------------
        # DIM DATE
        # ----------------------------------------------------

        print()
        print(
            "Loading dim_date..."
        )

        load_dim_date(
            cursor,
            df,
        )

        # ----------------------------------------------------
        # FACT
        # ----------------------------------------------------

        print()
        print(
            "Loading fact_stock_price..."
        )

        load_fact_stock_price(
            cursor,
            df,
        )

        # ----------------------------------------------------
        # COMMIT
        # ----------------------------------------------------

        conn.commit()

        # ----------------------------------------------------
        # SUMMARY
        # ----------------------------------------------------

        show_summary(
            cursor
        )

        cursor.close()

        print()
        print(
            "MULTI-SOURCE WAREHOUSE "
            "LOADED SUCCESSFULLY"
        )

    except Exception as e:

        conn.rollback()

        print()
        print(
            f"ERROR: {e}"
        )

        raise

    finally:

        conn.close()


# ============================================================
# ENTRY
# ============================================================

if __name__ == "__main__":
    main()