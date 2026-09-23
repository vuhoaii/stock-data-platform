DROP MATERIALIZED VIEW IF EXISTS mart_stock_signals;

CREATE MATERIALIZED VIEW mart_stock_signals AS

WITH base AS (

    SELECT
        full_date,
        symbol,
        close_price,
        volume,
        canonical_source,

        LAG(close_price, 1) OVER (
            PARTITION BY symbol
            ORDER BY full_date
        ) AS prev_close,

        LAG(close_price, 5) OVER (
            PARTITION BY symbol
            ORDER BY full_date
        ) AS close_5d_ago,

        LAG(close_price, 20) OVER (
            PARTITION BY symbol
            ORDER BY full_date
        ) AS close_20d_ago,

        AVG(close_price) OVER (
            PARTITION BY symbol
            ORDER BY full_date
            ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
        ) AS ma5,

        AVG(close_price) OVER (
            PARTITION BY symbol
            ORDER BY full_date
            ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
        ) AS ma20,

        AVG(volume) OVER (
            PARTITION BY symbol
            ORDER BY full_date
            ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
        ) AS volume_ma20,

        COUNT(*) OVER (
            PARTITION BY symbol
            ORDER BY full_date
            ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
        ) AS ma5_periods,

        COUNT(*) OVER (
            PARTITION BY symbol
            ORDER BY full_date
            ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
        ) AS ma20_periods

    FROM mart_stock_price_canonical
),

metrics AS (

    SELECT
        full_date,
        symbol,
        close_price,
        volume,
        canonical_source,

        CASE
            WHEN prev_close IS NULL
                OR prev_close = 0
            THEN NULL

            ELSE
                ROUND(
                    (
                        (
                            close_price
                            - prev_close
                        )
                        / prev_close
                        * 100
                    )::numeric,
                    4
                )
        END AS daily_return_pct,

        CASE
            WHEN ma5_periods = 5
            THEN ROUND(
                ma5::numeric,
                4
            )
            ELSE NULL
        END AS ma5,

        CASE
            WHEN ma20_periods = 20
            THEN ROUND(
                ma20::numeric,
                4
            )
            ELSE NULL
        END AS ma20,

        CASE
            WHEN ma20_periods = 20
            THEN ROUND(
                volume_ma20::numeric,
                2
            )
            ELSE NULL
        END AS volume_ma20,

        CASE
            WHEN ma20_periods = 20
                AND volume_ma20 > 0
            THEN ROUND(
                (
                    volume
                    / volume_ma20
                )::numeric,
                4
            )
            ELSE NULL
        END AS volume_ratio,

        CASE
            WHEN close_5d_ago IS NULL
                OR close_5d_ago = 0
            THEN NULL

            ELSE
                ROUND(
                    (
                        (
                            close_price
                            - close_5d_ago
                        )
                        / close_5d_ago
                        * 100
                    )::numeric,
                    4
                )
        END AS momentum_5d,

        CASE
            WHEN close_20d_ago IS NULL
                OR close_20d_ago = 0
            THEN NULL

            ELSE
                ROUND(
                    (
                        (
                            close_price
                            - close_20d_ago
                        )
                        / close_20d_ago
                        * 100
                    )::numeric,
                    4
                )
        END AS momentum_20d

    FROM base
)

SELECT
    full_date,
    symbol,

    close_price,
    volume,

    daily_return_pct,

    ma5,
    ma20,

    volume_ma20,
    volume_ratio,

    momentum_5d,
    momentum_20d,

    CASE
        WHEN ma5 IS NULL
            OR ma20 IS NULL
        THEN 'INSUFFICIENT_DATA'

        WHEN ma5 > ma20
        THEN 'UPTREND'

        WHEN ma5 < ma20
        THEN 'DOWNTREND'

        ELSE 'SIDEWAYS'
    END AS price_trend,

    CASE
        WHEN volume_ratio IS NULL
        THEN 'INSUFFICIENT_DATA'

        WHEN volume_ratio >= 1.5
        THEN 'HIGH_VOLUME'

        WHEN volume_ratio >= 1.0
        THEN 'ABOVE_AVERAGE'

        ELSE 'BELOW_AVERAGE'
    END AS volume_signal,

    CASE

        WHEN
            ma5 IS NULL
            OR ma20 IS NULL
            OR momentum_5d IS NULL
            OR volume_ratio IS NULL
        THEN 'INSUFFICIENT_DATA'

        WHEN
            ma5 > ma20
            AND momentum_5d > 0
            AND volume_ratio >= 1.0
        THEN 'BULLISH_SIGNAL'

        WHEN
            ma5 < ma20
            AND momentum_5d < 0
        THEN 'BEARISH_SIGNAL'

        ELSE 'NEUTRAL'

    END AS trading_signal,

    canonical_source

FROM metrics;


CREATE UNIQUE INDEX
IF NOT EXISTS idx_mart_stock_signals_unique
ON mart_stock_signals (
    full_date,
    symbol
);


CREATE INDEX
IF NOT EXISTS idx_mart_stock_signals_symbol
ON mart_stock_signals (
    symbol
);


CREATE INDEX
IF NOT EXISTS idx_mart_stock_signals_date
ON mart_stock_signals (
    full_date
);