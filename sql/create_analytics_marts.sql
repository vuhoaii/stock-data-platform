-- ============================================================
-- 1. STOCK QUALITY SUMMARY
-- ============================================================

DROP MATERIALIZED VIEW IF EXISTS mart_stock_quality_summary;

CREATE MATERIALIZED VIEW mart_stock_quality_summary AS

SELECT
    stock_key,
    symbol,

    COUNT(*) AS trading_days,

    AVG(close_diff_pct) AS avg_close_diff_pct,
    MAX(close_diff_pct) AS max_close_diff_pct,

    AVG(volume_diff_pct) AS avg_volume_diff_pct,
    MAX(volume_diff_pct) AS max_volume_diff_pct,

    SUM(
        CASE
            WHEN price_discrepancy
            THEN 1
            ELSE 0
        END
    ) AS price_discrepancy_days,

    SUM(
        CASE
            WHEN volume_discrepancy
            THEN 1
            ELSE 0
        END
    ) AS volume_discrepancy_days,

    SUM(
        CASE
            WHEN has_discrepancy
            THEN 1
            ELSE 0
        END
    ) AS discrepancy_days,

    (
        SUM(
            CASE
                WHEN has_discrepancy
                THEN 1
                ELSE 0
            END
        )::NUMERIC
        /
        NULLIF(
            COUNT(*),
            0
        )
        * 100
    ) AS discrepancy_rate_pct

FROM mart_stock_price_canonical

GROUP BY
    stock_key,
    symbol;


CREATE UNIQUE INDEX
idx_mart_stock_quality_summary
ON mart_stock_quality_summary (
    stock_key
);


-- ============================================================
-- 2. LATEST STOCK SNAPSHOT
-- ============================================================

DROP MATERIALIZED VIEW IF EXISTS mart_stock_overview;

CREATE MATERIALIZED VIEW mart_stock_overview AS

WITH ranked_price AS (

    SELECT
        m.*,

        ROW_NUMBER() OVER (
            PARTITION BY stock_key
            ORDER BY full_date DESC
        ) AS rn

    FROM mart_stock_price_canonical m
),

latest_price AS (

    SELECT *

    FROM ranked_price

    WHERE rn = 1
),

price_history AS (

    SELECT
        stock_key,

        MAX(
            CASE
                WHEN rn = 2
                THEN close_price
            END
        ) AS previous_close,

        MAX(
            CASE
                WHEN rn = 6
                THEN close_price
            END
        ) AS close_5_sessions_ago,

        MAX(
            CASE
                WHEN rn = 21
                THEN close_price
            END
        ) AS close_20_sessions_ago

    FROM ranked_price

    GROUP BY stock_key
)

SELECT
    l.stock_key,
    l.symbol,

    l.full_date AS latest_trade_date,

    l.open_price,
    l.high_price,
    l.low_price,
    l.close_price,
    l.volume,

    l.canonical_source,

    l.close_vci,
    l.close_kbs,

    l.close_diff,
    l.close_diff_pct,

    l.volume_diff,
    l.volume_diff_pct,

    l.has_discrepancy,

    h.previous_close,

    CASE
        WHEN h.previous_close IS NOT NULL
         AND h.previous_close <> 0

        THEN (
            l.close_price
            - h.previous_close
        )
        /
        h.previous_close
        * 100

    END AS daily_return_pct,

    CASE
        WHEN h.close_5_sessions_ago IS NOT NULL
         AND h.close_5_sessions_ago <> 0

        THEN (
            l.close_price
            - h.close_5_sessions_ago
        )
        /
        h.close_5_sessions_ago
        * 100

    END AS return_5_sessions_pct,

    CASE
        WHEN h.close_20_sessions_ago IS NOT NULL
         AND h.close_20_sessions_ago <> 0

        THEN (
            l.close_price
            - h.close_20_sessions_ago
        )
        /
        h.close_20_sessions_ago
        * 100

    END AS return_20_sessions_pct,

    q.trading_days,

    q.avg_close_diff_pct,

    q.max_close_diff_pct,

    q.avg_volume_diff_pct,

    q.max_volume_diff_pct,

    q.discrepancy_days,

    q.discrepancy_rate_pct

FROM latest_price l

LEFT JOIN price_history h
    ON l.stock_key = h.stock_key

LEFT JOIN mart_stock_quality_summary q
    ON l.stock_key = q.stock_key;


CREATE UNIQUE INDEX
idx_mart_stock_overview
ON mart_stock_overview (
    stock_key
);


-- ============================================================
-- 3. LATEST FINANCIAL PERIOD PER STOCK
-- ============================================================

DROP MATERIALIZED VIEW IF EXISTS mart_financial_latest;

CREATE MATERIALIZED VIEW mart_financial_latest AS

WITH ranked_period AS (

    SELECT
        f.stock_key,
        f.period_key,

        ROW_NUMBER() OVER (
            PARTITION BY f.stock_key
            ORDER BY
                p.year DESC,
                p.quarter DESC NULLS LAST,
                p.period_key DESC
        ) AS rn

    FROM fact_financial_metric f

    JOIN dim_period p
        ON f.period_key = p.period_key

    GROUP BY
        f.stock_key,
        f.period_key,
        p.year,
        p.quarter,
        p.period_key
),

latest_period AS (

    SELECT
        stock_key,
        period_key

    FROM ranked_period

    WHERE rn = 1
)

SELECT
    f.stock_key,

    s.symbol,

    f.period_key,

    p.period_label,

    p.year,

    p.quarter,

    f.statement_type,

    f.metric,

    f.value,

    f.source,

    f.report_period_type,

    f.orientation

FROM fact_financial_metric f

JOIN latest_period lp
    ON f.stock_key = lp.stock_key
   AND f.period_key = lp.period_key

JOIN dim_stock s
    ON f.stock_key = s.stock_key

JOIN dim_period p
    ON f.period_key = p.period_key;


CREATE INDEX
idx_mart_financial_latest
ON mart_financial_latest (
    stock_key,
    statement_type,
    metric
);


-- ============================================================
-- FINISHED
-- ============================================================