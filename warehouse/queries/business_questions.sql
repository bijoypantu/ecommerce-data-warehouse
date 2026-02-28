-- ============================================================
-- E-Commerce Data Warehouse
-- Business Questions — Analytical Queries
-- ============================================================

-- ------------------------------------------------------------
-- Q1 & Q2: Total orders, cancelled orders and cancellation
--           rate by month and year
-- ------------------------------------------------------------
SELECT
    dd.year,
    dd.month_number,
    dd.month_name,
    COUNT(fo.order_sk)                                                      AS total_orders,
    COUNT(*) FILTER (WHERE fo.order_status = 'cancelled')                   AS cancelled_orders,
    COUNT(*) FILTER (WHERE fo.order_status = 'cancelled')::NUMERIC
        / NULLIF(COUNT(*), 0)                                               AS cancellation_rate
FROM dw.fact_orders fo
JOIN dw.dim_date dd
    ON fo.date_sk = dd.date_sk
GROUP BY dd.year, dd.month_number, dd.month_name
ORDER BY dd.year, dd.month_number;


-- ------------------------------------------------------------
-- Q3: Net revenue (INR) across categories by month/year
--     (delivered orders only, minus processed refunds)
-- ------------------------------------------------------------
WITH cte_refund AS (
    SELECT
        order_item_sk,
        SUM(refund_amount_inr) AS total_refund_inr
    FROM dw.fact_refunds
    WHERE refund_status = 'processed'
    GROUP BY order_item_sk
)
SELECT
    dd.year,
    dd.month_number,
    dd.month_name,
    dc.category_sk,
    dc.category_name,
    SUM(foi.line_total_amount_inr)
        - SUM(COALESCE(cr.total_refund_inr, 0))                            AS net_revenue_inr
FROM dw.fact_order_items foi
JOIN dw.fact_orders fo
    ON fo.order_sk = foi.order_sk
JOIN dw.dim_date dd
    ON dd.date_sk = fo.date_sk
JOIN dw.dim_product dp
    ON dp.product_sk = foi.product_sk
JOIN dw.dim_category dc
    ON dc.category_sk = dp.category_sk
LEFT JOIN cte_refund cr
    ON cr.order_item_sk = foi.order_item_sk
WHERE fo.order_status = 'delivered'
GROUP BY dd.year, dd.month_number, dd.month_name, dc.category_sk, dc.category_name
ORDER BY dd.year, dd.month_number;


-- ------------------------------------------------------------
-- Q4: Average number of items per order by month/year
--     (delivered orders only)
-- ------------------------------------------------------------
WITH cte_item_cnt AS (
    SELECT
        foi.order_sk,
        COUNT(foi.order_item_sk) AS total_items_cnt
    FROM dw.fact_order_items foi
    GROUP BY foi.order_sk
)
SELECT
    dd.year,
    dd.month_number,
    dd.month_name,
    ROUND(AVG(cic.total_items_cnt), 2)                                      AS avg_items_per_order
FROM dw.fact_orders fo
JOIN dw.dim_date dd
    ON fo.date_sk = dd.date_sk
JOIN cte_item_cnt cic
    ON cic.order_sk = fo.order_sk
WHERE fo.order_status = 'delivered'
GROUP BY dd.year, dd.month_number, dd.month_name
ORDER BY dd.year, dd.month_number;


-- ------------------------------------------------------------
-- Q5: High-value customer segmentation
--     composite score = 0.7 * revenue_percentile + 0.3 * frequency_percentile
--     Based on last 12 months, delivered orders only
--
-- FIX APPLIED: Added WHERE dc.is_current = TRUE in customer_ltm CTE
-- so that SCD2 historical versions of customers are excluded from
-- the percentile calculation. Without this, a customer who changed
-- their address twice would appear as 3 separate customers,
-- corrupting all composite scores and segment labels.
-- ------------------------------------------------------------
WITH refund_agg AS (
    SELECT
        order_item_sk,
        SUM(refund_amount_inr) AS total_refund_inr
    FROM dw.fact_refunds
    WHERE refund_status = 'processed'
    GROUP BY order_item_sk
),
customer_ltm_active AS (
    SELECT
        fo.customer_sk,
        SUM(foi.line_total_amount_inr)
            - SUM(COALESCE(ra.total_refund_inr, 0))                        AS revenue_ltm,
        COUNT(DISTINCT fo.order_sk)                                         AS order_count_ltm
    FROM dw.fact_orders fo
    JOIN dw.fact_order_items foi
        ON foi.order_sk = fo.order_sk
    JOIN dw.dim_date dd
        ON dd.date_sk = fo.date_sk
    LEFT JOIN refund_agg ra
        ON ra.order_item_sk = foi.order_item_sk
    WHERE fo.order_status = 'delivered'
      AND dd.full_date >= date_trunc('month', DATE '2026-01-31') - INTERVAL '11 months'
      AND dd.full_date <= DATE '2026-01-31'
    GROUP BY fo.customer_sk
),
customer_ltm AS (
    SELECT
        dc.customer_sk,
        COALESCE(ca.revenue_ltm, 0)       AS revenue_ltm,
        COALESCE(ca.order_count_ltm, 0)   AS order_count_ltm
    FROM dw.dim_customer dc
    LEFT JOIN customer_ltm_active ca
        ON ca.customer_sk = dc.customer_sk
    -- CRITICAL: only one row per real customer; exclude SCD2 historical versions
    WHERE dc.is_current = TRUE
),
cust_percentile AS (
    SELECT
        customer_sk,
        revenue_ltm,
        order_count_ltm,
        PERCENT_RANK() OVER (ORDER BY revenue_ltm)        AS rev_perc,
        PERCENT_RANK() OVER (ORDER BY order_count_ltm)    AS freq_perc
    FROM customer_ltm
),
composite_score AS (
    SELECT
        customer_sk,
        revenue_ltm,
        order_count_ltm,
        rev_perc,
        freq_perc,
        (0.7 * rev_perc + 0.3 * freq_perc)               AS score
    FROM cust_percentile
),
cutoffs AS (
    SELECT
        PERCENTILE_CONT(0.8) WITHIN GROUP (ORDER BY score) AS high_cutoff,
        PERCENTILE_CONT(0.3) WITHIN GROUP (ORDER BY score) AS low_cutoff
    FROM composite_score
)
SELECT
    cs.customer_sk,
    cs.revenue_ltm,
    cs.order_count_ltm,
    cs.score                                              AS composite_score,
    CASE
        WHEN cs.score >= c.high_cutoff THEN 'high'
        WHEN cs.score <  c.low_cutoff  THEN 'low'
        ELSE 'medium'
    END                                                   AS segment_label
FROM composite_score cs
CROSS JOIN cutoffs c
ORDER BY cs.score DESC;


-- ------------------------------------------------------------
-- Q6: Top 10 products by net revenue (INR) in the last 5 years
--     (delivered orders only, minus processed refunds)
-- ------------------------------------------------------------
WITH refund_agg AS (
    SELECT
        order_item_sk,
        SUM(refund_amount_inr) AS total_refund_inr
    FROM dw.fact_refunds
    WHERE refund_status = 'processed'
    GROUP BY order_item_sk
),
product_revenue AS (
    SELECT
        dp.product_sk,
        dp.product_name,
        SUM(foi.line_total_amount_inr)
            - SUM(COALESCE(ra.total_refund_inr, 0))                        AS net_revenue_inr
    FROM dw.fact_order_items foi
    JOIN dw.fact_orders fo
        ON fo.order_sk = foi.order_sk
    JOIN dw.dim_date dd
        ON dd.date_sk = fo.date_sk
    JOIN dw.dim_product dp
        ON dp.product_sk = foi.product_sk
    LEFT JOIN refund_agg ra
        ON ra.order_item_sk = foi.order_item_sk
    WHERE fo.order_status = 'delivered'
      AND dd.full_date >= CURRENT_DATE - INTERVAL '5 years'
    GROUP BY dp.product_sk, dp.product_name
),
ranked_products AS (
    SELECT
        product_sk,
        product_name,
        net_revenue_inr,
        DENSE_RANK() OVER (ORDER BY net_revenue_inr DESC) AS revenue_rank
    FROM product_revenue
)
SELECT
    product_sk,
    product_name,
    net_revenue_inr,
    revenue_rank
FROM ranked_products
WHERE revenue_rank <= 10
ORDER BY revenue_rank, net_revenue_inr DESC;
