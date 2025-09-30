# E-commerce-Orders-Customer-Insights-Analysis (Case-Study)
📦 Target Orders — SQL Analytics 
We explore 100,000+ Target orders from Brazil across orders, payments, freight, delivery performance, product attributes, and customer reviews to uncover operational insights. The project ships with production-ready PostgreSQL queries, data quality checks, and a step-by-step guide to answer 20 practical business questions.

# Problem Statement

Target Brazil’s e-commerce team needs a reliable, reproducible way to analyze **100k+ historical orders** to answer core business questions across **demand, payments, logistics, product mix, and customer experience**. Today, insights are fragmented across ad-hoc spreadsheets and slow queries, making it hard to spot trends (seasonality, growth), quantify delivery performance vs. estimates, and prioritize operational fixes by **state, city, seller, category, and payment type**.

**Goal:** Build a SQL-first analytics pack (PostgreSQL) that enforces data integrity, runs fast on commodity hardware, and produces decision-ready metrics and trends for product, ops, and finance teams.

## Scope & Objectives

* **Integrity:** Add foreign keys and indexes to ensure trustworthy joins and performant aggregations.
* **Coverage:** Deliver ready-to-run queries for:

  * Order volume trends (yearly, monthly, daypart) and geographic distribution.
  * Revenue & AOV using item-level prices (and optional freight).
  * Freight totals/averages and delivery time vs. estimated SLA.
  * Payment mix (type, installments) and repeat-purchase behavior.
  * Category & city leaderboards with share of total.
* **Reusability:** Provide composable CTEs and patterns (e.g., `order_payment`, `order_state`) for rapid extensions.

## Key Questions

1. How has order volume evolved over time? Are there seasonal spikes?
2. Which states/cities drive orders, revenue, and high freight costs?
3. Are deliveries meeting or beating estimates? Where are delays concentrated?
4. What is the payment mix (types, installments) and how does it trend over months?
5. Which product categories generate the most revenue (without double-counting)?
6. What is the repeat purchase rate and how large is the loyal base?

## Data & Assumptions

* Tables: `customers`, `orders`, `order_items`, `order_reviews`, `payments`, `products`, `sellers`.
* Order-level revenue is computed from **item-level price** (optionally `+ freight_value`) to avoid over-attributing payments to multiple categories.
* Payments may have multiple rows per order → **aggregate by `order_id`**.
* Timestamps are analyzed as provided (single timezone assumed).

## Constraints & Risks

* Missing or null timestamps can bias delivery metrics → filter explicitly.
* Category or state sparsity may require minimum-volume thresholds.
* Freight may be influenced by outliers (oversized items, remote regions).

## Deliverables

* **FK & index script** to enforce referential integrity and speed.
* **Analysis SQL pack** answering the business questions above.
* **README usage guide** (how to run, extend, and interpret outputs).

## Success Criteria (Acceptance)

* All scripts run end-to-end on the dataset in < a few minutes on a standard laptop.
* Metrics are **consistent** across reruns and pass spot-checks (e.g., totals reconcile).
* Stakeholders can pinpoint top/bottom **states, cities, categories** and **delivery gaps** from query outputs without further munging.

## Out of Scope (v1)

* Real-time ingestion/streaming, ML forecasting, or BI dashboards (can be added later).
* Seller-level SLA root-cause analysis beyond aggregate delivery metrics

## We enforce clear foreign keys so all downstream metrics are trustworthy.

## ✅ Analysis Checklist (How we solve it, step by step)

## 0) Sanity checks (before analysis)
Row counts by table; duplicates on natural keys.
Null audits for key timestamps: purchase, delivered, estimated.
Payments roll up: sum by order_id because payments can be split into installments

# 1) Data types of all columns in customers
Goal: Confirm schema correctness for joins/filters.

SELECT column_name, data_type, is_nullable, character_maximum_length
FROM information_schema.columns
WHERE table_name = 'customers'
ORDER BY ordinal_position;


# 2) Time range (min/max) of orders
Goal: Know the analysis window.

SELECT MIN(order_purchase_timestamp) AS first_order,
       MAX(order_purchase_timestamp) AS last_order
FROM orders;


# 3) Distinct cities & states of customers who actually ordered
Goal: Real footprint (not just signups).

SELECT COUNT(DISTINCT c.customer_city)  AS cities,
       COUNT(DISTINCT c.customer_state) AS states
FROM customers c
JOIN orders o ON o.customer_id = c.customer_id;


# 4) Annual trend: orders per year
Goal: Growth lens.

SELECT EXTRACT(YEAR FROM order_purchase_timestamp)::int AS year,
       COUNT(*) AS orders_count
FROM orders
WHERE order_purchase_timestamp IS NOT NULL
GROUP BY 1
ORDER BY 1;


# 5) Monthly seasonality across all years
Goal: Peak months.

SELECT TO_CHAR(order_purchase_timestamp, 'Mon') AS month_name,
       EXTRACT(MONTH FROM order_purchase_timestamp)::int AS month_num,
       COUNT(*) AS orders_count
FROM orders
WHERE order_purchase_timestamp IS NOT NULL
GROUP BY 1,2
ORDER BY month_num;


# 6) Daypart mix (Dawn/Morning/Afternoon/Night)
Goal: When customers buy in a day.

WITH buckets AS (
  SELECT CASE
         WHEN EXTRACT(HOUR FROM order_purchase_timestamp) BETWEEN 0  AND 6  THEN 'Dawn'
         WHEN EXTRACT(HOUR FROM order_purchase_timestamp) BETWEEN 7  AND 12 THEN 'Morning'
         WHEN EXTRACT(HOUR FROM order_purchase_timestamp) BETWEEN 13 AND 18 THEN 'Afternoon'
         ELSE 'Night' END AS daypart
  FROM orders
  WHERE order_purchase_timestamp IS NOT NULL
)
SELECT daypart, COUNT(*) AS orders_count
FROM buckets
GROUP BY daypart
ORDER BY orders_count DESC;


# 7) MoM orders per state
Goal: Geographic trend lines.

SELECT c.customer_state AS state,
       date_trunc('month', o.order_purchase_timestamp)::date AS month,
       COUNT(*) AS orders_count
FROM orders o
JOIN customers c ON c.customer_id = o.customer_id
WHERE o.order_purchase_timestamp IS NOT NULL
GROUP BY 1,2
ORDER BY 1,2;


# 8) Customer distribution across states (unique buyers)
Goal: Market depth by state.

SELECT c.customer_state AS state,
       COUNT(DISTINCT o.customer_id) AS unique_customers
FROM orders o
JOIN customers c ON c.customer_id = o.customer_id
GROUP BY 1
ORDER BY unique_customers DESC;


# 9) % increase in order cost from 2017 → 2018 (Jan–Aug)
Goal: Revenue growth proxy using payments.payment_value.

WITH order_payment AS (
  SELECT order_id, SUM(payment_value) AS order_payment_value
  FROM payments GROUP BY order_id
),
yr AS (
  SELECT EXTRACT(YEAR FROM o.order_purchase_timestamp)::int AS yr,
         SUM(op.order_payment_value) AS total_payment_value
  FROM orders o
  JOIN order_payment op ON op.order_id = o.order_id
  WHERE o.order_purchase_timestamp >= DATE '2017-01-01'
    AND o.order_purchase_timestamp <  DATE '2018-09-01'
    AND EXTRACT(MONTH FROM o.order_purchase_timestamp) BETWEEN 1 AND 8
  GROUP BY 1
)
SELECT
  (SELECT total_payment_value FROM yr WHERE yr = 2017) AS total_2017_JanAug,
  (SELECT total_payment_value FROM yr WHERE yr = 2018) AS total_2018_JanAug,
  ROUND( ((SELECT total_payment_value FROM yr WHERE yr = 2018)
        - (SELECT total_payment_value FROM yr WHERE yr = 2017))
        / NULLIF((SELECT total_payment_value FROM yr WHERE yr = 2017),0) * 100.0, 2)
  AS pct_increase_JanAug_2017_to_2018;


# 10) Total & average order price per state
Goal: Revenue and AOV by state.

WITH order_payment AS (
  SELECT order_id, SUM(payment_value) AS order_payment_value
  FROM payments GROUP BY order_id
),
order_state AS (
  SELECT o.order_id, c.customer_state
  FROM orders o JOIN customers c ON c.customer_id = o.customer_id
)
SELECT os.customer_state AS state,
       SUM(op.order_payment_value)                   AS total_order_price,
       ROUND(AVG(op.order_payment_value)::numeric,2) AS avg_order_price
FROM order_payment op
JOIN order_state  os ON os.order_id = op.order_id
GROUP BY 1
ORDER BY total_order_price DESC;


# 11) Total & average order freight per state
Goal: Logistics intensity by state.

WITH order_freight AS (
  SELECT order_id, SUM(freight_value) AS order_freight_value
  FROM order_items GROUP BY order_id
),
order_state AS (
  SELECT o.order_id, c.customer_state
  FROM orders o JOIN customers c ON c.customer_id = o.customer_id
)
SELECT os.customer_state AS state,
       SUM(ofr.order_freight_value)                   AS total_freight,
       ROUND(AVG(ofr.order_freight_value)::numeric,2) AS avg_freight
FROM order_freight ofr
JOIN order_state  os ON os.order_id = ofr.order_id
GROUP BY 1
ORDER BY total_freight DESC;


# 12) Per-order delivery time & delta vs estimate
Goal: SLA adherence.

SELECT
  o.order_id,
  (o.order_delivered_customer_date - o.order_purchase_timestamp)      AS time_to_deliver_interval,
  EXTRACT(EPOCH FROM (o.order_delivered_customer_date - o.order_purchase_timestamp)) / 86400.0
      AS time_to_deliver_days,
  (o.order_delivered_customer_date - o.order_estimated_delivery_date) AS diff_estimated_delivery_interval,
  EXTRACT(EPOCH FROM (o.order_delivered_customer_date - o.order_estimated_delivery_date)) / 86400.0
      AS diff_estimated_delivery_days
FROM orders o
WHERE o.order_delivered_customer_date IS NOT NULL
  AND o.order_purchase_timestamp      IS NOT NULL
  AND o.order_estimated_delivery_date IS NOT NULL;


# 13) Top 5 states – highest & lowest avg freight

WITH order_freight AS (
  SELECT order_id, SUM(freight_value) AS order_freight_value
  FROM order_items GROUP BY order_id
),
order_state AS (
  SELECT o.order_id, c.customer_state
  FROM orders o JOIN customers c ON c.customer_id = o.customer_id
),
state_freight AS (
  SELECT os.customer_state AS state, AVG(ofr.order_freight_value) AS avg_freight
  FROM order_freight ofr JOIN order_state os ON os.order_id = ofr.order_id
  GROUP BY 1
),
highest AS (
  SELECT 'highest' AS kind, state, ROUND(avg_freight::numeric,2) AS avg_freight
  FROM state_freight ORDER BY avg_freight DESC LIMIT 5
),
lowest AS (
  SELECT 'lowest' AS kind, state, ROUND(avg_freight::numeric,2) AS avg_freight
  FROM state_freight ORDER BY avg_freight ASC LIMIT 5
)
SELECT * FROM highest
UNION ALL
SELECT * FROM lowest;


# 14) Top 5 states – highest & lowest avg delivery time

WITH order_state AS (
  SELECT o.order_id, c.customer_state
  FROM orders o JOIN customers c ON c.customer_id = o.customer_id
),
order_delivery AS (
  SELECT order_id,
         EXTRACT(EPOCH FROM (order_delivered_customer_date - order_purchase_timestamp)) / 86400.0
         AS delivery_days
  FROM orders
  WHERE order_delivered_customer_date IS NOT NULL
    AND order_purchase_timestamp      IS NOT NULL
),
state_delivery AS (
  SELECT os.customer_state AS state, AVG(od.delivery_days) AS avg_delivery_days
  FROM order_delivery od JOIN order_state os ON os.order_id = od.order_id
  GROUP BY 1
),
highest AS (
  SELECT 'highest' AS kind, state, ROUND(avg_delivery_days::numeric, 2) AS avg_delivery_days,
         ROW_NUMBER() OVER (ORDER BY avg_delivery_days DESC, state) AS rnk
  FROM state_delivery
),
lowest AS (
  SELECT 'lowest' AS kind, state, ROUND(avg_delivery_days::numeric, 2) AS avg_delivery_days,
         ROW_NUMBER() OVER (ORDER BY avg_delivery_days ASC, state) AS rnk
  FROM state_delivery
)
SELECT kind, rnk, state, avg_delivery_days
FROM (
  SELECT kind, rnk, state, avg_delivery_days, 0 AS sort_block FROM highest WHERE rnk <= 5
  UNION ALL
  SELECT kind, rnk, state, avg_delivery_days, 1 AS sort_block FROM lowest  WHERE rnk <= 5
) u
ORDER BY sort_block, rnk;


# 15) Top 5 states – fastest vs estimate (positive = earlier)

WITH order_state AS (
  SELECT o.order_id, c.customer_state
  FROM orders o JOIN customers c ON c.customer_id = o.customer_id
),
order_early AS (
  SELECT order_id,
         EXTRACT(EPOCH FROM (order_estimated_delivery_date - order_delivered_customer_date)) / 86400.0 AS early_days
  FROM orders
  WHERE order_delivered_customer_date IS NOT NULL
    AND order_estimated_delivery_date IS NOT NULL
),
state_early AS (
  SELECT os.customer_state AS state, AVG(oe.early_days) AS avg_days_early
  FROM order_early oe JOIN order_state os ON os.order_id = oe.order_id
  GROUP BY 1
)
SELECT 'fastest_vs_estimate' AS kind, state, ROUND(avg_days_early::numeric,2) AS avg_days_early
FROM state_early
ORDER BY avg_days_early DESC
LIMIT 5;


# 16) MoM orders by payment type

SELECT date_trunc('month', o.order_purchase_timestamp)::date AS month,
       p.payment_type,
       COUNT(DISTINCT o.order_id) AS orders_count
FROM orders o
JOIN payments p ON p.order_id = o.order_id
WHERE o.order_purchase_timestamp IS NOT NULL
GROUP BY 1,2
ORDER BY 1,2;


# 17) Orders by payment installments

SELECT p.payment_installments, COUNT(DISTINCT p.order_id) AS orders_count
FROM payments p
GROUP BY 1
ORDER BY 1;


# 18) Top 10 product categories by revenue — ✅ corrected

WITH item_revenue AS (
  SELECT
    oi.order_id,
    pr.product_category_name AS category,
    (oi.price + oi.freight_value) AS revenue_item
  FROM order_items oi
  JOIN products pr ON pr.product_id = oi.product_id
  WHERE pr.product_category_name IS NOT NULL
)
SELECT category,
       SUM(revenue_item) AS revenue
FROM item_revenue
GROUP BY 1
ORDER BY revenue DESC
LIMIT 10;


# 19) Repeat purchase rate (customers with ≥ 2 orders)

WITH cust_orders AS (
  SELECT customer_id, COUNT(*) AS orders_cnt
  FROM orders GROUP BY customer_id
)
SELECT
  SUM(CASE WHEN orders_cnt >= 2 THEN 1 ELSE 0 END) AS repeat_customers,
  COUNT(*) AS total_customers_with_orders,
  ROUND(100.0 * SUM(CASE WHEN orders_cnt >= 2 THEN 1 ELSE 0 END) / NULLIF(COUNT(*),0), 2) AS repeat_rate_pct
FROM cust_orders;


# 20) Top 10 cities by orders + share of total

WITH city_orders AS (
  SELECT c.customer_city AS city, c.customer_state AS state, COUNT(*) AS orders_count
  FROM orders o
  JOIN customers c ON c.customer_id = o.customer_id
  WHERE o.order_purchase_timestamp IS NOT NULL
  GROUP BY 1,2
),
total AS (SELECT SUM(orders_count) AS total_orders FROM city_orders)
SELECT co.city, co.state, co.orders_count,
       ROUND(100.0 * co.orders_count / NULLIF(t.total_orders,0), 2) AS orders_share_pct
FROM city_orders co
CROSS JOIN total t
ORDER BY co.orders_count DESC
LIMIT 10;

# 🧪 Data Quality & Edge Cases

Installments: payments may have multiple rows per order → always SUM by order_id for order-level metrics.
Multiple categories per order: Allocate revenue to items, not orders (see Q18 fix).
Null timestamps: Filter IS NOT NULL where needed to avoid skew.
Late vs early deliveries: Use consistent sign conventions (we used positive = earlier than estimate in Q15).
Timezone: Timestamps assumed in a single timezone (dataset native). If mixing timezones, normalize first.

# 📈 What to Look For (Interpretation Guide)

Q4/Q5: Is growth driven by certain months? Consider promotions/shipping cutoffs.
Q6: If “Night” is strong, push late-evening ads and ops staffing.
Q10/Q11: High revenue + high freight states → review pricing & route optimization.
Q12–Q15: If delivery is often later than estimate in some states, adjust SLA or partner mix.
Q16/Q17: Payment types/terms inform checkout UX and risk models.
Q18: Winning categories guide assortment, inventory safety stock, and merchandising.
Q19: Low repeat rate → focus on post-purchase emails, CX fixes, and loyalty programs.
Q20: Top cities → micro-fulfillment or regional promos.
