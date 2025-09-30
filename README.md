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
/* ============================================================
   1) FOREIGN KEYS — enforce relationships between tables
   ============================================================ */

-- Link each order to a valid customer
ALTER TABLE orders
  ADD CONSTRAINT fk_orders_customer
  FOREIGN KEY (customer_id)                  -- child column in orders
  REFERENCES customers (customer_id);        -- parent column in customers

-- Link each order_item to a valid order
ALTER TABLE order_items
  ADD CONSTRAINT fk_order_items_order
  FOREIGN KEY (order_id)
  REFERENCES orders (order_id);

-- Link each order_item to a valid product
ALTER TABLE order_items
  ADD CONSTRAINT fk_order_items_product
  FOREIGN KEY (product_id)
  REFERENCES products (product_id);

-- Link each order_item to a valid seller
ALTER TABLE order_items
  ADD CONSTRAINT fk_order_items_seller
  FOREIGN KEY (seller_id)
  REFERENCES sellers (seller_id);

-- Link each order_review to a valid order
ALTER TABLE order_reviews
  ADD CONSTRAINT fk_order_reviews_order
  FOREIGN KEY (order_id)
  REFERENCES orders (order_id);

-- Link each payment to a valid order
ALTER TABLE payments
  ADD CONSTRAINT fk_payments_order
  FOREIGN KEY (order_id)
  REFERENCES orders (order_id);



/* ============================================================
   2) CUSTOMERS TABLE: inspect column data types & nullability
   ============================================================ */

SELECT 
    column_name,                -- column name (e.g., customer_id)
    data_type,                  -- SQL type (e.g., varchar, integer)
    is_nullable,                -- 'YES' if column can be NULL
    character_maximum_length    -- length for char/varchar types
FROM information_schema.columns
WHERE table_name = 'customers'  -- limit to the 'customers' table
ORDER BY ordinal_position;      -- show columns in physical order



/* ============================================================
   3) ORDERS DATE RANGE: first & last purchase timestamps
   ============================================================ */

SELECT 
    MIN(order_purchase_timestamp) AS first_order,  -- earliest purchase
    MAX(order_purchase_timestamp) AS last_order    -- latest purchase
FROM orders;                                       -- from all orders



/* ============================================================
   4) DISTINCT CITIES/STATES among customers who actually ordered
   ============================================================ */

SELECT 
    COUNT(DISTINCT c.customer_city)  AS cities,    -- unique cities
    COUNT(DISTINCT c.customer_state) AS states     -- unique states
FROM customers c
JOIN orders o ON o.customer_id = c.customer_id;    -- only customers with orders



/* ============================================================
   5) ANNUAL TREND: number of orders per year
   ============================================================ */

SELECT
  EXTRACT(YEAR FROM order_purchase_timestamp)::int AS year,  -- pull year
  COUNT(*) AS orders_count                                   -- count orders
FROM orders
WHERE order_purchase_timestamp IS NOT NULL                   -- ignore NULLs
GROUP BY 1                                                   -- group by year
ORDER BY 1;                                                  -- sort by year



/* ============================================================
   6) MONTHLY SEASONALITY: total orders by calendar month (all years)
   ============================================================ */

SELECT
  TO_CHAR(order_purchase_timestamp, 'Mon') AS month_name,    -- Jan, Feb, ...
  EXTRACT(MONTH FROM order_purchase_timestamp)::int AS month_num, -- 1..12
  COUNT(*) AS orders_count                                   -- orders in that calendar month (across years)
FROM orders
WHERE order_purchase_timestamp IS NOT NULL
GROUP BY 1,2                                                 -- group by name & number
ORDER BY month_num;                                          -- sort chronologically



/* ============================================================
   7) DAYPART: time of day when orders are placed
   ------------------------------------------------------------
   Dawn: 0-6, Morning: 7-12, Afternoon: 13-18, Night: 19-23
   ============================================================ */

WITH buckets AS (                                  -- CTE to assign a label
  SELECT
    CASE
      WHEN EXTRACT(HOUR FROM order_purchase_timestamp) BETWEEN 0  AND 6  THEN 'Dawn'
      WHEN EXTRACT(HOUR FROM order_purchase_timestamp) BETWEEN 7  AND 12 THEN 'Morning'
      WHEN EXTRACT(HOUR FROM order_purchase_timestamp) BETWEEN 13 AND 18 THEN 'Afternoon'
      ELSE 'Night'
    END AS daypart
  FROM orders
  WHERE order_purchase_timestamp IS NOT NULL
)
SELECT daypart, COUNT(*) AS orders_count           -- count per label
FROM buckets
GROUP BY daypart
ORDER BY orders_count DESC;                        -- most common first



/* ============================================================
   8) MoM ORDERS PER STATE: count orders by state & month
   ============================================================ */

SELECT
  c.customer_state AS state,                                -- state code
  date_trunc('month', o.order_purchase_timestamp)::date AS month, -- first day of month
  COUNT(*) AS orders_count
FROM orders o
JOIN customers c ON c.customer_id = o.customer_id
WHERE o.order_purchase_timestamp IS NOT NULL
GROUP BY 1,2
ORDER BY 1,2;                                               -- sort by state, month



/* ============================================================
   9) UNIQUE CUSTOMERS PER STATE (who ever ordered)
   ============================================================ */

SELECT
  c.customer_state AS state,                     -- state code
  COUNT(DISTINCT o.customer_id) AS unique_customers  -- distinct customers
FROM orders o
JOIN customers c ON c.customer_id = o.customer_id
GROUP BY 1
ORDER BY unique_customers DESC;                  -- largest bases first



/* ============================================================
   10) % INCREASE IN COST from 2017 to 2018 (Jan–Aug), using payments
   ------------------------------------------------------------
   - Sum payments per order
   - Sum by year for Jan–Aug
   - Compute % change
   ============================================================ */

WITH order_payment AS (                          -- sum all payments per order
  SELECT order_id, SUM(payment_value) AS order_payment_value
  FROM payments
  GROUP BY order_id
),
yr AS (                                          -- sum those per year (Jan–Aug only)
  SELECT
    EXTRACT(YEAR FROM o.order_purchase_timestamp)::int AS yr,   -- 2017, 2018
    SUM(op.order_payment_value) AS total_payment_value          -- total value
  FROM orders o
  JOIN order_payment op ON op.order_id = o.order_id
  WHERE o.order_purchase_timestamp >= DATE '2017-01-01'         -- from 2017-01-01
    AND o.order_purchase_timestamp <  DATE '2018-09-01'         -- up to 2018-08-31
    AND EXTRACT(MONTH FROM o.order_purchase_timestamp) BETWEEN 1 AND 8 -- Jan..Aug
  GROUP BY 1
)
SELECT
  (SELECT total_payment_value FROM yr WHERE yr = 2017) AS total_2017_JanAug,  -- 2017 total
  (SELECT total_payment_value FROM yr WHERE yr = 2018) AS total_2018_JanAug,  -- 2018 total
  ROUND(                                                                      -- percentage change
    (
      (SELECT total_payment_value FROM yr WHERE yr = 2018)
      - (SELECT total_payment_value FROM yr WHERE yr = 2017)
    )
    / NULLIF((SELECT total_payment_value FROM yr WHERE yr = 2017),0) * 100.0
  ,2) AS pct_increase_JanAug_2017_to_2018;



/* ============================================================
   11) ORDER PRICE per STATE (total & average)
   ------------------------------------------------------------
   - Approximate "price" as total payments per order
   ============================================================ */

WITH order_payment AS (                           -- sum payments per order
  SELECT order_id, SUM(payment_value) AS order_payment_value
  FROM payments
  GROUP BY order_id
),
order_state AS (                                  -- map each order to a state
  SELECT o.order_id, c.customer_state
  FROM orders o
  JOIN customers c ON c.customer_id = o.customer_id
)
SELECT
  os.customer_state AS state,                             -- state code
  SUM(op.order_payment_value)                   AS total_order_price, -- total by state
  ROUND(AVG(op.order_payment_value)::numeric,2) AS avg_order_price    -- mean order value
FROM order_payment op
JOIN order_state os ON os.order_id = op.order_id
GROUP BY 1
ORDER BY total_order_price DESC;                          -- top revenue states



/* ============================================================
   12) FREIGHT per STATE (total & average)
   ------------------------------------------------------------
   - Sum freight per order from order_items
   - Then roll up by state
   ============================================================ */

WITH order_freight AS (                           -- freight per order
  SELECT order_id, SUM(freight_value) AS order_freight_value
  FROM order_items
  GROUP BY order_id
),
order_state AS (                                  -- map orders to state
  SELECT o.order_id, c.customer_state
  FROM orders o
  JOIN customers c ON c.customer_id = o.customer_id
)
SELECT
  os.customer_state AS state,                              -- state code
  SUM(ofr.order_freight_value)                   AS total_freight,     -- total freight
  ROUND(AVG(ofr.order_freight_value)::numeric,2) AS avg_freight        -- avg per order
FROM order_freight ofr
JOIN order_state os ON os.order_id = ofr.order_id
GROUP BY 1
ORDER BY total_freight DESC;                                -- most shipping spend



/* ============================================================
   13) DELIVERY TIME & DIFF vs ESTIMATE (per order)
   ------------------------------------------------------------
   - time_to_deliver = delivered - purchase
   - diff_estimated_delivery = delivered - estimated
     > 0 = late vs estimate, < 0 = earlier than estimate
   ============================================================ */

SELECT
  o.order_id,                                                        -- order id
  (o.order_delivered_customer_date - o.order_purchase_timestamp)      AS time_to_deliver_interval,   -- interval
  EXTRACT(EPOCH FROM (o.order_delivered_customer_date - o.order_purchase_timestamp)) / 86400.0
      AS time_to_deliver_days,                                       -- interval in days (float)
  (o.order_delivered_customer_date - o.order_estimated_delivery_date) AS diff_estimated_delivery_interval, -- interval
  EXTRACT(EPOCH FROM (o.order_delivered_customer_date - o.order_estimated_delivery_date)) / 86400.0
      AS diff_estimated_delivery_days                                 -- +late / -early vs estimate
FROM orders o
WHERE o.order_delivered_customer_date IS NOT NULL
  AND o.order_purchase_timestamp      IS NOT NULL
  AND o.order_estimated_delivery_date IS NOT NULL;                    -- ensure valid math



/* ============================================================
   14) TOP 5 STATES: highest & lowest avg freight value
   ============================================================ */

WITH order_freight AS (                                           -- freight per order
  SELECT order_id, SUM(freight_value) AS order_freight_value
  FROM order_items
  GROUP BY order_id
),
order_state AS (                                                  -- map to state
  SELECT o.order_id, c.customer_state
  FROM orders o
  JOIN customers c ON c.customer_id = o.customer_id
),
state_freight AS (                                                -- avg per state
  SELECT os.customer_state AS state,
         AVG(ofr.order_freight_value) AS avg_freight
  FROM order_freight ofr
  JOIN order_state os ON os.order_id = ofr.order_id
  GROUP BY 1
),
highest AS (                                                      -- top 5 highest avg
  SELECT 'highest' AS kind, state, ROUND(avg_freight::numeric,2) AS avg_freight
  FROM state_freight
  ORDER BY avg_freight DESC
  LIMIT 5
),
lowest AS (                                                       -- top 5 lowest avg
  SELECT 'lowest' AS kind, state, ROUND(avg_freight::numeric,2) AS avg_freight
  FROM state_freight
  ORDER BY avg_freight ASC
  LIMIT 5
)
SELECT * FROM highest
UNION ALL
SELECT * FROM lowest;                                             -- show both lists



/* ============================================================
   15) TOP 5 STATES: highest & lowest avg delivery time (days)
   ============================================================ */

WITH order_state AS (                                   -- map orders to state
  SELECT o.order_id, c.customer_state
  FROM orders o
  JOIN customers c ON c.customer_id = o.customer_id
),
order_delivery AS (                                     -- delivery time per order (days)
  SELECT
    order_id,
    EXTRACT(EPOCH FROM (order_delivered_customer_date - order_purchase_timestamp)) / 86400.0
      AS delivery_days
  FROM orders
  WHERE order_delivered_customer_date IS NOT NULL
    AND order_purchase_timestamp      IS NOT NULL
),
state_delivery AS (                                     -- average per state
  SELECT os.customer_state AS state,
         AVG(od.delivery_days) AS avg_delivery_days
  FROM order_delivery od
  JOIN order_state os ON os.order_id = od.order_id
  GROUP BY 1
),
highest AS (                                            -- rank longest averages
  SELECT
    'highest' AS kind,
    state,
    ROUND(avg_delivery_days::numeric, 2) AS avg_delivery_days,
    ROW_NUMBER() OVER (ORDER BY avg_delivery_days DESC, state) AS rnk
  FROM state_delivery
),
lowest AS (                                             -- rank shortest averages
  SELECT
    'lowest' AS kind,
    state,
    ROUND(avg_delivery_days::numeric, 2) AS avg_delivery_days,
    ROW_NUMBER() OVER (ORDER BY avg_delivery_days ASC, state) AS rnk
  FROM state_delivery
)
SELECT kind, rnk, state, avg_delivery_days
FROM (
  SELECT kind, rnk, state, avg_delivery_days, 0 AS sort_block
  FROM highest
  WHERE rnk <= 5

  UNION ALL

  SELECT kind, rnk, state, avg_delivery_days, 1 AS sort_block
  FROM lowest
  WHERE rnk <= 5
) u
ORDER BY sort_block, rnk;                               -- show highest then lowest



/* ============================================================
   16) TOP 5 STATES: fastest vs estimate (positive = earlier)
   ------------------------------------------------------------
   early_days = estimated - delivered (in days)
   > 0 => delivered BEFORE estimate; < 0 => after estimate
   ============================================================ */

WITH order_state AS (                                               -- map to state
  SELECT o.order_id, c.customer_state
  FROM orders o JOIN customers c ON c.customer_id = o.customer_id
),
order_early AS (                                                    -- +days = earlier
  SELECT
    order_id,
    EXTRACT(EPOCH FROM (order_estimated_delivery_date - order_delivered_customer_date)) / 86400.0
      AS early_days
  FROM orders
  WHERE order_delivered_customer_date IS NOT NULL
    AND order_estimated_delivery_date IS NOT NULL
),
state_early AS (                                                    -- avg by state
  SELECT os.customer_state AS state,
         AVG(oe.early_days) AS avg_days_early
  FROM order_early oe
  JOIN order_state os ON os.order_id = oe.order_id
  GROUP BY 1
)
SELECT 'fastest_vs_estimate' AS kind, state, ROUND(avg_days_early::numeric,2) AS avg_days_early
FROM state_early
ORDER BY avg_days_early DESC                                       -- most days early first
LIMIT 5;



/* ============================================================
   17) MoM ORDERS by PAYMENT TYPE
   ------------------------------------------------------------
   Count distinct orders per month per payment_type
   ============================================================ */

SELECT
  date_trunc('month', o.order_purchase_timestamp)::date AS month,  -- month bucket
  p.payment_type,                                                  -- e.g., credit_card
  COUNT(DISTINCT o.order_id) AS orders_count                       -- unique orders
FROM orders o
JOIN payments p ON p.order_id = o.order_id
WHERE o.order_purchase_timestamp IS NOT NULL
GROUP BY 1,2
ORDER BY 1,2;                                                      -- chronological then type



/* ============================================================
   18) ORDERS by NUMBER OF INSTALLMENTS
   ------------------------------------------------------------
   Each row = how many orders used N installments
   ============================================================ */

SELECT
  p.payment_installments,                -- installments count (0,1,2,...)
  COUNT(DISTINCT p.order_id) AS orders_count
FROM payments p
GROUP BY 1
ORDER BY 1;                              -- ascending by installments



/* ============================================================
   19) TOP 10 PRODUCT CATEGORIES by "revenue"
   ------------------------------------------------------------
   NOTE: This attributes the FULL order payment to EACH category
         present in the order (multi-category orders get double-counted).
         Use item-level subtotals to avoid double counting if needed.
   ============================================================ */

WITH order_payment AS (                                       -- sum payments per order
  SELECT order_id, SUM(payment_value) AS order_payment_value
  FROM payments GROUP BY order_id
),
order_category AS (                                           -- map orders to categories
  SELECT
    oi.order_id,
    pr.product_category_name
  FROM order_items oi
  JOIN products pr ON pr.product_id = oi.product_id
  WHERE pr.product_category_name IS NOT NULL
),
category_revenue AS (                                         -- sum "revenue" per category
  SELECT oc.product_category_name AS category,
         SUM(op.order_payment_value) AS revenue
  FROM order_category oc
  JOIN order_payment op ON op.order_id = oc.order_id
  GROUP BY 1
)
SELECT category, revenue
FROM category_revenue
ORDER BY revenue DESC
LIMIT 10;                                                     -- top 10 categories



/* ============================================================
   20) REPEAT PURCHASE RATE (customers with >= 2 orders)
   ============================================================ */

WITH cust_orders AS (                                 -- orders count per customer
  SELECT customer_id, COUNT(*) AS orders_cnt
  FROM orders
  GROUP BY customer_id
)
SELECT
  SUM(CASE WHEN orders_cnt >= 2 THEN 1 ELSE 0 END) AS repeat_customers,   -- # repeaters
  COUNT(*) AS total_customers_with_orders,                                 -- # customers overall
  ROUND(
    100.0 * SUM(CASE WHEN orders_cnt >= 2 THEN 1 ELSE 0 END) / NULLIF(COUNT(*),0)
  , 2) AS repeat_rate_pct                                                  -- % repeaters
FROM cust_orders;



/* ============================================================
   21) TOP 10 CITIES by orders + share of total
   ============================================================ */

WITH city_orders AS (                                       -- orders per city/state
  SELECT
    c.customer_city  AS city,
    c.customer_state AS state,
    COUNT(*)         AS orders_count
  FROM orders o
  JOIN customers c ON c.customer_id = o.customer_id
  WHERE o.order_purchase_timestamp IS NOT NULL
  GROUP BY 1,2
),
total AS (                                                  -- total orders (all cities)
  SELECT SUM(orders_count) AS total_orders
  FROM city_orders
)
SELECT
  co.city,                                                  -- city name
  co.state,                                                 -- state code
  co.orders_count,                                          -- orders from this city
  ROUND(100.0 * co.orders_count / NULLIF(t.total_orders,0), 2) AS orders_share_pct  -- % share
FROM city_orders co
CROSS JOIN total t                                          -- attach total to each row
ORDER BY co.orders_count DESC                               -- top cities first
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
