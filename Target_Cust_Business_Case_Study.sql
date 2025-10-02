ALTER TABLE orders
  ADD CONSTRAINT fk_orders_customer
  FOREIGN KEY (customer_id) REFERENCES customers (customer_id);

ALTER TABLE order_items
  ADD CONSTRAINT fk_order_items_order
  FOREIGN KEY (order_id) REFERENCES orders (order_id);

ALTER TABLE order_items
  ADD CONSTRAINT fk_order_items_product
  FOREIGN KEY (product_id) REFERENCES products (product_id);

ALTER TABLE order_items
  ADD CONSTRAINT fk_order_items_seller
  FOREIGN KEY (seller_id) REFERENCES sellers (seller_id);

ALTER TABLE order_reviews
  ADD CONSTRAINT fk_order_reviews_order
  FOREIGN KEY (order_id) REFERENCES orders (order_id);

ALTER TABLE payments
  ADD CONSTRAINT fk_payments_order
  FOREIGN KEY (order_id) REFERENCES orders (order_id);





-- Data types for customers
SELECT 
    column_name,
    data_type,
    is_nullable,
    character_maximum_length
FROM information_schema.columns
WHERE table_name = 'customers'
ORDER BY ordinal_position;


-- Earliest and latest purchase timestamps
SELECT 
    MIN(order_purchase_timestamp) AS first_order,
    MAX(order_purchase_timestamp) AS last_order
FROM orders;


-- Distinct cities & states among customers who actually placed orders
SELECT 
    COUNT(DISTINCT c.customer_city)  AS cities,
    COUNT(DISTINCT c.customer_state) AS states
FROM customers c
JOIN orders o ON o.customer_id = c.customer_id;


-- Annual trend: number of orders per year (is there growth?)
SELECT
  EXTRACT(YEAR FROM order_purchase_timestamp)::int AS year,
  COUNT(*) AS orders_count
FROM orders
WHERE order_purchase_timestamp IS NOT NULL
GROUP BY 1
ORDER BY 1;


-- Monthly seasonality: total orders by calendar month (across all years)
SELECT
  TO_CHAR(order_purchase_timestamp, 'Mon') AS month_name,
  EXTRACT(MONTH FROM order_purchase_timestamp)::int AS month_num,
  COUNT(*) AS orders_count
FROM orders
WHERE order_purchase_timestamp IS NOT NULL
GROUP BY 1,2
ORDER BY month_num;


-- Time of day when orders are placed (Dawn/Morning/Afternoon/Night)
-- Dawn: 0-6, Morning: 7-12, Afternoon: 13-18, Night: 19-23
WITH buckets AS (
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
SELECT daypart, COUNT(*) AS orders_count
FROM buckets
GROUP BY daypart
ORDER BY orders_count DESC;


-- Month-on-month number of orders per state (customer state)
SELECT
  c.customer_state AS state,
  date_trunc('month', o.order_purchase_timestamp)::date AS month,
  COUNT(*) AS orders_count
FROM orders o
JOIN customers c ON c.customer_id = o.customer_id
WHERE o.order_purchase_timestamp IS NOT NULL
GROUP BY 1,2
ORDER BY 1,2;


-- Customer distribution across states (unique customers who ever ordered)
SELECT
  c.customer_state AS state,
  COUNT(DISTINCT o.customer_id) AS unique_customers
FROM orders o
JOIN customers c ON c.customer_id = o.customer_id
GROUP BY 1
ORDER BY unique_customers DESC;


-- % increase in cost of orders from 2017 to 2018 (Jan–Aug only), using payments.payment_value
WITH order_payment AS (
  SELECT order_id, SUM(payment_value) AS order_payment_value
  FROM payments
  GROUP BY order_id
),
yr AS (
  SELECT
    EXTRACT(YEAR FROM o.order_purchase_timestamp)::int AS yr,
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
  ROUND(
    (
      (SELECT total_payment_value FROM yr WHERE yr = 2018)
      - (SELECT total_payment_value FROM yr WHERE yr = 2017)
    )
    / NULLIF((SELECT total_payment_value FROM yr WHERE yr = 2017),0) * 100.0
  ,2) AS pct_increase_JanAug_2017_to_2018;


 -- Total & average order price per state (price ≈ total payment per order)
WITH order_payment AS (
  SELECT order_id, SUM(payment_value) AS order_payment_value
  FROM payments
  GROUP BY order_id
),
order_state AS (
  SELECT o.order_id, c.customer_state
  FROM orders o
  JOIN customers c ON c.customer_id = o.customer_id
)
SELECT
  os.customer_state AS state,
  SUM(op.order_payment_value)                    AS total_order_price,
  ROUND(AVG(op.order_payment_value)::numeric,2)  AS avg_order_price
FROM order_payment op
JOIN order_state os ON os.order_id = op.order_id
GROUP BY 1
ORDER BY total_order_price DESC;


-- Total & average order freight per state
WITH order_freight AS (
  SELECT order_id, SUM(freight_value) AS order_freight_value
  FROM order_items
  GROUP BY order_id
),
order_state AS (
  SELECT o.order_id, c.customer_state
  FROM orders o
  JOIN customers c ON c.customer_id = o.customer_id
)
SELECT
  os.customer_state AS state,
  SUM(ofr.order_freight_value)                   AS total_freight,
  ROUND(AVG(ofr.order_freight_value)::numeric,2) AS avg_freight
FROM order_freight ofr
JOIN order_state os ON os.order_id = ofr.order_id
GROUP BY 1
ORDER BY total_freight DESC;


-- Per-order delivery time and difference vs estimate (in days and intervals)
-- time_to_deliver = delivered_customer_date - purchase_timestamp
-- diff_estimated_delivery = delivered_customer_date - estimated_delivery_date
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

-- Top 5 states with highest & lowest average freight value
WITH order_freight AS (
  SELECT order_id, SUM(freight_value) AS order_freight_value
  FROM order_items
  GROUP BY order_id
),
order_state AS (
  SELECT o.order_id, c.customer_state
  FROM orders o
  JOIN customers c ON c.customer_id = o.customer_id
),
state_freight AS (
  SELECT os.customer_state AS state,
         AVG(ofr.order_freight_value) AS avg_freight
  FROM order_freight ofr
  JOIN order_state os ON os.order_id = ofr.order_id
  GROUP BY 1
),
highest AS (
  SELECT 'highest' AS kind, state, ROUND(avg_freight::numeric,2) AS avg_freight
  FROM state_freight
  ORDER BY avg_freight DESC
  LIMIT 5
),
lowest AS (
  SELECT 'lowest' AS kind, state, ROUND(avg_freight::numeric,2) AS avg_freight
  FROM state_freight
  ORDER BY avg_freight ASC
  LIMIT 5
)
SELECT * FROM highest
UNION ALL
SELECT * FROM lowest;


-- Top 5 states with highest & lowest average delivery time (days)
WITH order_state AS (
  SELECT o.order_id, c.customer_state
  FROM orders o
  JOIN customers c ON c.customer_id = o.customer_id
),
order_delivery AS (
  SELECT
    order_id,
    EXTRACT(EPOCH FROM (order_delivered_customer_date - order_purchase_timestamp)) / 86400.0
      AS delivery_days
  FROM orders
  WHERE order_delivered_customer_date IS NOT NULL
    AND order_purchase_timestamp      IS NOT NULL
),
state_delivery AS (
  SELECT os.customer_state AS state,
         AVG(od.delivery_days) AS avg_delivery_days
  FROM order_delivery od
  JOIN order_state os ON os.order_id = od.order_id
  GROUP BY 1
),
highest AS (
  SELECT
    'highest' AS kind,
    state,
    ROUND(avg_delivery_days::numeric, 2) AS avg_delivery_days,
    ROW_NUMBER() OVER (ORDER BY avg_delivery_days DESC, state) AS rnk
  FROM state_delivery
),
lowest AS (
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
ORDER BY sort_block, rnk;


-- Top 5 states where delivery is fastest vs the estimate (positive = earlier than estimate)
WITH order_state AS (
  SELECT o.order_id, c.customer_state
  FROM orders o JOIN customers c ON c.customer_id = o.customer_id
),
order_early AS (
  SELECT
    order_id,
    EXTRACT(EPOCH FROM (order_estimated_delivery_date - order_delivered_customer_date)) / 86400.0
      AS early_days  -- positive means delivered before estimate
  FROM orders
  WHERE order_delivered_customer_date IS NOT NULL
    AND order_estimated_delivery_date IS NOT NULL
),
state_early AS (
  SELECT os.customer_state AS state,
         AVG(oe.early_days) AS avg_days_early
  FROM order_early oe
  JOIN order_state os ON os.order_id = oe.order_id
  GROUP BY 1
)
SELECT 'fastest_vs_estimate' AS kind, state, ROUND(avg_days_early::numeric,2) AS avg_days_early
FROM state_early
ORDER BY avg_days_early DESC
LIMIT 5;


-- Month-on-month number of orders by payment type
SELECT
  date_trunc('month', o.order_purchase_timestamp)::date AS month,
  p.payment_type,
  COUNT(DISTINCT o.order_id) AS orders_count
FROM orders o
JOIN payments p ON p.order_id = o.order_id
WHERE o.order_purchase_timestamp IS NOT NULL
GROUP BY 1,2
ORDER BY 1,2;


-- Number of orders by payment installments
SELECT
  p.payment_installments,
  COUNT(DISTINCT p.order_id) AS orders_count
FROM payments p
GROUP BY 1
ORDER BY 1;


-- Top 10 product categories by revenue (requires products.product_category_name)
WITH order_payment AS (
  SELECT order_id, SUM(payment_value) AS order_payment_value
  FROM payments GROUP BY order_id
),
order_category AS (
  SELECT
    oi.order_id,
    pr.product_category_name
  FROM order_items oi
  JOIN products pr ON pr.product_id = oi.product_id
  WHERE pr.product_category_name IS NOT NULL
),
category_revenue AS (
  SELECT oc.product_category_name AS category,
         SUM(op.order_payment_value) AS revenue
  FROM order_category oc
  JOIN order_payment op ON op.order_id = oc.order_id
  GROUP BY 1
)
SELECT category, revenue
FROM category_revenue
ORDER BY revenue DESC
LIMIT 10;


-- Repeat purchase rate: customers with >=2 orders
WITH cust_orders AS (
  SELECT customer_id, COUNT(*) AS orders_cnt
  FROM orders
  GROUP BY customer_id
)
SELECT
  SUM(CASE WHEN orders_cnt >= 2 THEN 1 ELSE 0 END)       AS repeat_customers,
  COUNT(*)                                               AS total_customers_with_orders,
  ROUND(100.0 * SUM(CASE WHEN orders_cnt >= 2 THEN 1 ELSE 0 END) / NULLIF(COUNT(*),0), 2)
    AS repeat_rate_pct
FROM cust_orders;


-- Top 10 cities by number of orders, plus share of total orders

WITH city_orders AS (
  SELECT
    c.customer_city  AS city,
    c.customer_state AS state,
    COUNT(*)         AS orders_count
  FROM orders o
  JOIN customers c ON c.customer_id = o.customer_id
  WHERE o.order_purchase_timestamp IS NOT NULL
  GROUP BY 1,2
),
total AS (
  SELECT SUM(orders_count) AS total_orders
  FROM city_orders
)
SELECT
  co.city,
  co.state,
  co.orders_count,
  ROUND(100.0 * co.orders_count / NULLIF(t.total_orders,0), 2) AS orders_share_pct
FROM city_orders co
CROSS JOIN total t
ORDER BY co.orders_count DESC
LIMIT 10;