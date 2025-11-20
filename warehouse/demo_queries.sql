SELECT status,
       COUNT(*) AS cnt,
       SUM(amount_cents) / 100.0 AS total_amount
FROM analytics.fct_orders
GROUP BY status
ORDER BY total_amount DESC;

SELECT country,
       COUNT(*) AS cnt,
       SUM(amount_cents) / 100.0 AS total_amount
FROM analytics.fct_orders
GROUP BY country
ORDER BY total_amount DESC;