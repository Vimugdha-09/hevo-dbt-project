{{ config(materialized='table') }}

WITH customers AS (
    SELECT
        id AS customer_id,
        first_name,
        last_name
    FROM {{ source('raw', 'hevo_raw_customers') }}
),

orders AS (
    SELECT
        user_id,
        MIN(order_date) AS first_order,
        MAX(order_date) AS most_recent_order,
        COUNT(*) AS number_of_orders
    FROM {{ source('raw', 'hevo_raw_orders') }}
    GROUP BY user_id
),

payments AS (
    SELECT
        o.user_id,
        SUM(p.amount) AS customer_lifetime_value
    FROM {{ source('raw', 'hevo_raw_payments') }} p
    JOIN {{ source('raw', 'hevo_raw_orders') }} o
        ON p.order_id = o.id
    GROUP BY o.user_id
)

SELECT
    c.customer_id,
    c.first_name,
    c.last_name,
    o.first_order,
    o.most_recent_order,
    COALESCE(o.number_of_orders, 0) AS number_of_orders,
    COALESCE(p.customer_lifetime_value, 0) AS customer_lifetime_value
FROM customers c
LEFT JOIN orders o ON c.customer_id = o.user_id
LEFT JOIN payments p ON c.customer_id = p.user_id
