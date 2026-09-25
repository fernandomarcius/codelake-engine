{{ config(materialized='view', database='hive', schema='shadow_mart') }}

-- Latest state of each order that still exists.
SELECT order_id, status, amount, created_at, note, code, qty
FROM (
    SELECT o.*,
           ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY __start_lsn DESC, __seqval DESC) AS rn
    FROM {{ ref('orders') }} o
)
WHERE rn = 1 AND deleted = 0
