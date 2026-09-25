{{ config(materialized='view', database='hive', schema='shadow_mart') }}

-- Latest state of each order that still exists.
SELECT order_id, status, amount, created_at, note, code, qty
FROM (
    SELECT o.*,
           ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY {{ binary_order_key('__start_lsn') }} DESC, {{ binary_order_key('__seqval') }} DESC) AS rn
    FROM {{ ref('orders') }} o
)
WHERE rn = 1 AND deleted = 0
