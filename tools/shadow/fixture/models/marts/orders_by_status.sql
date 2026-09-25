{{ config(materialized='table', database='hive', schema='shadow_mart') }}

SELECT
    status,
    COUNT(*)                                        AS orders,
    CAST(SUM(amount) AS decimal(38, 6))             AS total_amount,
    MAX(created_at)                                 AS last_created_at,
    COUNT(*) FILTER (WHERE note IS NULL)            AS without_note,
    COUNT(*) FILTER (WHERE note = '')               AS empty_note
FROM {{ ref('orders_current_v') }}
GROUP BY status
