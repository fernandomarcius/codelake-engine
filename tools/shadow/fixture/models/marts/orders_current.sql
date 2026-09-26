{{ config(materialized='table', database='hive', schema='shadow_mart') }}

SELECT * FROM {{ ref('orders_current_v') }}
