{{ config(database='hive', schema='shadow_pilot') }}

-- Insert-only event store of the orders table: one row per change.
WITH src AS (
    SELECT
        ct."__$start_lsn"                     AS __start_lsn,
        ct."__$seqval"                        AS __seqval,
        ct."__$operation"                     AS __operation,
        ltm.tran_end_time                     AS lsn_time,
        ROW_NUMBER() OVER (
            PARTITION BY ct."__$start_lsn"
            ORDER BY {{ binary_order_key('ct."__$seqval"') }} ASC
        )                                     AS seq_within_lsn,
        ct.order_id, ct.status, ct.amount, ct.created_at, ct.note, ct.code, ct.qty
    FROM {{ ref('dbo_orders_ct') }} ct
    LEFT JOIN {{ ref('lsn_time_mapping') }} ltm
        ON ltm.start_lsn = ct."__$start_lsn"
    WHERE ct."__$operation" != 3
      AND ct."__$start_lsn" <= (SELECT COALESCE(MAX(start_lsn), X'00') FROM {{ ref('lsn_time_mapping') }})
    {% if is_incremental() %}
      AND ct."__$start_lsn" > (SELECT COALESCE(MAX(__start_lsn), X'00') FROM {{ this }})
    {% endif %}
)
SELECT
    CAST(uuid() AS VARCHAR)                                        AS id,
    __start_lsn,
    __seqval,
    __operation                                                    AS operation,
    CASE WHEN __operation = 1 THEN 1 ELSE 0 END                    AS deleted,
    CAST(lsn_time AS timestamp(3))                                 AS changed_at,
    date_add('millisecond', (seq_within_lsn - 1) * 5, CAST(lsn_time AS timestamp(3))) AS recorded_at,
    CONCAT_WS('_', 'order', CAST(order_id AS VARCHAR))             AS order_key,
    order_id,
    status,
    CAST(amount AS decimal(18, 6))                                 AS amount,
    CAST(created_at AS timestamp(3))                               AS created_at,
    note,
    code,
    qty,
    from_big_endian_64(substr(__seqval, 3))                        AS seqval_number,
    localtimestamp                                                 AS loaded_at
FROM src
