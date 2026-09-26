{{ config(database='hive', schema='shadow_raw') }}

-- Change rows up to the cycle's watermark, appended after the last one read.
SELECT {{ select_widened(source('src', 'dbo_orders_ct'),
                         declared={'status': 'varchar(20)', 'note': 'varchar(100)'}) }}
FROM {{ source('src', 'dbo_orders_ct') }}
WHERE "__$start_lsn" <= {{ shadow_cutoff() }}
{% if is_incremental() %}
  AND "__$start_lsn" > (SELECT COALESCE(MAX("__$start_lsn"), X'00') FROM {{ this }})
{% endif %}
