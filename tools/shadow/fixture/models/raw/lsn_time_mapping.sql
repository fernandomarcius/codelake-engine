{{ config(database='hive', schema='shadow_raw') }}

SELECT start_lsn, tran_begin_time, tran_end_time
FROM {{ source('src', 'lsn_time_mapping') }}
WHERE start_lsn <= {{ shadow_cutoff() }}
{% if is_incremental() %}
  AND start_lsn > (SELECT COALESCE(MAX(start_lsn), X'00') FROM {{ this }})
{% endif %}
