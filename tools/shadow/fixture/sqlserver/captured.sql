-- 1 when the capture job has read every change and the LSN time mapping
-- covers it; prints the change row count after it.
SET NOCOUNT ON;
USE shadow;
SELECT CASE
         WHEN (SELECT MAX(__$start_lsn) FROM cdc.dbo_orders_CT)
              <= (SELECT MAX(start_lsn) FROM cdc.lsn_time_mapping) THEN 1 ELSE 0
       END,
       (SELECT COUNT(*) FROM cdc.dbo_orders_CT);
