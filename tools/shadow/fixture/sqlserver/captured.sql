-- 1 when the capture job has read every change and the LSN time mapping
-- covers it; prints the change row count after it. Three-part names, no
-- USE: sqlcmd would print the context change.
SET NOCOUNT ON;
SELECT CASE
         WHEN (SELECT MAX(__$start_lsn) FROM shadow.cdc.dbo_orders_CT)
              <= (SELECT MAX(start_lsn) FROM shadow.cdc.lsn_time_mapping) THEN 1 ELSE 0
       END,
       (SELECT COUNT(*) FROM shadow.cdc.dbo_orders_CT);
