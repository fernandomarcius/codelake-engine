-- Between cycles: inserts, an update (two change rows) and a delete: six
-- more change rows, appended by the incremental models in cycle 2.
USE shadow;
INSERT INTO dbo.orders VALUES (7, 'open', 7.000007, '2026-04-01T08:00:00.000', 'seven', 0x07, 7);
INSERT INTO dbo.orders VALUES
    (8, 'open', NULL, '2026-04-02T08:00:00.000', NULL,    NULL,   NULL),
    (9, 'paid', 9.9,  '2026-04-03T08:00:00.000', 'nine ', 0x0909, 9);
UPDATE dbo.orders SET status = 'paid', amount = amount + 1, note = 'updated' WHERE order_id = 1;
DELETE FROM dbo.orders WHERE order_id = 6;
GO
