-- Cycle 1: two transactions (two LSNs), six change rows. Covers a trailing
-- space, an empty string next to a NULL, extreme decimals, milliseconds.
USE shadow;
INSERT INTO dbo.orders (order_id, status, amount, created_at, note, code, qty) VALUES
    (1, 'open', 12.500000,             '2026-01-01T10:00:00.123', 'first',      0x01AB,     1),
    (2, 'open', -0.000001,             '2026-01-02T00:00:00.000', NULL,         NULL,       2),
    (3, 'paid', 0,                     '2026-01-03T23:59:59.999', '',           0x,         NULL),
    (4, 'paid', 999999999999.999999,   '2026-02-28T12:00:00.500', 'trailing  ', 0xFFFF,     32767);
INSERT INTO dbo.orders (order_id, status, amount, created_at, note, code, qty) VALUES
    (5, 'late', 1.1,                   '1999-12-31T23:59:59.997', 'café',       0x00,       -5),
    (6, 'open', 3.333333,              '2026-03-01T00:00:00.001', 'x',          0xDEADBEEF, 0);
GO
