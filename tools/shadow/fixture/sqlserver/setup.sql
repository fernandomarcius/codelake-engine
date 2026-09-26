-- Source database with change data capture on one table (SQL Server 2022).
CREATE DATABASE shadow;
GO
USE shadow;
EXEC sys.sp_cdc_enable_db;
GO
CREATE TABLE dbo.orders (
    order_id   int            NOT NULL PRIMARY KEY,
    status     varchar(20)    NOT NULL,
    amount     decimal(18, 6) NULL,
    created_at datetime2(3)   NOT NULL,
    note       varchar(100)   NULL,
    code       varbinary(16)  NULL,
    qty        smallint       NULL
);
GO
EXEC sys.sp_cdc_enable_table
    @source_schema = N'dbo',
    @source_name = N'orders',
    @role_name = NULL,
    @supports_net_changes = 0;
GO
