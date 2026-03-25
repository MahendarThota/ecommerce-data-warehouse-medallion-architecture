
USE master;
GO

IF NOT EXISTS (SELECT name FROM sys.databases WHERE name = 'Ecommerce_DW')
    CREATE DATABASE Ecommerce_DW;
GO

USE Ecommerce_DW;
GO

IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'source')  EXEC('CREATE SCHEMA source');
IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'bronze')  EXEC('CREATE SCHEMA bronze');
IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'silver')  EXEC('CREATE SCHEMA silver');
IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'gold')    EXEC('CREATE SCHEMA gold');
IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'control') EXEC('CREATE SCHEMA control');
GO



IF OBJECT_ID('control.pipeline_watermark','U') IS NOT NULL DROP TABLE control.pipeline_watermark;
GO
CREATE TABLE control.pipeline_watermark (
    watermark_id    INT IDENTITY(1,1)   PRIMARY KEY,
    pipeline_name   VARCHAR(100)        NOT NULL UNIQUE,
    last_load_time  DATETIME2           NULL,
    updated_at      DATETIME2           DEFAULT GETDATE()
);
GO



IF OBJECT_ID('control.pipeline_log','U') IS NOT NULL DROP TABLE control.pipeline_log;
GO
CREATE TABLE control.pipeline_log (
    log_id          INT IDENTITY(1,1)   PRIMARY KEY,
    pipeline_name   VARCHAR(100)        NOT NULL,
    layer           VARCHAR(20),
    load_type       VARCHAR(20),
    status          VARCHAR(20),
    rows_processed  INT                 DEFAULT 0,
    start_time      DATETIME2           DEFAULT GETDATE(),
    end_time        DATETIME2,
    duration_seconds INT,
    error_message   NVARCHAR(2000),
    created_at      DATETIME2           DEFAULT GETDATE()
);
GO

INSERT INTO control.pipeline_watermark (pipeline_name, last_load_time)
VALUES
    ('bronze_customers',    NULL),
    ('bronze_orders',       NULL),
    ('bronze_order_items',  NULL),
    ('bronze_products',     NULL),
    ('bronze_payments',     NULL),
    ('silver_customers',    NULL),
    ('silver_orders',       NULL),
    ('silver_order_items',  NULL),
    ('silver_products',     NULL),
    ('silver_payments',     NULL),
    ('gold_dim_customers',  NULL),
    ('gold_dim_products',   NULL),
    ('gold_fact_sales',     NULL),
    ('gold_fact_orders',    NULL);
GO

PRINT 'Setup complete.';
GO
