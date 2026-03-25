

USE Ecommerce_DW;
GO

IF OBJECT_ID('source.customers','U')   IS NOT NULL DROP TABLE source.customers;
IF OBJECT_ID('source.orders','U')      IS NOT NULL DROP TABLE source.orders;
IF OBJECT_ID('source.order_items','U') IS NOT NULL DROP TABLE source.order_items;
IF OBJECT_ID('source.products','U')    IS NOT NULL DROP TABLE source.products;
IF OBJECT_ID('source.payments','U')    IS NOT NULL DROP TABLE source.payments;
GO

CREATE TABLE source.customers (
    customer_id                 VARCHAR(50),
    customer_unique_id          VARCHAR(50),
    customer_zip_code_prefix    VARCHAR(20),
    customer_city               VARCHAR(100),
    customer_state              VARCHAR(10)
);
GO
CREATE TABLE source.orders (
    order_id                        VARCHAR(50),
    customer_id                     VARCHAR(50),
    order_status                    VARCHAR(20),
    order_purchase_timestamp        VARCHAR(50),
    order_approved_at               VARCHAR(50),
    order_delivered_carrier_date    VARCHAR(50),
    order_delivered_customer_date   VARCHAR(50),
    order_estimated_delivery_date   VARCHAR(50)
);
GO
CREATE TABLE source.order_items (
    order_id            VARCHAR(50),
    order_item_id       VARCHAR(10),
    product_id          VARCHAR(50),
    seller_id           VARCHAR(50),
    shipping_limit_date VARCHAR(50),
    price               VARCHAR(20),
    freight_value       VARCHAR(20)
);
GO
CREATE TABLE source.products (
    product_id                  VARCHAR(50),
    product_category_name       VARCHAR(100),
    product_name_lenght         VARCHAR(10),
    product_description_lenght  VARCHAR(10),
    product_photos_qty          VARCHAR(10),
    product_weight_g            VARCHAR(10),
    product_length_cm           VARCHAR(10),
    product_height_cm           VARCHAR(10),
    product_width_cm            VARCHAR(10)
);
GO
CREATE TABLE source.payments (
    order_id                VARCHAR(50),
    payment_sequential      VARCHAR(10),
    payment_type            VARCHAR(50),
    payment_installments    VARCHAR(10),
    payment_value           VARCHAR(20)
);
GO

PRINT 'Source tables created.';
GO



CREATE OR ALTER PROCEDURE source.load_customers
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @start_time DATETIME2 = GETDATE(), @row_count INT;
    BEGIN TRY
        TRUNCATE TABLE source.customers;
        BULK INSERT source.customers
        FROM 'D:\datasets\olist_customers_dataset.csv'
        WITH (FIRSTROW=2, FIELDTERMINATOR=',', ROWTERMINATOR='\n', TABLOCK);
        SELECT @row_count = COUNT(*) FROM source.customers;
        PRINT 'source.customers: ' + CAST(@row_count AS VARCHAR) + ' rows | ' + CAST(DATEDIFF(second,@start_time,GETDATE()) AS VARCHAR) + 's';
    END TRY
    BEGIN CATCH PRINT 'ERROR source.customers: ' + ERROR_MESSAGE(); THROW; END CATCH
END;
GO

CREATE OR ALTER PROCEDURE source.load_orders
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @start_time DATETIME2 = GETDATE(), @row_count INT;
    BEGIN TRY
        TRUNCATE TABLE source.orders;
        BULK INSERT source.orders
        FROM 'D:\datasets\olist_orders_dataset.csv'
        WITH (FIRSTROW=2, FIELDTERMINATOR=',', ROWTERMINATOR='\n', TABLOCK);
        SELECT @row_count = COUNT(*) FROM source.orders;
        PRINT 'source.orders: ' + CAST(@row_count AS VARCHAR) + ' rows | ' + CAST(DATEDIFF(second,@start_time,GETDATE()) AS VARCHAR) + 's';
    END TRY
    BEGIN CATCH PRINT 'ERROR source.orders: ' + ERROR_MESSAGE(); THROW; END CATCH
END;
GO

CREATE OR ALTER PROCEDURE source.load_order_items
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @start_time DATETIME2 = GETDATE(), @row_count INT;
    BEGIN TRY
        TRUNCATE TABLE source.order_items;
        BULK INSERT source.order_items
        FROM 'D:\datasets\olist_order_items_dataset.csv'
        WITH (FIRSTROW=2, FIELDTERMINATOR=',', ROWTERMINATOR='\n', TABLOCK);
        SELECT @row_count = COUNT(*) FROM source.order_items;
        PRINT 'source.order_items: ' + CAST(@row_count AS VARCHAR) + ' rows | ' + CAST(DATEDIFF(second,@start_time,GETDATE()) AS VARCHAR) + 's';
    END TRY
    BEGIN CATCH PRINT 'ERROR source.order_items: ' + ERROR_MESSAGE(); THROW; END CATCH
END;
GO

CREATE OR ALTER PROCEDURE source.load_products
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @start_time DATETIME2 = GETDATE(), @row_count INT;
    BEGIN TRY
        TRUNCATE TABLE source.products;
        BULK INSERT source.products
        FROM 'D:\datasets\olist_products_dataset.csv'
        WITH (FIRSTROW=2, FIELDTERMINATOR=',', ROWTERMINATOR='\n', TABLOCK);
        SELECT @row_count = COUNT(*) FROM source.products;
        PRINT 'source.products: ' + CAST(@row_count AS VARCHAR) + ' rows | ' + CAST(DATEDIFF(second,@start_time,GETDATE()) AS VARCHAR) + 's';
    END TRY
    BEGIN CATCH PRINT 'ERROR source.products: ' + ERROR_MESSAGE(); THROW; END CATCH
END;
GO

CREATE OR ALTER PROCEDURE source.load_payments
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @start_time DATETIME2 = GETDATE(), @row_count INT;
    BEGIN TRY
        TRUNCATE TABLE source.payments;
        BULK INSERT source.payments
        FROM 'D:\datasets\olist_order_payments_dataset.csv'
        WITH (FIRSTROW=2, FIELDTERMINATOR=',', ROWTERMINATOR='\n', TABLOCK);
        SELECT @row_count = COUNT(*) FROM source.payments;
        PRINT 'source.payments: ' + CAST(@row_count AS VARCHAR) + ' rows | ' + CAST(DATEDIFF(second,@start_time,GETDATE()) AS VARCHAR) + 's';
    END TRY
    BEGIN CATCH PRINT 'ERROR source.payments: ' + ERROR_MESSAGE(); THROW; END CATCH
END;
GO



CREATE OR ALTER PROCEDURE source.load_source
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @start_time DATETIME2 = GETDATE(), @end_time DATETIME2, @row_count INT = 0, @log_id INT;
    BEGIN TRY
        INSERT INTO control.pipeline_log (pipeline_name, layer, load_type, status, start_time)
        VALUES ('load_source','source','snapshot','RUNNING',@start_time);
        SET @log_id = SCOPE_IDENTITY();

        PRINT '=== SOURCE LAYER | SNAPSHOT ===';
        EXEC source.load_customers;
        EXEC source.load_orders;
        EXEC source.load_order_items;
        EXEC source.load_products;
        EXEC source.load_payments;

        SELECT @row_count =
            (SELECT COUNT(*) FROM source.customers) +
            (SELECT COUNT(*) FROM source.orders) +
            (SELECT COUNT(*) FROM source.order_items) +
            (SELECT COUNT(*) FROM source.products) +
            (SELECT COUNT(*) FROM source.payments);

        SET @end_time = GETDATE();
        UPDATE control.pipeline_log SET status='SUCCESS', end_time=@end_time,
            duration_seconds=DATEDIFF(second,@start_time,@end_time), rows_processed=@row_count WHERE log_id=@log_id;
        PRINT '=== SOURCE Done: ' + CAST(@row_count AS VARCHAR) + ' rows | ' + CAST(DATEDIFF(second,@start_time,@end_time) AS VARCHAR) + 's ===';
    END TRY
    BEGIN CATCH
        UPDATE control.pipeline_log SET status='FAILED', end_time=GETDATE(), error_message=ERROR_MESSAGE() WHERE log_id=@log_id;
        PRINT 'ERROR source: ' + ERROR_MESSAGE(); THROW;
    END CATCH
END;
GO

PRINT 'Source layer ready.';
GO

