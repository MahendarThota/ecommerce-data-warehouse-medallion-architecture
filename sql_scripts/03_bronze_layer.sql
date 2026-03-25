

USE Ecommerce_DW;
GO

IF OBJECT_ID('bronze.bronze_customers','U')   IS NOT NULL DROP TABLE bronze.bronze_customers;
IF OBJECT_ID('bronze.bronze_orders','U')      IS NOT NULL DROP TABLE bronze.bronze_orders;
IF OBJECT_ID('bronze.bronze_order_items','U') IS NOT NULL DROP TABLE bronze.bronze_order_items;
IF OBJECT_ID('bronze.bronze_products','U')    IS NOT NULL DROP TABLE bronze.bronze_products;
IF OBJECT_ID('bronze.bronze_payments','U')    IS NOT NULL DROP TABLE bronze.bronze_payments;
GO

CREATE TABLE bronze.bronze_customers (
    customer_id                 VARCHAR(50),
    customer_unique_id          VARCHAR(50),
    customer_zip_code_prefix    VARCHAR(20),
    customer_city               VARCHAR(100),
    customer_state              VARCHAR(10),
    ingestion_timestamp         DATETIME2       DEFAULT GETDATE(),
    source_system               VARCHAR(50)     DEFAULT 'olist_csv'
);
GO
CREATE TABLE bronze.bronze_orders (
    order_id                        VARCHAR(50),
    customer_id                     VARCHAR(50),
    order_status                    VARCHAR(20),
    order_purchase_timestamp        VARCHAR(50),
    order_approved_at               VARCHAR(50),
    order_delivered_carrier_date    VARCHAR(50),
    order_delivered_customer_date   VARCHAR(50),
    order_estimated_delivery_date   VARCHAR(50),
    ingestion_timestamp             DATETIME2       DEFAULT GETDATE(),
    source_system                   VARCHAR(50)     DEFAULT 'olist_csv'
);
GO
CREATE TABLE bronze.bronze_order_items (
    order_id            VARCHAR(50),
    order_item_id       VARCHAR(10),
    product_id          VARCHAR(50),
    seller_id           VARCHAR(50),
    shipping_limit_date VARCHAR(50),
    price               VARCHAR(20),
    freight_value       VARCHAR(20),
    ingestion_timestamp DATETIME2       DEFAULT GETDATE(),
    source_system       VARCHAR(50)     DEFAULT 'olist_csv'
);
GO
CREATE TABLE bronze.bronze_products (
    product_id                  VARCHAR(50),
    product_category_name       VARCHAR(100),
    product_name_lenght         VARCHAR(10),
    product_description_lenght  VARCHAR(10),
    product_photos_qty          VARCHAR(10),
    product_weight_g            VARCHAR(10),
    product_length_cm           VARCHAR(10),
    product_height_cm           VARCHAR(10),
    product_width_cm            VARCHAR(10),
    ingestion_timestamp         DATETIME2       DEFAULT GETDATE(),
    source_system               VARCHAR(50)     DEFAULT 'olist_csv'
);
GO
CREATE TABLE bronze.bronze_payments (
    order_id                VARCHAR(50),
    payment_sequential      VARCHAR(10),
    payment_type            VARCHAR(50),
    payment_installments    VARCHAR(10),
    payment_value           VARCHAR(20),
    ingestion_timestamp     DATETIME2       DEFAULT GETDATE(),
    source_system           VARCHAR(50)     DEFAULT 'olist_csv'
);
GO

PRINT 'Bronze tables created.';
GO



CREATE OR ALTER PROCEDURE bronze.load_customers
    @load_type  VARCHAR(20) = 'incremental',
    @rows_out   INT         = 0 OUTPUT
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @start_time DATETIME2 = GETDATE(), @last_load_time DATETIME2, @row_count INT = 0;
    BEGIN TRY
        PRINT '>>> bronze.load_customers | ' + @load_type;
        SELECT @last_load_time = last_load_time FROM control.pipeline_watermark WHERE pipeline_name = 'bronze_customers';

        IF @load_type = 'snapshot'
        BEGIN
            TRUNCATE TABLE bronze.bronze_customers;
            SET @last_load_time = NULL;
        END

        INSERT INTO bronze.bronze_customers
            (customer_id, customer_unique_id, customer_zip_code_prefix,
             customer_city, customer_state, ingestion_timestamp, source_system)
        SELECT s.customer_id, s.customer_unique_id, s.customer_zip_code_prefix,
               s.customer_city, s.customer_state, GETDATE(), 'olist_csv'
        FROM source.customers s
        WHERE @last_load_time IS NULL
           OR NOT EXISTS (SELECT 1 FROM bronze.bronze_customers b WHERE b.customer_id = s.customer_id);

        SET @row_count = @@ROWCOUNT;
        SET @rows_out = @row_count;
        UPDATE control.pipeline_watermark SET last_load_time=GETDATE(), updated_at=GETDATE() WHERE pipeline_name='bronze_customers';
        PRINT 'bronze_customers: ' + CAST(@row_count AS VARCHAR) + ' rows | ' + CAST(DATEDIFF(second,@start_time,GETDATE()) AS VARCHAR) + 's';
    END TRY
    BEGIN CATCH PRINT 'ERROR bronze.load_customers: ' + ERROR_MESSAGE(); THROW; END CATCH
END;
GO



CREATE OR ALTER PROCEDURE bronze.load_orders
    @load_type  VARCHAR(20) = 'incremental',
    @rows_out   INT         = 0 OUTPUT
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @start_time DATETIME2 = GETDATE(), @last_load_time DATETIME2, @row_count INT = 0;
    BEGIN TRY
        PRINT '>>> bronze.load_orders | ' + @load_type;
        SELECT @last_load_time = last_load_time FROM control.pipeline_watermark WHERE pipeline_name = 'bronze_orders';

        IF @load_type = 'snapshot'
        BEGIN
            TRUNCATE TABLE bronze.bronze_orders;
            SET @last_load_time = NULL;
        END

        INSERT INTO bronze.bronze_orders
            (order_id, customer_id, order_status, order_purchase_timestamp,
             order_approved_at, order_delivered_carrier_date,
             order_delivered_customer_date, order_estimated_delivery_date,
             ingestion_timestamp, source_system)
        SELECT o.order_id, o.customer_id, o.order_status, o.order_purchase_timestamp,
               o.order_approved_at, o.order_delivered_carrier_date,
               o.order_delivered_customer_date, o.order_estimated_delivery_date,
               GETDATE(), 'olist_csv'
        FROM source.orders o
        WHERE @last_load_time IS NULL
           OR NOT EXISTS (
               SELECT 1 FROM bronze.bronze_orders b
               WHERE b.order_id = o.order_id
           );

        SET @row_count = @@ROWCOUNT;
        SET @rows_out = @row_count;
        UPDATE control.pipeline_watermark SET last_load_time=GETDATE(), updated_at=GETDATE() WHERE pipeline_name='bronze_orders';
        PRINT 'bronze_orders: ' + CAST(@row_count AS VARCHAR) + ' rows | ' + CAST(DATEDIFF(second,@start_time,GETDATE()) AS VARCHAR) + 's';
    END TRY
    BEGIN CATCH PRINT 'ERROR bronze.load_orders: ' + ERROR_MESSAGE(); THROW; END CATCH
END;
GO



CREATE OR ALTER PROCEDURE bronze.load_order_items
    @load_type  VARCHAR(20) = 'incremental',
    @rows_out   INT         = 0 OUTPUT
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @start_time DATETIME2 = GETDATE(), @last_load_time DATETIME2, @row_count INT = 0;
    BEGIN TRY
        PRINT '>>> bronze.load_order_items | ' + @load_type;
        SELECT @last_load_time = last_load_time FROM control.pipeline_watermark WHERE pipeline_name = 'bronze_order_items';

        IF @load_type = 'snapshot'
        BEGIN
            TRUNCATE TABLE bronze.bronze_order_items;
            SET @last_load_time = NULL;
        END

        INSERT INTO bronze.bronze_order_items
            (order_id, order_item_id, product_id, seller_id,
             shipping_limit_date, price, freight_value, ingestion_timestamp, source_system)
        SELECT oi.order_id, oi.order_item_id, oi.product_id, oi.seller_id,
               oi.shipping_limit_date, oi.price, oi.freight_value, GETDATE(), 'olist_csv'
        FROM source.order_items oi
        WHERE @last_load_time IS NULL
           OR NOT EXISTS (
               SELECT 1 FROM bronze.bronze_order_items b
               WHERE b.order_id = oi.order_id AND b.order_item_id = oi.order_item_id
           );

        SET @row_count = @@ROWCOUNT;
        SET @rows_out = @row_count;
        UPDATE control.pipeline_watermark SET last_load_time=GETDATE(), updated_at=GETDATE() WHERE pipeline_name='bronze_order_items';
        PRINT 'bronze_order_items: ' + CAST(@row_count AS VARCHAR) + ' rows | ' + CAST(DATEDIFF(second,@start_time,GETDATE()) AS VARCHAR) + 's';
    END TRY
    BEGIN CATCH PRINT 'ERROR bronze.load_order_items: ' + ERROR_MESSAGE(); THROW; END CATCH
END;
GO



CREATE OR ALTER PROCEDURE bronze.load_products
    @load_type  VARCHAR(20) = 'incremental',
    @rows_out   INT         = 0 OUTPUT
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @start_time DATETIME2 = GETDATE(), @last_load_time DATETIME2, @row_count INT = 0;
    BEGIN TRY
        PRINT '>>> bronze.load_products | ' + @load_type;
        SELECT @last_load_time = last_load_time FROM control.pipeline_watermark WHERE pipeline_name = 'bronze_products';

        IF @load_type = 'snapshot'
        BEGIN
            TRUNCATE TABLE bronze.bronze_products;
            SET @last_load_time = NULL;
        END

        INSERT INTO bronze.bronze_products
            (product_id, product_category_name, product_name_lenght, product_description_lenght,
             product_photos_qty, product_weight_g, product_length_cm, product_height_cm, product_width_cm,
             ingestion_timestamp, source_system)
        SELECT s.product_id, s.product_category_name, s.product_name_lenght, s.product_description_lenght,
               s.product_photos_qty, s.product_weight_g, s.product_length_cm, s.product_height_cm, s.product_width_cm,
               GETDATE(), 'olist_csv'
        FROM source.products s
        WHERE @last_load_time IS NULL
           OR NOT EXISTS (SELECT 1 FROM bronze.bronze_products b WHERE b.product_id = s.product_id);

        SET @row_count = @@ROWCOUNT;
        SET @rows_out = @row_count;
        UPDATE control.pipeline_watermark SET last_load_time=GETDATE(), updated_at=GETDATE() WHERE pipeline_name='bronze_products';
        PRINT 'bronze_products: ' + CAST(@row_count AS VARCHAR) + ' rows | ' + CAST(DATEDIFF(second,@start_time,GETDATE()) AS VARCHAR) + 's';
    END TRY
    BEGIN CATCH PRINT 'ERROR bronze.load_products: ' + ERROR_MESSAGE(); THROW; END CATCH
END;
GO



CREATE OR ALTER PROCEDURE bronze.load_payments
    @load_type  VARCHAR(20) = 'incremental',
    @rows_out   INT         = 0 OUTPUT
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @start_time DATETIME2 = GETDATE(), @last_load_time DATETIME2, @row_count INT = 0;
    BEGIN TRY
        PRINT '>>> bronze.load_payments | ' + @load_type;
        SELECT @last_load_time = last_load_time FROM control.pipeline_watermark WHERE pipeline_name = 'bronze_payments';

        IF @load_type = 'snapshot'
        BEGIN
            TRUNCATE TABLE bronze.bronze_payments;
            SET @last_load_time = NULL;
        END

        INSERT INTO bronze.bronze_payments
            (order_id, payment_sequential, payment_type, payment_installments, payment_value,
             ingestion_timestamp, source_system)
        SELECT p.order_id, p.payment_sequential, p.payment_type, p.payment_installments, p.payment_value,
               GETDATE(), 'olist_csv'
        FROM source.payments p
        WHERE @last_load_time IS NULL
           OR NOT EXISTS (
               SELECT 1 FROM bronze.bronze_payments b
               WHERE b.order_id = p.order_id AND b.payment_sequential = p.payment_sequential
           );

        SET @row_count = @@ROWCOUNT;
        SET @rows_out = @row_count;
        UPDATE control.pipeline_watermark SET last_load_time=GETDATE(), updated_at=GETDATE() WHERE pipeline_name='bronze_payments';
        PRINT 'bronze_payments: ' + CAST(@row_count AS VARCHAR) + ' rows | ' + CAST(DATEDIFF(second,@start_time,GETDATE()) AS VARCHAR) + 's';
    END TRY
    BEGIN CATCH PRINT 'ERROR bronze.load_payments: ' + ERROR_MESSAGE(); THROW; END CATCH 
END;
GO



CREATE OR ALTER PROCEDURE bronze.load_bronze
    @load_type VARCHAR(20) = 'incremental'
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @start_time DATETIME2 = GETDATE(), @end_time DATETIME2, @total_rows INT = 0, @log_id INT,
            @rows_customers INT=0, @rows_orders INT=0, @rows_order_items INT=0, @rows_products INT=0, @rows_payments INT=0;
    BEGIN TRY
        INSERT INTO control.pipeline_log (pipeline_name, layer, load_type, status, start_time)
        VALUES ('load_bronze','bronze',@load_type,'RUNNING',@start_time);
        SET @log_id = SCOPE_IDENTITY();

        PRINT '==========================================';
        PRINT 'BRONZE | Mode: ' + UPPER(@load_type);
        PRINT '==========================================';

        EXEC bronze.load_customers   @load_type=@load_type, @rows_out=@rows_customers   OUTPUT;
        EXEC bronze.load_orders      @load_type=@load_type, @rows_out=@rows_orders      OUTPUT;
        EXEC bronze.load_order_items @load_type=@load_type, @rows_out=@rows_order_items OUTPUT;
        EXEC bronze.load_products    @load_type=@load_type, @rows_out=@rows_products    OUTPUT;
        EXEC bronze.load_payments    @load_type=@load_type, @rows_out=@rows_payments    OUTPUT;

        SET @total_rows = @rows_customers + @rows_orders + @rows_order_items + @rows_products + @rows_payments;
        SET @end_time = GETDATE();
        UPDATE control.pipeline_log SET status='SUCCESS', end_time=@end_time,
            duration_seconds=DATEDIFF(second,@start_time,@end_time), rows_processed=@total_rows WHERE log_id=@log_id;
        PRINT '==========================================';
        PRINT 'BRONZE Done: ' + CAST(@total_rows AS VARCHAR) + ' rows | ' + CAST(DATEDIFF(second,@start_time,@end_time) AS VARCHAR) + 's';
        PRINT '==========================================';
    END TRY
    BEGIN CATCH
        UPDATE control.pipeline_log SET status='FAILED', end_time=GETDATE(), error_message=ERROR_MESSAGE() WHERE log_id=@log_id;
        PRINT 'ERROR bronze: ' + ERROR_MESSAGE(); THROW;
    END CATCH
END;
GO

PRINT 'Bronze layer ready.';
GO
