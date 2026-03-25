

USE Ecommerce_DW;
GO

IF OBJECT_ID('gold.fact_sales','U')    IS NOT NULL DROP TABLE gold.fact_sales;
IF OBJECT_ID('gold.fact_orders','U')   IS NOT NULL DROP TABLE gold.fact_orders;
IF OBJECT_ID('gold.dim_customers','U') IS NOT NULL DROP TABLE gold.dim_customers;
IF OBJECT_ID('gold.dim_products','U')  IS NOT NULL DROP TABLE gold.dim_products;
IF OBJECT_ID('gold.dim_date','U')      IS NOT NULL DROP TABLE gold.dim_date;
GO

CREATE TABLE gold.dim_customers (
    customer_key                INT IDENTITY(1,1)   PRIMARY KEY,
    customer_id                 VARCHAR(50)         NOT NULL UNIQUE,
    customer_unique_id          VARCHAR(50),
    customer_zip_code_prefix    INT,
    customer_city               VARCHAR(100),
    customer_state              VARCHAR(10),
    created_at                  DATETIME2           DEFAULT GETDATE(),
    updated_at                  DATETIME2
);
GO

CREATE TABLE gold.dim_products (
    product_key                 INT IDENTITY(1,1)   PRIMARY KEY,
    product_id                  VARCHAR(50)         NOT NULL UNIQUE,
    product_category_name       VARCHAR(100),
    product_name_length         INT,
    product_description_length  INT,
    product_photos_qty          INT,
    product_weight_g            DECIMAL(10,2),
    product_length_cm           DECIMAL(10,2),
    product_height_cm           DECIMAL(10,2),
    product_width_cm            DECIMAL(10,2),
    product_volume_cm3          DECIMAL(12,2),
    is_missing_category         BIT,
    is_missing_dimensions       BIT,
    created_at                  DATETIME2           DEFAULT GETDATE(),
    updated_at                  DATETIME2
);
GO

CREATE TABLE gold.dim_date (
    date_key        INT         PRIMARY KEY,
    full_date       DATE        NOT NULL,
    year            INT,
    quarter         INT,
    month           INT,
    month_name      VARCHAR(10),
    week_of_year    INT,
    day_of_week     INT,
    day_name        VARCHAR(10),
    is_weekend      BIT
);
GO

DECLARE @d DATE = '2016-01-01';
WHILE @d <= '2020-12-31'
BEGIN
    INSERT INTO gold.dim_date (date_key,full_date,year,quarter,month,month_name,week_of_year,day_of_week,day_name,is_weekend)
    VALUES (CAST(CONVERT(VARCHAR(8),@d,112) AS INT),@d,
        YEAR(@d),DATEPART(QUARTER,@d),MONTH(@d),DATENAME(MONTH,@d),
        DATEPART(WEEK,@d),DATEPART(WEEKDAY,@d),DATENAME(WEEKDAY,@d),
        CASE WHEN DATEPART(WEEKDAY,@d) IN (1,7) THEN 1 ELSE 0 END);
    SET @d = DATEADD(day,1,@d);
END
PRINT 'dim_date populated 2016-2020.';
GO

CREATE TABLE gold.fact_sales (
    sales_key               INT IDENTITY(1,1)   PRIMARY KEY,
    order_id                VARCHAR(50)         NOT NULL,
    order_item_id           INT                 NOT NULL,
    customer_key            INT,
    product_key             INT,
    date_key                INT,
    price                   DECIMAL(10,2),
    freight_value           DECIMAL(10,2),
    total_item_value        DECIMAL(10,2),
    payment_value           DECIMAL(10,2),
    payment_type            VARCHAR(50),
    payment_installments    INT,
    order_status            VARCHAR(50),
    is_late_delivery        BIT,
    is_free_shipping        BIT,
    order_purchase_date     DATE,
    order_year              INT,
    order_month             INT,
    order_quarter           INT,
    created_at              DATETIME2           DEFAULT GETDATE()
);
GO

CREATE TABLE gold.fact_orders (
    order_key                   INT IDENTITY(1,1)   PRIMARY KEY,
    order_id                    VARCHAR(50)         NOT NULL UNIQUE,
    customer_key                INT,
    date_key                    INT,
    order_status                VARCHAR(50),
    total_order_value           DECIMAL(10,2),
    total_payment_value         DECIMAL(10,2),
    item_count                  INT,
    is_late_delivery            BIT,
    is_delivery_inconsistent    BIT,
    is_canceled_in_transit      BIT,
    is_canceled_but_reached     BIT,
    is_invalid_date_sequence    BIT,
    order_purchase_date         DATE,
    order_year                  INT,
    order_month                 INT,
    created_at                  DATETIME2           DEFAULT GETDATE()
);
GO

PRINT 'Gold tables created.';
GO



CREATE OR ALTER PROCEDURE gold.load_dim_customers
    @load_type  VARCHAR(20) = 'incremental',
    @rows_out   INT         = 0 OUTPUT
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @start_time DATETIME2 = GETDATE(), @last_load_time DATETIME2;
    BEGIN TRY
        PRINT '>>> gold.load_dim_customers | ' + @load_type;
        SELECT @last_load_time = last_load_time FROM control.pipeline_watermark WHERE pipeline_name = 'gold_dim_customers';
        IF @load_type = 'snapshot' SET @last_load_time = NULL;

        MERGE gold.dim_customers AS target
        USING (
            SELECT * FROM silver.customers_clean
            WHERE @last_load_time IS NULL OR updated_at > @last_load_time OR (updated_at IS NULL AND created_at > @last_load_time)
        ) AS source
        ON target.customer_id = source.customer_id
        WHEN MATCHED AND (
            ISNULL(target.customer_city,'')            <> ISNULL(source.customer_city,'') OR
            ISNULL(target.customer_state,'')           <> ISNULL(source.customer_state,'') OR
            ISNULL(target.customer_zip_code_prefix, 0) <> ISNULL(source.customer_zip_code_prefix, 0)
        ) THEN UPDATE SET
            target.customer_zip_code_prefix = source.customer_zip_code_prefix,
            target.customer_city            = source.customer_city,
            target.customer_state           = source.customer_state,
            target.updated_at               = GETDATE()
        WHEN NOT MATCHED THEN INSERT
            (customer_id, customer_unique_id, customer_zip_code_prefix, customer_city, customer_state, created_at)
        VALUES
            (source.customer_id, source.customer_unique_id, source.customer_zip_code_prefix,
             source.customer_city, source.customer_state, GETDATE());

        SET @rows_out = @@ROWCOUNT;
        UPDATE control.pipeline_watermark SET last_load_time=GETDATE(), updated_at=GETDATE() WHERE pipeline_name='gold_dim_customers';
        PRINT 'dim_customers done | ' + CAST(DATEDIFF(second,@start_time,GETDATE()) AS VARCHAR) + 's';
    END TRY
    BEGIN CATCH PRINT 'ERROR gold.load_dim_customers: ' + ERROR_MESSAGE(); THROW; END CATCH
END;
GO



CREATE OR ALTER PROCEDURE gold.load_dim_products
    @load_type  VARCHAR(20) = 'incremental',
    @rows_out   INT         = 0 OUTPUT
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @start_time DATETIME2 = GETDATE(), @last_load_time DATETIME2;
    BEGIN TRY
        PRINT '>>> gold.load_dim_products | ' + @load_type;
        SELECT @last_load_time = last_load_time FROM control.pipeline_watermark WHERE pipeline_name = 'gold_dim_products';
        IF @load_type = 'snapshot' SET @last_load_time = NULL;

        MERGE gold.dim_products AS target
        USING (
            SELECT * FROM silver.products_clean
            WHERE @last_load_time IS NULL OR updated_at > @last_load_time OR (updated_at IS NULL AND created_at > @last_load_time)
        ) AS source
        ON target.product_id = source.product_id
        WHEN NOT MATCHED THEN INSERT (
            product_id, product_category_name, product_name_length, product_description_length,
            product_photos_qty, product_weight_g, product_length_cm, product_height_cm, product_width_cm,
            product_volume_cm3, is_missing_category, is_missing_dimensions, created_at
        ) VALUES (
            source.product_id, source.product_category_name, source.product_name_length, source.product_description_length,
            source.product_photos_qty, source.product_weight_g, source.product_length_cm, source.product_height_cm,
            source.product_width_cm, source.product_volume_cm3, source.is_missing_category, source.is_missing_dimensions, GETDATE()
        )
        WHEN MATCHED AND ISNULL(target.product_category_name,'') <> ISNULL(source.product_category_name,'') THEN UPDATE SET
            target.product_category_name = source.product_category_name,
            target.updated_at            = GETDATE();

        SET @rows_out = @@ROWCOUNT;
        UPDATE control.pipeline_watermark SET last_load_time=GETDATE(), updated_at=GETDATE() WHERE pipeline_name='gold_dim_products';
        PRINT 'dim_products done | ' + CAST(DATEDIFF(second,@start_time,GETDATE()) AS VARCHAR) + 's';
    END TRY
    BEGIN CATCH PRINT 'ERROR gold.load_dim_products: ' + ERROR_MESSAGE(); THROW; END CATCH
END;
GO



CREATE OR ALTER PROCEDURE gold.load_fact_sales
    @load_type  VARCHAR(20) = 'incremental',
    @rows_out   INT         = 0 OUTPUT
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @start_time DATETIME2 = GETDATE(), @last_load_time DATETIME2;
    BEGIN TRY
        PRINT '>>> gold.load_fact_sales | ' + @load_type;
        SELECT @last_load_time = last_load_time FROM control.pipeline_watermark WHERE pipeline_name = 'gold_fact_sales';
        IF @load_type = 'snapshot' BEGIN TRUNCATE TABLE gold.fact_sales; SET @last_load_time = NULL; END

        INSERT INTO gold.fact_sales (
            order_id, order_item_id, customer_key, product_key, date_key,
            price, freight_value, total_item_value,
            payment_value, payment_type, payment_installments,
            order_status, is_late_delivery, is_free_shipping,
            order_purchase_date, order_year, order_month, order_quarter
        )
        SELECT
            oi.order_id, oi.order_item_id,
            dc.customer_key, dp.product_key, dd.date_key,
            oi.price, oi.freight_value, oi.total_item_value,
            ISNULL(p.payment_value, 0),
            ISNULL(p.payment_type, 'unknown'),
            ISNULL(p.payment_installments, 1),
            o.order_status, o.is_late_delivery, oi.is_free_shipping,
            CAST(o.order_purchase_timestamp AS DATE),
            YEAR(o.order_purchase_timestamp),
            MONTH(o.order_purchase_timestamp),
            DATEPART(QUARTER, o.order_purchase_timestamp)
        FROM silver.order_items_clean oi
        INNER JOIN silver.orders_clean o ON oi.order_id = o.order_id
        LEFT JOIN silver.payments_clean p
            ON oi.order_id = p.order_id
            AND p.payment_sequential = (
                SELECT MIN(pp.payment_sequential) FROM silver.payments_clean pp WHERE pp.order_id = oi.order_id
            )
        LEFT JOIN gold.dim_customers dc ON o.customer_id = dc.customer_id
        LEFT JOIN gold.dim_products  dp ON oi.product_id = dp.product_id
        LEFT JOIN gold.dim_date      dd ON dd.date_key = TRY_CAST(CONVERT(VARCHAR(8), CAST(o.order_purchase_timestamp AS DATE), 112) AS INT)
        WHERE (@last_load_time IS NULL OR o.updated_at > @last_load_time OR (o.updated_at IS NULL AND o.created_at > @last_load_time))
          AND NOT EXISTS (
            SELECT 1 FROM gold.fact_sales fs WHERE fs.order_id = oi.order_id AND fs.order_item_id = oi.order_item_id
          );

        SET @rows_out = @@ROWCOUNT;
        UPDATE control.pipeline_watermark SET last_load_time=GETDATE(), updated_at=GETDATE() WHERE pipeline_name='gold_fact_sales';
        PRINT 'fact_sales done | ' + CAST(DATEDIFF(second,@start_time,GETDATE()) AS VARCHAR) + 's';
    END TRY
    BEGIN CATCH PRINT 'ERROR gold.load_fact_sales: ' + ERROR_MESSAGE(); THROW; END CATCH
END;
GO



CREATE OR ALTER PROCEDURE gold.load_fact_orders
    @load_type  VARCHAR(20) = 'incremental',
    @rows_out   INT         = 0 OUTPUT
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @start_time DATETIME2 = GETDATE(), @last_load_time DATETIME2;
    BEGIN TRY
        PRINT '>>> gold.load_fact_orders | ' + @load_type;
        SELECT @last_load_time = last_load_time FROM control.pipeline_watermark WHERE pipeline_name = 'gold_fact_orders';
        IF @load_type = 'snapshot' BEGIN TRUNCATE TABLE gold.fact_orders; SET @last_load_time = NULL; END

        INSERT INTO gold.fact_orders (
            order_id, customer_key, date_key, order_status,
            total_order_value, total_payment_value, item_count,
            is_late_delivery, is_delivery_inconsistent,
            is_canceled_in_transit, is_canceled_but_reached,
            is_invalid_date_sequence,
            order_purchase_date, order_year, order_month
        )
        SELECT
            o.order_id, dc.customer_key, dd.date_key, o.order_status,
            ISNULL(items.total_order_value, 0),
            ISNULL(pays.total_payment_value, 0),
            ISNULL(items.item_count, 0),
            o.is_late_delivery, o.is_delivery_inconsistent,
            o.is_canceled_in_transit, o.is_canceled_but_reached,
            o.is_invalid_date_sequence,
            CAST(o.order_purchase_timestamp AS DATE),
            YEAR(o.order_purchase_timestamp),
            MONTH(o.order_purchase_timestamp)
        FROM silver.orders_clean o
        LEFT JOIN gold.dim_customers dc ON o.customer_id = dc.customer_id
        LEFT JOIN gold.dim_date dd ON dd.date_key = TRY_CAST(CONVERT(VARCHAR(8), CAST(o.order_purchase_timestamp AS DATE), 112) AS INT)
        LEFT JOIN (
            SELECT order_id, SUM(total_item_value) AS total_order_value, COUNT(*) AS item_count
            FROM silver.order_items_clean GROUP BY order_id
        ) items ON o.order_id = items.order_id
        LEFT JOIN (
            SELECT order_id, SUM(payment_value) AS total_payment_value
            FROM silver.payments_clean GROUP BY order_id
        ) pays ON o.order_id = pays.order_id
        WHERE (@last_load_time IS NULL OR o.updated_at > @last_load_time OR (o.updated_at IS NULL AND o.created_at > @last_load_time))
          AND NOT EXISTS (SELECT 1 FROM gold.fact_orders fo WHERE fo.order_id = o.order_id);

        SET @rows_out = @@ROWCOUNT;
        UPDATE control.pipeline_watermark SET last_load_time=GETDATE(), updated_at=GETDATE() WHERE pipeline_name='gold_fact_orders';
        PRINT 'fact_orders done | ' + CAST(DATEDIFF(second,@start_time,GETDATE()) AS VARCHAR) + 's';
    END TRY
    BEGIN CATCH PRINT 'ERROR gold.load_fact_orders: ' + ERROR_MESSAGE(); THROW; END CATCH
END;
GO



CREATE OR ALTER PROCEDURE gold.load_gold
    @load_type VARCHAR(20) = 'incremental'
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @start_time DATETIME2 = GETDATE(), @end_time DATETIME2, @total_rows INT = 0, @log_id INT,
            @rows_dim_customers INT=0, @rows_dim_products INT=0, @rows_fact_sales INT=0, @rows_fact_orders INT=0;
    BEGIN TRY
        INSERT INTO control.pipeline_log (pipeline_name, layer, load_type, status, start_time)
        VALUES ('load_gold','gold',@load_type,'RUNNING',@start_time);
        SET @log_id = SCOPE_IDENTITY();

        PRINT '==========================================';
        PRINT 'GOLD | Mode: ' + UPPER(@load_type);
        PRINT '==========================================';

        IF @load_type = 'snapshot'
        BEGIN
            TRUNCATE TABLE gold.fact_sales;
            TRUNCATE TABLE gold.fact_orders;
            PRINT 'Snapshot: facts truncated.';
        END

        EXEC gold.load_dim_customers @load_type=@load_type, @rows_out=@rows_dim_customers OUTPUT;
        EXEC gold.load_dim_products  @load_type=@load_type, @rows_out=@rows_dim_products  OUTPUT;
        EXEC gold.load_fact_sales    @load_type=@load_type, @rows_out=@rows_fact_sales    OUTPUT;
        EXEC gold.load_fact_orders   @load_type=@load_type, @rows_out=@rows_fact_orders   OUTPUT;

        SET @total_rows = @rows_dim_customers + @rows_dim_products + @rows_fact_sales + @rows_fact_orders;
        SET @end_time = GETDATE();
        UPDATE control.pipeline_log SET status='SUCCESS', end_time=@end_time,
            duration_seconds=DATEDIFF(second,@start_time,@end_time), rows_processed=@total_rows WHERE log_id=@log_id;
        PRINT '==========================================';
        PRINT 'GOLD Done: ' + CAST(@total_rows AS VARCHAR) + ' rows | ' + CAST(DATEDIFF(second,@start_time,@end_time) AS VARCHAR) + 's';
        PRINT '==========================================';
    END TRY
    BEGIN CATCH
        UPDATE control.pipeline_log SET status='FAILED', end_time=GETDATE(), error_message=ERROR_MESSAGE() WHERE log_id=@log_id;
        PRINT 'ERROR gold: ' + ERROR_MESSAGE(); THROW;
    END CATCH
END;
GO


































CREATE OR ALTER VIEW gold.sales_summary AS
SELECT
    fs.order_year, fs.order_month, fs.order_quarter, dd.month_name,
    CAST(fs.order_year AS VARCHAR) + '-' + RIGHT('0'+CAST(fs.order_month AS VARCHAR),2) AS year_month,
    COUNT(DISTINCT fs.order_id)     AS total_orders,
    COUNT(*)                        AS total_items_sold,
    SUM(fs.total_item_value)        AS total_revenue,
    SUM(fs.freight_value)           AS total_freight,
    AVG(fo.total_order_value)       AS avg_order_value,
    SUM(fs.payment_value)           AS total_payment_collected,
    SUM(CASE WHEN fo.order_status = 'DELIVERED' AND fo.is_delivery_inconsistent = 0 THEN 1 ELSE 0 END) AS delivered_orders,
    SUM(CASE WHEN fo.is_canceled_in_transit  = 1 THEN 1 ELSE 0 END) AS canceled_in_transit,
    SUM(CASE WHEN fo.is_canceled_but_reached = 1 THEN 1 ELSE 0 END) AS canceled_but_reached,
    SUM(CASE WHEN fs.is_late_delivery = 1 AND fo.is_invalid_date_sequence = 0 THEN 1 ELSE 0 END) AS late_deliveries,
    CASE WHEN SUM(CASE WHEN fo.order_status='DELIVERED' AND fo.is_delivery_inconsistent=0 AND fo.is_invalid_date_sequence=0 THEN 1 ELSE 0 END) = 0 THEN NULL
         ELSE CAST(SUM(CASE WHEN fs.is_late_delivery=1 AND fo.is_invalid_date_sequence=0 THEN 1.0 ELSE 0 END)
              / SUM(CASE WHEN fo.order_status='DELIVERED' AND fo.is_delivery_inconsistent=0 AND fo.is_invalid_date_sequence=0 THEN 1.0 ELSE 0 END) * 100 AS DECIMAL(5,2))
    END AS late_delivery_rate_pct
FROM gold.fact_sales fs
LEFT JOIN gold.dim_date    dd ON fs.date_key = dd.date_key
LEFT JOIN gold.fact_orders fo ON fs.order_id = fo.order_id
GROUP BY fs.order_year, fs.order_month, fs.order_quarter, dd.month_name;
GO

CREATE OR ALTER VIEW gold.product_performance AS
SELECT
    dp.product_id, dp.product_category_name, dp.is_missing_category,
    COUNT(DISTINCT fs.order_id)                                         AS total_orders,
    SUM(fs.total_item_value)                                            AS total_revenue,
    AVG(fs.price)                                                       AS avg_price,
    COUNT(*)                                                            AS units_sold,
    SUM(CASE WHEN fs.is_free_shipping=1 THEN 1 ELSE 0 END)              AS free_shipping_orders,
    SUM(CASE WHEN fs.is_late_delivery=1 AND fo.is_invalid_date_sequence=0 THEN 1 ELSE 0 END) AS late_deliveries,
    RANK() OVER (ORDER BY SUM(fs.total_item_value) DESC)                AS revenue_rank
FROM gold.fact_sales fs
INNER JOIN gold.dim_products dp ON fs.product_key = dp.product_key
LEFT JOIN  gold.fact_orders  fo ON fs.order_id    = fo.order_id
GROUP BY dp.product_id, dp.product_category_name, dp.is_missing_category;
GO

CREATE OR ALTER VIEW gold.customer_revenue AS
SELECT
    dc.customer_id, dc.customer_unique_id,
    dc.customer_city, dc.customer_state, dc.customer_zip_code_prefix,
    COUNT(DISTINCT fo.order_id)                                         AS total_orders,
    SUM(fo.total_order_value)                                           AS lifetime_revenue,
    AVG(fo.total_order_value)                                           AS avg_order_value,
    MIN(fo.order_purchase_date)                                         AS first_order_date,
    MAX(fo.order_purchase_date)                                         AS last_order_date,
    DATEDIFF(day, MIN(fo.order_purchase_date), MAX(fo.order_purchase_date)) AS customer_lifespan_days,
    SUM(CASE WHEN fo.is_late_delivery=1 AND fo.is_invalid_date_sequence=0 THEN 1 ELSE 0 END) AS late_delivery_count,
    SUM(CASE WHEN fo.is_delivery_inconsistent=1 THEN 1 ELSE 0 END)     AS inconsistent_delivery_count,
    SUM(CASE WHEN fo.is_canceled_in_transit=1  THEN 1 ELSE 0 END)      AS canceled_in_transit_count,
    SUM(CASE WHEN fo.is_canceled_but_reached=1 THEN 1 ELSE 0 END)      AS canceled_but_reached_count,
    CASE WHEN COUNT(DISTINCT fo.order_id) > 1 THEN 'Repeat' ELSE 'One-Time' END AS customer_type
FROM gold.fact_orders fo
INNER JOIN gold.dim_customers dc ON fo.customer_key = dc.customer_key
GROUP BY dc.customer_id, dc.customer_unique_id,
         dc.customer_city, dc.customer_state, dc.customer_zip_code_prefix;
GO

CREATE OR ALTER VIEW gold.delivery_quality AS
SELECT
    fo.order_year, fo.order_month,
    COUNT(*)                                                            AS total_orders,
    SUM(CASE WHEN fo.order_status='DELIVERED' THEN 1 ELSE 0 END)        AS status_delivered,
    SUM(CASE WHEN fo.order_status='DELIVERED' AND fo.is_delivery_inconsistent=0 THEN 1 ELSE 0 END) AS clean_delivered,
    SUM(CASE WHEN fo.is_delivery_inconsistent=1  THEN 1 ELSE 0 END)    AS delivery_inconsistent,
    SUM(CASE WHEN fo.is_invalid_date_sequence=1  THEN 1 ELSE 0 END)    AS invalid_date_sequence,
    SUM(CASE WHEN fo.is_canceled_in_transit=1    THEN 1 ELSE 0 END)    AS canceled_in_transit,
    SUM(CASE WHEN fo.is_canceled_but_reached=1   THEN 1 ELSE 0 END)    AS canceled_but_reached,
    SUM(CASE WHEN fo.is_late_delivery=1 AND fo.is_invalid_date_sequence=0 THEN 1 ELSE 0 END) AS late_deliveries,
    CASE WHEN SUM(CASE WHEN fo.order_status='DELIVERED' AND fo.is_delivery_inconsistent=0 AND fo.is_invalid_date_sequence=0 THEN 1 ELSE 0 END) = 0 THEN NULL
         ELSE CAST(SUM(CASE WHEN fo.is_late_delivery=1 AND fo.is_invalid_date_sequence=0 THEN 1.0 ELSE 0 END)
              / SUM(CASE WHEN fo.order_status='DELIVERED' AND fo.is_delivery_inconsistent=0 AND fo.is_invalid_date_sequence=0 THEN 1.0 ELSE 0 END) * 100 AS DECIMAL(5,2))
    END AS late_delivery_rate_pct
FROM gold.fact_orders fo
GROUP BY fo.order_year, fo.order_month;
GO

PRINT 'Gold layer ready: 5 tables + 4 views.';
GO
