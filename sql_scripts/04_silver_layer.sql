

USE Ecommerce_DW;
GO

IF OBJECT_ID('silver.customers_clean','U')   IS NOT NULL DROP TABLE silver.customers_clean;
IF OBJECT_ID('silver.orders_clean','U')      IS NOT NULL DROP TABLE silver.orders_clean;
IF OBJECT_ID('silver.order_items_clean','U') IS NOT NULL DROP TABLE silver.order_items_clean;
IF OBJECT_ID('silver.products_clean','U')    IS NOT NULL DROP TABLE silver.products_clean;
IF OBJECT_ID('silver.payments_clean','U')    IS NOT NULL DROP TABLE silver.payments_clean;
GO

CREATE TABLE silver.customers_clean (
    customer_id                 VARCHAR(50)     NOT NULL,
    customer_unique_id          VARCHAR(50),
    customer_zip_code_prefix    INT,
    customer_city               VARCHAR(100),
    customer_state              VARCHAR(10),
    created_at                  DATETIME2       DEFAULT GETDATE(),
    updated_at                  DATETIME2,
    CONSTRAINT pk_silver_customers PRIMARY KEY (customer_id)
);
GO

CREATE TABLE silver.orders_clean (
    order_id                        VARCHAR(50)     NOT NULL,
    customer_id                     VARCHAR(50),
    order_status                    VARCHAR(50),
    order_purchase_timestamp        DATETIME2,
    order_approved_at               DATETIME2,
    order_delivered_carrier_date    DATETIME2,
    order_delivered_customer_date   DATETIME2,
    order_estimated_delivery_date   DATETIME2,
    is_delivery_inconsistent    BIT DEFAULT 0,
    is_shipped_missing          BIT DEFAULT 0,
    is_canceled_in_transit      BIT DEFAULT 0,
    is_canceled_but_reached     BIT DEFAULT 0,
    is_invalid_date_sequence    BIT DEFAULT 0,
    is_late_delivery            BIT DEFAULT 0,
    created_at                  DATETIME2       DEFAULT GETDATE(),
    updated_at                  DATETIME2,
    CONSTRAINT pk_silver_orders PRIMARY KEY (order_id)
);
GO

CREATE TABLE silver.order_items_clean (
    order_id            VARCHAR(50)     NOT NULL,
    order_item_id       INT             NOT NULL,
    product_id          VARCHAR(50),
    seller_id           VARCHAR(50),
    shipping_limit_date DATETIME2,
    price               DECIMAL(10,2),
    freight_value       DECIMAL(10,2)   DEFAULT 0,
    total_item_value    DECIMAL(10,2),
    is_free_shipping    BIT             DEFAULT 0,
    created_at          DATETIME2       DEFAULT GETDATE(),
    updated_at          DATETIME2,
    CONSTRAINT pk_silver_order_items PRIMARY KEY (order_id, order_item_id)
);
GO

CREATE TABLE silver.products_clean (
    product_id                  VARCHAR(50)     NOT NULL,
    product_category_name       VARCHAR(100),
    product_name_length         INT,
    product_description_length  INT,
    product_photos_qty          INT,
    product_weight_g            DECIMAL(10,2),
    product_length_cm           DECIMAL(10,2),
    product_height_cm           DECIMAL(10,2),
    product_width_cm            DECIMAL(10,2),
    product_volume_cm3          DECIMAL(12,2),
    is_missing_category         BIT DEFAULT 0,
    is_missing_dimensions       BIT DEFAULT 0,
    created_at                  DATETIME2       DEFAULT GETDATE(),
    updated_at                  DATETIME2,
    CONSTRAINT pk_silver_products PRIMARY KEY (product_id)
);
GO

CREATE TABLE silver.payments_clean (
    order_id                VARCHAR(50)     NOT NULL,
    payment_sequential      INT             NOT NULL,
    payment_type            VARCHAR(50),
    payment_installments    INT             DEFAULT 1,
    payment_value           DECIMAL(10,2),
    is_not_defined          BIT DEFAULT 0,
    is_zero_value           BIT DEFAULT 0,
    created_at              DATETIME2       DEFAULT GETDATE(),
    updated_at              DATETIME2,
    CONSTRAINT pk_silver_payments PRIMARY KEY (order_id, payment_sequential)
);
GO

PRINT 'Silver tables created.';
GO



CREATE OR ALTER PROCEDURE silver.load_customers
    @load_type  VARCHAR(20) = 'incremental',
    @rows_out   INT         = 0 OUTPUT
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @start_time DATETIME2 = GETDATE(), @last_load_time DATETIME2;
    BEGIN TRY
        PRINT '>>> silver.load_customers | ' + @load_type;
        SELECT @last_load_time = last_load_time FROM control.pipeline_watermark WHERE pipeline_name = 'silver_customers';
        IF @load_type = 'snapshot' BEGIN TRUNCATE TABLE silver.customers_clean; SET @last_load_time = NULL; END

        MERGE silver.customers_clean AS target
        USING (
            -- ROW_NUMBER: bronze has multiple rows per customer_id
            -- (each source reload = new ingestion_timestamp)
            -- Take latest only to avoid MERGE source duplicate error
            SELECT customer_id, customer_unique_id,
                   customer_zip_code_prefix, customer_city, customer_state
            FROM (
                SELECT
                    TRIM(customer_id)       AS customer_id,
                    TRIM(customer_unique_id) AS customer_unique_id,
                    TRY_CAST(TRIM(customer_zip_code_prefix) AS INT) AS customer_zip_code_prefix,
                    UPPER(LEFT(TRIM(customer_city),1))
                        + LOWER(SUBSTRING(TRIM(customer_city),2,LEN(TRIM(customer_city)))) AS customer_city,
                    UPPER(TRIM(customer_state)) AS customer_state,
                    ROW_NUMBER() OVER (
                        PARTITION BY TRIM(customer_id)
                        ORDER BY ingestion_timestamp DESC
                    ) AS rn
                FROM bronze.bronze_customers
                WHERE customer_id IS NOT NULL AND customer_unique_id IS NOT NULL
                  AND (@last_load_time IS NULL OR ingestion_timestamp > @last_load_time)
            ) d WHERE rn = 1
        ) AS source
        ON target.customer_id = source.customer_id
        WHEN MATCHED THEN UPDATE SET
            target.customer_unique_id       = source.customer_unique_id,
            target.customer_zip_code_prefix = source.customer_zip_code_prefix,
            target.customer_city            = source.customer_city,
            target.customer_state           = source.customer_state,
            target.updated_at               = GETDATE()
        WHEN NOT MATCHED THEN INSERT
            (customer_id, customer_unique_id, customer_zip_code_prefix,
             customer_city, customer_state, created_at)
        VALUES
            (source.customer_id, source.customer_unique_id, source.customer_zip_code_prefix,
             source.customer_city, source.customer_state, GETDATE());

        SET @rows_out = @@ROWCOUNT;
        UPDATE control.pipeline_watermark SET last_load_time=GETDATE(), updated_at=GETDATE()
        WHERE pipeline_name='silver_customers';
        PRINT 'customers_clean done | ' + CAST(DATEDIFF(second,@start_time,GETDATE()) AS VARCHAR) + 's';
    END TRY
    BEGIN CATCH PRINT 'ERROR silver.load_customers: ' + ERROR_MESSAGE(); THROW; END CATCH
END;
GO



CREATE OR ALTER PROCEDURE silver.load_orders
    @load_type  VARCHAR(20) = 'incremental',
    @rows_out   INT         = 0 OUTPUT
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @start_time DATETIME2 = GETDATE(), @last_load_time DATETIME2;
    BEGIN TRY
        PRINT '>>> silver.load_orders | ' + @load_type;
        SELECT @last_load_time = last_load_time FROM control.pipeline_watermark WHERE pipeline_name = 'silver_orders';
        IF @load_type = 'snapshot' BEGIN TRUNCATE TABLE silver.orders_clean; SET @last_load_time = NULL; END

        MERGE silver.orders_clean AS target
        USING (
            SELECT order_id, customer_id, order_status,
                   order_purchase_timestamp, order_approved_at,
                   order_delivered_carrier_date, order_delivered_customer_date,
                   order_estimated_delivery_date,
                   is_delivery_inconsistent, is_shipped_missing,
                   is_canceled_in_transit, is_canceled_but_reached,
                   is_invalid_date_sequence, is_late_delivery
            FROM (
                SELECT
                    TRIM(order_id)          AS order_id,
                    TRIM(customer_id)       AS customer_id,
                    UPPER(TRIM(order_status)) AS order_status,
                    TRY_CONVERT(DATETIME2, NULLIF(TRIM(order_purchase_timestamp),''),      105) AS order_purchase_timestamp,
                    TRY_CONVERT(DATETIME2, NULLIF(TRIM(order_approved_at),''),             105) AS order_approved_at,
                    TRY_CONVERT(DATETIME2, NULLIF(TRIM(order_delivered_carrier_date),''),  105) AS order_delivered_carrier_date,
                    TRY_CONVERT(DATETIME2, NULLIF(TRIM(order_delivered_customer_date),''), 105) AS order_delivered_customer_date,
                    TRY_CONVERT(DATETIME2, NULLIF(TRIM(order_estimated_delivery_date),''), 105) AS order_estimated_delivery_date,
                    -- DQ FLAG 1: DELIVERED but no customer_date (8 rows)
                    CASE WHEN UPPER(TRIM(order_status)) = 'DELIVERED'
                          AND NULLIF(TRIM(order_delivered_customer_date),'') IS NULL
                         THEN 1 ELSE 0 END AS is_delivery_inconsistent,
                    -- DQ FLAG 2: SHIPPED/DELIVERED but no carrier_date (2 rows)
                    CASE WHEN UPPER(TRIM(order_status)) IN ('SHIPPED','DELIVERED')
                          AND NULLIF(TRIM(order_delivered_carrier_date),'') IS NULL
                         THEN 1 ELSE 0 END AS is_shipped_missing,
                    -- DQ FLAG 3A: CANCELED, shipped but returned
                    CASE WHEN UPPER(TRIM(order_status)) IN ('CANCELED','UNAVAILABLE')
                          AND NULLIF(TRIM(order_delivered_carrier_date),'')  IS NOT NULL
                          AND NULLIF(TRIM(order_delivered_customer_date),'') IS NULL
                         THEN 1 ELSE 0 END AS is_canceled_in_transit,
                    -- DQ FLAG 3B: CANCELED but reached customer
                    CASE WHEN UPPER(TRIM(order_status)) IN ('CANCELED','UNAVAILABLE')
                          AND NULLIF(TRIM(order_delivered_carrier_date),'')  IS NOT NULL
                          AND NULLIF(TRIM(order_delivered_customer_date),'') IS NOT NULL
                         THEN 1 ELSE 0 END AS is_canceled_but_reached,
                    -- DQ FLAG 4: Date sequence issues (carrier < approved = system lag)
                    CASE WHEN TRY_CONVERT(DATETIME2, NULLIF(TRIM(order_approved_at),''), 105)
                                 < TRY_CONVERT(DATETIME2, NULLIF(TRIM(order_purchase_timestamp),''), 105)
                           OR TRY_CONVERT(DATETIME2, NULLIF(TRIM(order_delivered_carrier_date),''), 105)
                                 < TRY_CONVERT(DATETIME2, NULLIF(TRIM(order_approved_at),''), 105)
                           OR TRY_CONVERT(DATETIME2, NULLIF(TRIM(order_delivered_customer_date),''), 105)
                                 < TRY_CONVERT(DATETIME2, NULLIF(TRIM(order_delivered_carrier_date),''), 105)
                         THEN 1 ELSE 0 END AS is_invalid_date_sequence,
                    -- DQ FLAG 5: Late delivery
                    CASE WHEN TRY_CONVERT(DATETIME2, NULLIF(TRIM(order_delivered_customer_date),''), 105)
                                 > TRY_CONVERT(DATETIME2, NULLIF(TRIM(order_estimated_delivery_date),''), 105)
                         THEN 1 ELSE 0 END AS is_late_delivery,
                    ROW_NUMBER() OVER (
                        PARTITION BY TRIM(order_id)
                        ORDER BY ingestion_timestamp DESC
                    ) AS rn
                FROM bronze.bronze_orders
                WHERE order_id IS NOT NULL AND customer_id IS NOT NULL
                  AND TRY_CONVERT(DATETIME2, NULLIF(TRIM(order_purchase_timestamp),''), 105) IS NOT NULL
                  AND (@last_load_time IS NULL OR ingestion_timestamp > @last_load_time)
            ) d WHERE rn = 1
        ) AS source
        ON target.order_id = source.order_id
        WHEN MATCHED THEN UPDATE SET
            target.customer_id                   = source.customer_id,
            target.order_status                  = source.order_status,
            target.order_purchase_timestamp      = source.order_purchase_timestamp,
            target.order_approved_at             = source.order_approved_at,
            target.order_delivered_carrier_date  = source.order_delivered_carrier_date,
            target.order_delivered_customer_date = source.order_delivered_customer_date,
            target.order_estimated_delivery_date = source.order_estimated_delivery_date,
            target.is_delivery_inconsistent      = source.is_delivery_inconsistent,
            target.is_shipped_missing            = source.is_shipped_missing,
            target.is_canceled_in_transit        = source.is_canceled_in_transit,
            target.is_canceled_but_reached       = source.is_canceled_but_reached,
            target.is_invalid_date_sequence      = source.is_invalid_date_sequence,
            target.is_late_delivery              = source.is_late_delivery,
            target.updated_at                    = GETDATE()
        WHEN NOT MATCHED THEN INSERT (
            order_id, customer_id, order_status,
            order_purchase_timestamp, order_approved_at,
            order_delivered_carrier_date, order_delivered_customer_date,
            order_estimated_delivery_date,
            is_delivery_inconsistent, is_shipped_missing,
            is_canceled_in_transit, is_canceled_but_reached,
            is_invalid_date_sequence, is_late_delivery, created_at
        ) VALUES (
            source.order_id, source.customer_id, source.order_status,
            source.order_purchase_timestamp, source.order_approved_at,
            source.order_delivered_carrier_date, source.order_delivered_customer_date,
            source.order_estimated_delivery_date,
            source.is_delivery_inconsistent, source.is_shipped_missing,
            source.is_canceled_in_transit, source.is_canceled_but_reached,
            source.is_invalid_date_sequence, source.is_late_delivery, GETDATE()
        );

        SET @rows_out = @@ROWCOUNT;
        UPDATE control.pipeline_watermark SET last_load_time=GETDATE(), updated_at=GETDATE()
        WHERE pipeline_name='silver_orders';
        PRINT 'orders_clean done | ' + CAST(DATEDIFF(second,@start_time,GETDATE()) AS VARCHAR) + 's';
    END TRY
    BEGIN CATCH PRINT 'ERROR silver.load_orders: ' + ERROR_MESSAGE(); THROW; END CATCH
END;
GO



CREATE OR ALTER PROCEDURE silver.load_order_items
    @load_type  VARCHAR(20) = 'incremental',
    @rows_out   INT         = 0 OUTPUT
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @start_time DATETIME2 = GETDATE(), @last_load_time DATETIME2;
    BEGIN TRY
        PRINT '>>> silver.load_order_items | ' + @load_type;
        SELECT @last_load_time = last_load_time FROM control.pipeline_watermark WHERE pipeline_name = 'silver_order_items';
        IF @load_type = 'snapshot' BEGIN TRUNCATE TABLE silver.order_items_clean; SET @last_load_time = NULL; END

        MERGE silver.order_items_clean AS target
        USING (
            SELECT order_id, order_item_id, product_id, seller_id,
                   shipping_limit_date, price, freight_value,
                   total_item_value, is_free_shipping
            FROM (
                SELECT
                    TRIM(order_id)                                                    AS order_id,
                    TRY_CAST(TRIM(order_item_id) AS INT)                              AS order_item_id,
                    TRIM(product_id)                                                  AS product_id,
                    TRIM(seller_id)                                                   AS seller_id,
                    TRY_CONVERT(DATETIME2, NULLIF(TRIM(shipping_limit_date),''), 105) AS shipping_limit_date,
                    TRY_CAST(NULLIF(TRIM(price),'') AS DECIMAL(10,2))                 AS price,
                    ISNULL(TRY_CAST(NULLIF(TRIM(freight_value),'') AS DECIMAL(10,2)),0) AS freight_value,
                    TRY_CAST(NULLIF(TRIM(price),'') AS DECIMAL(10,2))
                        + ISNULL(TRY_CAST(NULLIF(TRIM(freight_value),'') AS DECIMAL(10,2)),0) AS total_item_value,
                    CASE WHEN TRIM(freight_value) IN ('0','0.0') THEN 1 ELSE 0 END   AS is_free_shipping,
                    ROW_NUMBER() OVER (
                        PARTITION BY TRIM(order_id), TRIM(order_item_id)
                        ORDER BY ingestion_timestamp DESC
                    ) AS rn
                FROM bronze.bronze_order_items
                WHERE order_id IS NOT NULL
                  AND TRY_CAST(TRIM(order_item_id) AS INT) IS NOT NULL
                  AND TRY_CAST(NULLIF(TRIM(price),'') AS DECIMAL(10,2)) IS NOT NULL
                  AND (@last_load_time IS NULL OR ingestion_timestamp > @last_load_time)
            ) d WHERE rn = 1
        ) AS source
        ON target.order_id = source.order_id AND target.order_item_id = source.order_item_id
        WHEN MATCHED THEN UPDATE SET
            target.price            = source.price,
            target.freight_value    = source.freight_value,
            target.total_item_value = source.total_item_value,
            target.is_free_shipping = source.is_free_shipping,
            target.updated_at       = GETDATE()
        WHEN NOT MATCHED THEN INSERT (
            order_id, order_item_id, product_id, seller_id,
            shipping_limit_date, price, freight_value, total_item_value,
            is_free_shipping, created_at
        ) VALUES (
            source.order_id, source.order_item_id, source.product_id, source.seller_id,
            source.shipping_limit_date, source.price, source.freight_value, source.total_item_value,
            source.is_free_shipping, GETDATE()
        );

        SET @rows_out = @@ROWCOUNT;
        UPDATE control.pipeline_watermark SET last_load_time=GETDATE(), updated_at=GETDATE()
        WHERE pipeline_name='silver_order_items';
        PRINT 'order_items_clean done | ' + CAST(DATEDIFF(second,@start_time,GETDATE()) AS VARCHAR) + 's';
    END TRY
    BEGIN CATCH PRINT 'ERROR silver.load_order_items: ' + ERROR_MESSAGE(); THROW; END CATCH
END;
GO



CREATE OR ALTER PROCEDURE silver.load_products
    @load_type  VARCHAR(20) = 'incremental',
    @rows_out   INT         = 0 OUTPUT
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @start_time DATETIME2 = GETDATE(), @last_load_time DATETIME2;
    BEGIN TRY
        PRINT '>>> silver.load_products | ' + @load_type;
        SELECT @last_load_time = last_load_time FROM control.pipeline_watermark WHERE pipeline_name = 'silver_products';
        IF @load_type = 'snapshot' BEGIN TRUNCATE TABLE silver.products_clean; SET @last_load_time = NULL; END

        MERGE silver.products_clean AS target
        USING (
            SELECT product_id, product_category_name, product_name_length,
                   product_description_length, product_photos_qty,
                   product_weight_g, product_length_cm, product_height_cm, product_width_cm,
                   product_volume_cm3, is_missing_category, is_missing_dimensions
            FROM (
                SELECT
                    TRIM(product_id)                                                  AS product_id,
                    NULLIF(TRIM(product_category_name),'')                            AS product_category_name,
                    TRY_CAST(NULLIF(TRIM(product_name_lenght),'') AS INT)             AS product_name_length,
                    TRY_CAST(NULLIF(TRIM(product_description_lenght),'') AS INT)      AS product_description_length,
                    TRY_CAST(NULLIF(TRIM(product_photos_qty),'') AS INT)              AS product_photos_qty,
                    TRY_CAST(NULLIF(TRIM(product_weight_g),'') AS DECIMAL(10,2))      AS product_weight_g,
                    TRY_CAST(NULLIF(TRIM(product_length_cm),'') AS DECIMAL(10,2))     AS product_length_cm,
                    TRY_CAST(NULLIF(TRIM(product_height_cm),'') AS DECIMAL(10,2))     AS product_height_cm,
                    TRY_CAST(NULLIF(TRIM(product_width_cm),'') AS DECIMAL(10,2))      AS product_width_cm,
                    TRY_CAST(NULLIF(TRIM(product_length_cm),'') AS DECIMAL(10,2))
                        * TRY_CAST(NULLIF(TRIM(product_height_cm),'') AS DECIMAL(10,2))
                        * TRY_CAST(NULLIF(TRIM(product_width_cm),'') AS DECIMAL(10,2)) AS product_volume_cm3,
                    CASE WHEN NULLIF(TRIM(product_category_name),'') IS NULL
                         THEN 1 ELSE 0 END AS is_missing_category,
                    CASE WHEN NULLIF(TRIM(product_weight_g),'') IS NULL
                         THEN 1 ELSE 0 END AS is_missing_dimensions,
                    ROW_NUMBER() OVER (
                        PARTITION BY TRIM(product_id)
                        ORDER BY ingestion_timestamp DESC
                    ) AS rn
                FROM bronze.bronze_products
                WHERE product_id IS NOT NULL
                  AND (@last_load_time IS NULL OR ingestion_timestamp > @last_load_time)
            ) d WHERE rn = 1
        ) AS source
        ON target.product_id = source.product_id
        WHEN MATCHED THEN UPDATE SET
            target.product_category_name      = source.product_category_name,
            target.product_name_length        = source.product_name_length,
            target.product_description_length = source.product_description_length,
            target.product_photos_qty         = source.product_photos_qty,
            target.product_weight_g           = source.product_weight_g,
            target.product_length_cm          = source.product_length_cm,
            target.product_height_cm          = source.product_height_cm,
            target.product_width_cm           = source.product_width_cm,
            target.product_volume_cm3         = source.product_volume_cm3,
            target.is_missing_category        = source.is_missing_category,
            target.is_missing_dimensions      = source.is_missing_dimensions,
            target.updated_at                 = GETDATE()
        WHEN NOT MATCHED THEN INSERT (
            product_id, product_category_name, product_name_length,
            product_description_length, product_photos_qty,
            product_weight_g, product_length_cm, product_height_cm, product_width_cm,
            product_volume_cm3, is_missing_category, is_missing_dimensions, created_at
        ) VALUES (
            source.product_id, source.product_category_name, source.product_name_length,
            source.product_description_length, source.product_photos_qty,
            source.product_weight_g, source.product_length_cm, source.product_height_cm, source.product_width_cm,
            source.product_volume_cm3, source.is_missing_category, source.is_missing_dimensions, GETDATE()
        );

        SET @rows_out = @@ROWCOUNT;
        UPDATE control.pipeline_watermark SET last_load_time=GETDATE(), updated_at=GETDATE()
        WHERE pipeline_name='silver_products';
        PRINT 'products_clean done | ' + CAST(DATEDIFF(second,@start_time,GETDATE()) AS VARCHAR) + 's';
    END TRY
    BEGIN CATCH PRINT 'ERROR silver.load_products: ' + ERROR_MESSAGE(); THROW; END CATCH
END;
GO



CREATE OR ALTER PROCEDURE silver.load_payments
    @load_type  VARCHAR(20) = 'incremental',
    @rows_out   INT         = 0 OUTPUT
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @start_time DATETIME2 = GETDATE(), @last_load_time DATETIME2;
    BEGIN TRY
        PRINT '>>> silver.load_payments | ' + @load_type;
        SELECT @last_load_time = last_load_time FROM control.pipeline_watermark WHERE pipeline_name = 'silver_payments';
        IF @load_type = 'snapshot' BEGIN TRUNCATE TABLE silver.payments_clean; SET @last_load_time = NULL; END

        MERGE silver.payments_clean AS target
        USING (
            SELECT order_id, payment_sequential, payment_type,
                   payment_installments, payment_value,
                   is_not_defined, is_zero_value
            FROM (
                SELECT
                    TRIM(order_id)                                                   AS order_id,
                    TRY_CAST(TRIM(payment_sequential) AS INT)                        AS payment_sequential,
                    LOWER(TRIM(payment_type))                                        AS payment_type,
                    ISNULL(TRY_CAST(TRIM(payment_installments) AS INT), 1)           AS payment_installments,
                    ISNULL(TRY_CAST(NULLIF(TRIM(payment_value),'') AS DECIMAL(10,2)),0) AS payment_value,
                    CASE WHEN LOWER(TRIM(payment_type)) = 'not_defined'
                         THEN 1 ELSE 0 END AS is_not_defined,
                    CASE WHEN TRY_CAST(NULLIF(TRIM(payment_value),'') AS DECIMAL(10,2)) = 0
                         THEN 1 ELSE 0 END AS is_zero_value,
                    ROW_NUMBER() OVER (
                        PARTITION BY TRIM(order_id), TRIM(payment_sequential)
                        ORDER BY ingestion_timestamp DESC
                    ) AS rn
                FROM bronze.bronze_payments
                WHERE order_id IS NOT NULL
                  AND payment_sequential IS NOT NULL
                  AND TRY_CAST(TRIM(payment_sequential) AS INT) IS NOT NULL
                  AND (@last_load_time IS NULL OR ingestion_timestamp > @last_load_time)
            ) d WHERE rn = 1
        ) AS source
        ON target.order_id = source.order_id AND target.payment_sequential = source.payment_sequential
        WHEN MATCHED THEN UPDATE SET
            target.payment_type         = source.payment_type,
            target.payment_installments = source.payment_installments,
            target.payment_value        = source.payment_value,
            target.is_not_defined       = source.is_not_defined,
            target.is_zero_value        = source.is_zero_value,
            target.updated_at           = GETDATE()
        WHEN NOT MATCHED THEN INSERT (
            order_id, payment_sequential, payment_type,
            payment_installments, payment_value,
            is_not_defined, is_zero_value, created_at
        ) VALUES (
            source.order_id, source.payment_sequential, source.payment_type,
            source.payment_installments, source.payment_value,
            source.is_not_defined, source.is_zero_value, GETDATE()
        );

        SET @rows_out = @@ROWCOUNT;
        UPDATE control.pipeline_watermark SET last_load_time=GETDATE(), updated_at=GETDATE()
        WHERE pipeline_name='silver_payments';
        PRINT 'payments_clean done | ' + CAST(DATEDIFF(second,@start_time,GETDATE()) AS VARCHAR) + 's';
    END TRY
    BEGIN CATCH PRINT 'ERROR silver.load_payments: ' + ERROR_MESSAGE(); THROW; END CATCH
END;
GO



CREATE OR ALTER PROCEDURE silver.load_silver
    @load_type VARCHAR(20) = 'incremental'
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @start_time DATETIME2 = GETDATE(), @end_time DATETIME2, @total_rows INT = 0, @log_id INT,
            @rows_customers INT=0, @rows_orders INT=0, @rows_order_items INT=0, @rows_products INT=0, @rows_payments INT=0;
    BEGIN TRY
        INSERT INTO control.pipeline_log (pipeline_name, layer, load_type, status, start_time)
        VALUES ('load_silver','silver',@load_type,'RUNNING',@start_time);
        SET @log_id = SCOPE_IDENTITY();

        PRINT '==========================================';
        PRINT 'SILVER | Mode: ' + UPPER(@load_type);
        PRINT '==========================================';

        EXEC silver.load_customers   @load_type=@load_type, @rows_out=@rows_customers   OUTPUT;
        EXEC silver.load_orders      @load_type=@load_type, @rows_out=@rows_orders      OUTPUT;
        EXEC silver.load_order_items @load_type=@load_type, @rows_out=@rows_order_items OUTPUT;
        EXEC silver.load_products    @load_type=@load_type, @rows_out=@rows_products    OUTPUT;
        EXEC silver.load_payments    @load_type=@load_type, @rows_out=@rows_payments    OUTPUT;

        SET @total_rows = @rows_customers + @rows_orders + @rows_order_items + @rows_products + @rows_payments;
        SET @end_time = GETDATE();
        UPDATE control.pipeline_log SET status='SUCCESS', end_time=@end_time,
            duration_seconds=DATEDIFF(second,@start_time,@end_time), rows_processed=@total_rows WHERE log_id=@log_id;
        PRINT '==========================================';
        PRINT 'SILVER Done: ' + CAST(@total_rows AS VARCHAR) + ' rows | ' + CAST(DATEDIFF(second,@start_time,@end_time) AS VARCHAR) + 's';
        PRINT '==========================================';
    END TRY
    BEGIN CATCH
        UPDATE control.pipeline_log SET status='FAILED', end_time=GETDATE(), error_message=ERROR_MESSAGE() WHERE log_id=@log_id;
        PRINT 'ERROR silver: ' + ERROR_MESSAGE(); THROW;
    END CATCH
END;
GO

PRINT 'Silver layer ready.';
GO
