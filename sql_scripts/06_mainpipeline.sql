

USE Ecommerce_DW;
GO

CREATE OR ALTER PROCEDURE dbo.run_pipeline
    @load_type VARCHAR(20) = 'incremental'
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @pipeline_start DATETIME2 = GETDATE(), @pipeline_end DATETIME2, @total_rows INT = 0, @log_id INT;
    BEGIN TRY

        INSERT INTO control.pipeline_log (pipeline_name, layer, load_type, status, start_time)
        VALUES ('MAIN_PIPELINE','all',@load_type,'RUNNING',@pipeline_start);
        SET @log_id = SCOPE_IDENTITY();

        PRINT '';
        PRINT '##############################################';
        PRINT '  ECOMMERCE DW PIPELINE';
        PRINT '  Mode    : ' + UPPER(@load_type);
        PRINT '  Started : ' + CONVERT(VARCHAR(25),@pipeline_start,120);
        PRINT '##############################################';
        PRINT '';

        -- STAGE 0: CSV → Source (always snapshot)
        PRINT '>>> STAGE 0: Source Load';
        EXEC source.load_source;
        PRINT '>>> STAGE 0 Complete'; PRINT '';

        -- STAGE 1: Source → Bronze
        PRINT '>>> STAGE 1: Bronze Load';
        EXEC bronze.load_bronze @load_type=@load_type;
        PRINT '>>> STAGE 1 Complete'; PRINT '';

        -- STAGE 2: Bronze → Silver
        PRINT '>>> STAGE 2: Silver Load';
        EXEC silver.load_silver @load_type=@load_type;
        PRINT '>>> STAGE 2 Complete'; PRINT '';

        -- STAGE 3: Silver → Gold
        PRINT '>>> STAGE 3: Gold Load';
        EXEC gold.load_gold @load_type=@load_type;
        PRINT '>>> STAGE 3 Complete'; PRINT '';

        -- rows processed this run from pipeline_log
        SELECT @total_rows = ISNULL(SUM(rows_processed),0)
        FROM control.pipeline_log
        WHERE pipeline_name IN ('load_bronze','load_silver','load_gold')
          AND start_time >= @pipeline_start;

        SET @pipeline_end = GETDATE();
        UPDATE control.pipeline_log
        SET status='SUCCESS', end_time=@pipeline_end,
            duration_seconds=DATEDIFF(second,@pipeline_start,@pipeline_end), rows_processed=@total_rows 
        WHERE log_id=@log_id;

        PRINT '##############################################';
        PRINT '  PIPELINE COMPLETED SUCCESSFULLY';
        PRINT '  Mode    : ' + UPPER(@load_type);
        PRINT '  Duration: ' + CAST(DATEDIFF(second,@pipeline_start,@pipeline_end) AS VARCHAR) + ' seconds';
        PRINT '  Rows    : ' + CAST(@total_rows AS VARCHAR);
        PRINT '##############################################';

    END TRY
    BEGIN CATCH
        SET @pipeline_end = GETDATE();
        UPDATE control.pipeline_log
        SET status='FAILED', end_time=@pipeline_end,
            duration_seconds=DATEDIFF(second,@pipeline_start,@pipeline_end),
            error_message=ERROR_MESSAGE()
        WHERE log_id=@log_id;
        PRINT 'PIPELINE FAILED: ' + ERROR_MESSAGE();
        THROW;
    END CATCH
END;
GO

PRINT 'dbo.run_pipeline ready.';
GO

-- ============================================================
-- RUN PIPELINE
-- First time + every run after:
EXEC dbo.run_pipeline @load_type = 'incremental';

-- After first run, use incremental:
-- EXEC dbo.run_pipeline;

