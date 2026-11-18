CREATE   PROCEDURE dbo.sp_log_inicio
    @id_ejecucion  VARCHAR(64),
    @pipeline      VARCHAR(100),
    @fecha_proceso VARCHAR(10)
AS
BEGIN
    -- Idempotente: si el orquestador se re-ejecuta con el mismo RunId (no deberia), no duplica
    DELETE FROM dbo.log_ejecucion WHERE id_ejecucion = @id_ejecucion;
    INSERT INTO dbo.log_ejecucion (id_ejecucion, pipeline, fecha_proceso, estado, inicio)
    VALUES (@id_ejecucion, @pipeline, @fecha_proceso, 'EnCurso', SYSUTCDATETIME());
END;

GO