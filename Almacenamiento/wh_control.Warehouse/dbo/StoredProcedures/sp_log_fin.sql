CREATE   PROCEDURE dbo.sp_log_fin
    @id_ejecucion VARCHAR(64),
    @estado       VARCHAR(20),
    @mensaje      VARCHAR(8000)
AS
BEGIN
    UPDATE dbo.log_ejecucion
    SET estado       = @estado,
        fin          = SYSUTCDATETIME(),
        duracion_seg = DATEDIFF(SECOND, inicio, SYSUTCDATETIME()),
        mensaje      = LEFT(@mensaje, 4000)
    WHERE id_ejecucion = @id_ejecucion;

    IF @estado = 'Failed'
        INSERT INTO dbo.log_error (id_ejecucion, pipeline, mensaje_error, registrado_en)
        SELECT id_ejecucion, pipeline, LEFT(@mensaje, 4000), SYSUTCDATETIME()
        FROM dbo.log_ejecucion WHERE id_ejecucion = @id_ejecucion;
END;

GO