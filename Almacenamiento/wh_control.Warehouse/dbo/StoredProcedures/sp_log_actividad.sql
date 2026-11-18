CREATE   PROCEDURE dbo.sp_log_actividad
    @id_ejecucion     VARCHAR(64),
    @actividad        VARCHAR(100),
    @filas_leidas     BIGINT,
    @filas_escritas   BIGINT,
    @filas_cuarentena BIGINT,
    @detalle_json     VARCHAR(8000)
AS
BEGIN
    -- Idempotente: un reintento de la actividad reemplaza su fila, no agrega otra
    DELETE FROM dbo.log_actividad WHERE id_ejecucion = @id_ejecucion AND actividad = @actividad;
    INSERT INTO dbo.log_actividad
        (id_ejecucion, actividad, estado, filas_leidas, filas_escritas, filas_cuarentena, detalle_json, registrado_en)
    VALUES
        (@id_ejecucion, @actividad, 'Succeeded', @filas_leidas, @filas_escritas, @filas_cuarentena,
         LEFT(@detalle_json, 4000), SYSUTCDATETIME());
END;

GO