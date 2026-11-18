CREATE FUNCTION seguridad.fn_filtro_tienda(@nombre_tienda AS VARCHAR(100)) RETURNS TABLE WITH SCHEMABINDING AS RETURN SELECT 1 AS permitido FROM seguridad.usuario_tienda u WHERE u.nombre_tienda = @nombre_tienda AND u.usuario = USER_NAME()

GO