CREATE SECURITY POLICY [seguridad].[pol_ventas_por_tienda]
    ADD FILTER PREDICATE [seguridad].[fn_filtro_tienda]([nombre_tienda]) ON [dbo].[demo_ventas_tienda]
    WITH (STATE = ON);


GO