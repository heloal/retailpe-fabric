CREATE VIEW dbo.vw_ventas_diarias_por_tienda AS SELECT fecha AS fecha_venta, nombre_tienda AS tienda, ciudad_tienda AS ciudad, num_ventas AS cantidad_ventas, ventas_totales AS ventas_totales_soles, ticket_promedio AS ticket_promedio_soles FROM lh_gold.dbo.agg_ventas_diarias_tienda

GO