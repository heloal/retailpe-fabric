CREATE VIEW dbo.vw_ventas_mensuales AS SELECT anio, mes, nombre_mes, num_ventas AS cantidad_ventas, ventas_totales AS ventas_totales_soles, ticket_promedio AS ticket_promedio_soles, variacion_ventas_pct AS variacion_vs_mes_anterior_pct FROM lh_gold.dbo.agg_ventas_mensual

GO