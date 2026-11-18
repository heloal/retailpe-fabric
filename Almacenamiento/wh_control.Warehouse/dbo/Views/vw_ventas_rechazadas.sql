CREATE VIEW dbo.vw_ventas_rechazadas AS SELECT motivo_rechazo_codigo AS motivo, COUNT(*) AS cantidad_ventas_rechazadas FROM lh_silver.dbo.silver_ventas_cuarentena GROUP BY motivo_rechazo_codigo

GO