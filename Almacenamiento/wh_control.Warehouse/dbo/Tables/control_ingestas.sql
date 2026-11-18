CREATE TABLE [dbo].[control_ingestas] (
    [id_ingesta]        VARCHAR (20)  NULL,
    [id_conexion]       VARCHAR (20)  NULL,
    [nombre_fuente]     VARCHAR (100) NULL,
    [objeto_origen]     VARCHAR (300) NULL,
    [modo_carga]        VARCHAR (20)  NULL,
    [columna_watermark] VARCHAR (100) NULL,
    [lakehouse_destino] VARCHAR (100) NULL,
    [tabla_destino]     VARCHAR (100) NULL,
    [activo]            VARCHAR (5)   NULL
);


GO