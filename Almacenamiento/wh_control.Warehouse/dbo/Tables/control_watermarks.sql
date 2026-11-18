CREATE TABLE [dbo].[control_watermarks] (
    [id_ingesta]       VARCHAR (20)  NULL,
    [ultimo_watermark] VARCHAR (50)  NULL,
    [actualizado_en]   DATETIME2 (6) NULL
);


GO