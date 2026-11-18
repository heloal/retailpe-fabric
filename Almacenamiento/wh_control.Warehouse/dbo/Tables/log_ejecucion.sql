CREATE TABLE [dbo].[log_ejecucion] (
    [id_ejecucion]  VARCHAR (64)   NOT NULL,
    [pipeline]      VARCHAR (100)  NOT NULL,
    [fecha_proceso] VARCHAR (10)   NULL,
    [estado]        VARCHAR (20)   NOT NULL,
    [inicio]        DATETIME2 (6)  NOT NULL,
    [fin]           DATETIME2 (6)  NULL,
    [duracion_seg]  INT            NULL,
    [mensaje]       VARCHAR (4000) NULL
);


GO