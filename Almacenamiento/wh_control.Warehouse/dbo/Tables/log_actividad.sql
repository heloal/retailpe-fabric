CREATE TABLE [dbo].[log_actividad] (
    [id_ejecucion]     VARCHAR (64)   NOT NULL,
    [actividad]        VARCHAR (100)  NOT NULL,
    [estado]           VARCHAR (20)   NOT NULL,
    [filas_leidas]     BIGINT         NULL,
    [filas_escritas]   BIGINT         NULL,
    [filas_cuarentena] BIGINT         NULL,
    [detalle_json]     VARCHAR (4000) NULL,
    [registrado_en]    DATETIME2 (6)  NOT NULL
);


GO