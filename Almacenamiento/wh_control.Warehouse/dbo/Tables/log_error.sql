CREATE TABLE [dbo].[log_error] (
    [id_ejecucion]  VARCHAR (64)   NOT NULL,
    [pipeline]      VARCHAR (100)  NOT NULL,
    [mensaje_error] VARCHAR (4000) NULL,
    [registrado_en] DATETIME2 (6)  NOT NULL
);


GO