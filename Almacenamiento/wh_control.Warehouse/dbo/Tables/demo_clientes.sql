CREATE TABLE [dbo].[demo_clientes] (
    [id_cliente] INT                                             NULL,
    [nombre]     VARCHAR (8000)                                  NULL,
    [ciudad]     VARCHAR (8000)                                  NULL,
    [email]      VARCHAR (30) MASKED WITH (FUNCTION = 'email()') NOT NULL
);


GO