CREATE TABLE [dbo].[control_conexiones] (
    [id_conexion]         VARCHAR (20)  NULL,
    [nombre]              VARCHAR (100) NULL,
    [tipo_origen]         VARCHAR (20)  NULL,
    [servidor_o_endpoint] VARCHAR (300) NULL,
    [base_datos]          VARCHAR (100) NULL,
    [autenticacion]       VARCHAR (50)  NULL,
    [secreto_keyvault]    VARCHAR (100) NULL,
    [activo]              VARCHAR (5)   NULL
);


GO