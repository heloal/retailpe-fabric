# Fabric notebook source

# METADATA ********************

# META {
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "ade212a5-a19a-413c-baa7-11fbd7cfbaf8",
# META       "default_lakehouse_name": "lh_bronze",
# META       "default_lakehouse_workspace_id": "bfa889cd-7370-4fd7-b815-ade98d5b6d43",
# META       "known_lakehouses": [
# META         {
# META           "id": "ade212a5-a19a-413c-baa7-11fbd7cfbaf8"
# META         },
# META         {
# META           "id": "0479cc73-85db-4b7e-98c6-f4e0d80d2c8e"
# META         },
# META         {
# META           "id": "f3a7d915-6de2-4c17-b508-80edd65afbbe"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# # Sesion 1 - Diagnostico inicial de RetailPe
# 
# **Data Wizard Academy - Microsoft Fabric Track**
# 
# Notebook de demo en vivo (Parte 4, ~15 min). Adjunta el Lakehouse `lh_bronze` antes de ejecutar (menu izquierdo del notebook -> Lakehouses -> Add -> `lh_bronze`).
# 
# ## Por que este notebook existe
# 
# Antes de construir un solo pipeline de RetailPe, un Data Engineer necesita responder: *"que tan sucia esta esta fuente, y que tanto trabajo me va a dar limpiarla?"* A eso se le llama **perfilado (profiling)**: contar filas, revisar el esquema real (no el que dice la documentacion), y medir nulos/formatos raros ANTES de escribir una linea de transformacion.
# 
# **Objetivo de la demo:** perfilar 2 fuentes heterogeneas de RetailPe -- un archivo (`tiendas.csv`) y una base de datos (`dbo.ventas` en Azure SQL) -- con el MISMO metodo, para que quede claro que el perfilado no depende de la tecnologia de origen.

# MARKDOWN ********************

# ---
# ## Fuente 1 - Archivo de referencia (tiendas.csv)
# 
# `tiendas.csv` es la fuente mas simple de RetailPe: un catalogo de referencia (dimension), no una tabla transaccional. Llega como archivo plano porque en la vida real este tipo de dato de referencia casi siempre lo mantiene alguien en Excel o un sistema legado que solo exporta CSV.
# 
# **Requisito:** sube `tiendas.csv` a `Files/raw/` del Lakehouse `lh_bronze` antes de correr esta celda (boton **Upload** en el explorador de archivos del Lakehouse).

# CELL ********************

# Si aun no tienes el archivo, genera uno de ejemplo para la demo:
import csv, os

ruta_local = "/tmp/tiendas.csv"
tiendas = [
    (1, "Tienda Miraflores", "Lima"),
    (2, "Tienda San Isidro", "Lima"),
    (3, "Tienda Arequipa Centro", "Arequipa"),
    (4, "Tienda Cusco Plaza", "Cusco"),
    (5, "Tienda Trujillo Mall", "Trujillo"),
]
with open(ruta_local, "w", newline="", encoding="utf-8") as f:
    w = csv.writer(f)
    w.writerow(["id_tienda", "nombre", "ciudad"])
    w.writerows(tiendas)

# Sube el archivo al Lakehouse (solo si lo generaste local; si ya lo subiste por UI, omite esto)
# notebookutils.fs.cp(f"file:{ruta_local}", "Files/raw/tiendas.csv")
print("Archivo de ejemplo listo en", ruta_local)

# MARKDOWN ********************

# ### Por que leemos con `inferSchema=False`
# 
# Es intencional: TODA columna llega como texto (`string`), incluso `id_tienda` que parece numero. En Bronze el objetivo es guardar el dato **crudo, tal como llego**, sin que Spark adivine tipos y rompa silenciosamente algo (ej. un ID con ceros a la izquierda que Spark convertiria a numero y perderia el formato). Los tipos correctos se deciden con intencion recien en Silver.

# CELL ********************

df_tiendas = spark.read.csv("Files/raw/tiendas.csv", header=True, inferSchema=False)

print("Filas:", df_tiendas.count())
df_tiendas.printSchema()
df_tiendas.show(5, truncate=False)

# MARKDOWN ********************

# **Explicar en clase:** *"Miren el `printSchema()`: todo es `string`, hasta el id. Eso no es un error, es la regla de Bronze: raw-as-is. El dia que necesitemos `id_tienda` como entero, lo casteamos en Silver, con control, no aqui."*

# MARKDOWN ********************

# ---
## Fuente 2 - Base de datos (ventas, via JDBC)

`dbo.ventas` es la fuente transaccional principal de RetailPe: cada fila es una venta real de alguna tienda. A diferencia del CSV, aqui conectamos en vivo a Azure SQL via JDBC -- no copiamos un archivo, leemos directo de la base de datos.

**Autenticacion:** usamos un **Service Principal** (`sp-retailpe-fabric`), no tu cuenta personal. Asi la conexion no depende de un usuario especifico -- cualquier alumno corre el mismo notebook con las mismas credenciales de aplicacion, y si algun dia cambias de cuenta, el pipeline sigue funcionando. El client secret vive en Key Vault, nunca en texto plano en el notebook.

# CELL ********************

# Autenticacion via Service Principal (no cuenta de dominio/usuario).
# El secret se lee de Key Vault, nunca se pega en texto plano aqui.
import notebookutils
from azure.identity import ClientSecretCredential

TENANT_ID = "d1a7fb8d-5ef8-4536-8e37-229e30665b9b"
CLIENT_ID = "eadbac89-5b87-4e5f-ac15-0e44bd7bec3e"
KEYVAULT_URL = "https://kv-retailpe-hector.vault.azure.net/"

client_secret = notebookutils.credentials.getSecret(KEYVAULT_URL, "sp-retailpe-secret")
cred = ClientSecretCredential(tenant_id=TENANT_ID, client_id=CLIENT_ID, client_secret=client_secret)
token = cred.get_token("https://database.windows.net/.default").token

jdbc_url = "jdbc:sqlserver://datawizard.database.windows.net;database=bdacademy;encrypt=true"

df_ventas = (spark.read.format("jdbc")
    .option("url", jdbc_url)
    .option("dbtable", "dbo.ventas")
    .option("accessToken", token)
    .load())

print("Filas:", df_ventas.count())
df_ventas.printSchema()

# MARKDOWN ********************

# ### Alternativa si la conexion JDBC aun no esta lista
# 
# Para no bloquear la demo en vivo por un problema de firewall/credenciales de ultimo minuto, esta celda genera datos simulados con la MISMA forma que `dbo.ventas` real -- incluyendo nulos y fechas en dos formatos distintos a proposito, porque asi llega en produccion. Usala solo si la celda de arriba falla.

# CELL ********************

from pyspark.sql import functions as F
import random

random.seed(7)
filas = []
for i in range(1, 501):
    # a proposito con nulos y formatos de fecha mezclados, como en produccion
    fecha = f"2026-08-{(i % 28) + 1:02d}" if i % 5 else f"{(i % 28) + 1:02d}/08/2026"
    monto = round(random.uniform(20, 500), 2) if i % 17 else None
    filas.append((i, random.randint(1, 5), monto, fecha))

df_ventas = spark.createDataFrame(filas, ["id_venta", "id_tienda", "monto", "fecha_venta"])
print("Filas (simuladas):", df_ventas.count())
df_ventas.show(5)

# MARKDOWN ********************

# ---
# ## Perfilado: el metodo es igual para cualquier fuente
# 
# Aqui esta el punto central de la demo: no importa si la fuente es un CSV o una base de datos SQL, el perfilado se hace con las MISMAS 3 preguntas:
# 
# 1. **Cuantas filas tiene** (`count()`) -- para saber el volumen y detectar si algo se corto en la carga.
# 2. **Que esquema tiene realmente** (`printSchema()`) -- no el que dice la documentacion del sistema origen.
# 3. **Que porcentaje de nulos tiene cada columna** -- el indicador mas rapido de calidad de dato: si `monto` tiene 8% de nulos, ya sabes que Silver va a necesitar una regla para esos casos.

# CELL ********************

def perfilar(df, nombre):
    print(f"=== {nombre} ===")
    print("Filas:", df.count())
    df.printSchema()
    total = df.count()
    df.select([
        (F.sum(F.col(c).isNull().cast("int")) / total * 100).alias(c)
        for c in df.columns
    ]).show()

perfilar(df_tiendas, "tiendas (archivo)")
perfilar(df_ventas, "ventas (BD)")

# MARKDOWN ********************

# **Cierre de la demo:** *"Ya vieron conteo, esquema y % de nulos de DOS fuentes distintas -- un archivo y una base de datos -- con la misma funcion `perfilar()`. Ese es el mismo metodo que van a aplicar en el lab a las 6 fuentes de RetailPe: Azure SQL (ventas, clientes), API REST (productos, tipo_cambio), CSV (tiendas) y el stream de eventos_ecommerce."*
# 
# ### Anota aqui los 3 problemas de calidad mas graves que encontraste:
# 
# 1. 
# 2. 
# 3. 


# MARKDOWN ********************

# ---
# ## Checklist
# 
# - [ ] Perfilado de >= 2 fuentes distintas (BD + archivo)
# - [ ] 3 problemas de calidad anotados
# - [ ] Commit al repo (una vez conectado Git)

