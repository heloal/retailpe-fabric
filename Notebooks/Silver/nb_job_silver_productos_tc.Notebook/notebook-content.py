# Fabric notebook source

# METADATA ********************

# META {
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "0479cc73-85db-4b7e-98c6-f4e0d80d2c8e",
# META       "default_lakehouse_name": "lh_silver",
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

# # nb_job_silver_productos_tc - Sesion 10 (notebook de PRODUCCION)
# 
# **Data Wizard Academy - Microsoft Fabric Track - RetailPe**
# 
# Version de produccion de `nb_silver_productos_tipocambio` (Sesion 8) para las dos fuentes de la
# API REST (`func-retailpe-api`):
# 
# - **productos:** con *Treat array as string* activado en la Copy, `bronze_productos` trae una
#   columna `data` con el arreglo JSON como texto -> `from_json` + `explode`. Si tu Bronze quedo
#   con columnas planas (`id_producto`, `nombre`...), el notebook lo detecta y las usa directo.
#   R3: categoria nula o vacia -> `"Sin categoria"`. Es un catalogo completo: **overwrite**.
# - **tipo_cambio:** la API devuelve **un valor por llamada** (el del dia). En la Sesion 8 se
#   hacia overwrite y por eso habia una sola fila (limitacion que vimos en la Sesion 9 con el
#   preview en USD). Aqui se hace **MERGE por fecha**: cada noche se agrega el dia y, con el tiempo,
#   `silver_tipo_cambio` acumula la historia que Finanzas necesita.

# MARKDOWN ********************

# ## Celda de parametros (Toggle parameter cell)

# PARAMETERS CELL ********************

fecha_proceso = ""
id_ejecucion = "manual"

# MARKDOWN ********************

# ## Paso 0 - Configuracion

# CELL ********************

import json
from datetime import datetime, date, timedelta
from zoneinfo import ZoneInfo

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import (IntegerType, LongType, DoubleType, StringType,
                               DateType, TimestampType, BooleanType)

# Nombres de lakehouse en UN solo lugar. Si en clase tus tablas quedaron en otro
# lakehouse, cambia SOLO estas constantes.
LH_BRONZE = "lh_bronze"
LH_SILVER = "lh_silver"
ZONA_HORARIA = "America/Lima"

def resolver_fecha_proceso(valor):
    """'' -> hoy en Lima. 'yyyy-MM-dd' -> esa fecha. Cualquier otra cosa -> error claro."""
    if valor is None or str(valor).strip() == "":
        return datetime.now(ZoneInfo(ZONA_HORARIA)).date()
    return datetime.strptime(str(valor).strip(), "%Y-%m-%d").date()

def alinear_a_destino(df, tabla):
    """Castea las columnas de df a los tipos de la tabla destino, agrega como NULL las que
    falten y devuelve las columnas en el orden del destino. Evita DELTA_FAILED_TO_MERGE_FIELDS."""
    destino = spark.read.table(tabla).schema
    for campo in destino:
        if campo.name in df.columns:
            df = df.withColumn(campo.name, F.col(campo.name).cast(campo.dataType))
        else:
            df = df.withColumn(campo.name, F.lit(None).cast(campo.dataType))
    return df.select(*[c.name for c in destino])
from pyspark.sql.types import ArrayType, StructType, StructField

f_proceso = resolver_fecha_proceso(fecha_proceso)
print("id_ejecucion:", id_ejecucion, "| fecha_proceso:", f_proceso)

# MARKDOWN ********************

# ## Paso 1 - productos: aplanar el JSON (o usar columnas planas) y aplicar R3
# 
# El esquema del arreglo es el de la Azure Function (`function_app.py`): `id_producto`,
# `nombre`, `categoria`, `precio_usd`.

# CELL ********************

schema_producto = ArrayType(StructType([
    StructField("id_producto", IntegerType()),
    StructField("nombre", StringType()),
    StructField("categoria", StringType()),
    StructField("precio_usd", DoubleType()),
]))

df_bp = spark.read.table(f"{LH_BRONZE}.bronze_productos")
if "data" in df_bp.columns:
    df_productos = (df_bp
        .withColumn("arr", F.from_json(F.col("data").cast(StringType()), schema_producto))
        .select(F.explode("arr").alias("p")).select("p.*"))
elif "id_producto" in df_bp.columns:
    df_productos = df_bp.select(
        F.col("id_producto").cast(IntegerType()).alias("id_producto"),
        F.col("nombre").cast(StringType()).alias("nombre"),
        F.col("categoria").cast(StringType()).alias("categoria"),
        F.col("precio_usd").cast(DoubleType()).alias("precio_usd"))
else:
    raise RuntimeError(f"bronze_productos con columnas inesperadas: {df_bp.columns}")

filas_leidas_prod = df_productos.count()
if filas_leidas_prod == 0:
    raise RuntimeError("bronze_productos sin productos: no se sobrescribe el catalogo con vacio")

df_productos = (df_productos
    .filter(F.col("id_producto").isNotNull())
    .dropDuplicates(["id_producto"])
    .withColumn("categoria",
        F.when(F.col("categoria").isNull() | (F.trim(F.col("categoria")) == ""), "Sin categoria")
         .otherwise(F.trim(F.col("categoria")))))

(df_productos.write.format("delta").mode("overwrite").option("overwriteSchema", "true")
    .saveAsTable(f"{LH_SILVER}.silver_productos"))
filas_prod = spark.read.table(f"{LH_SILVER}.silver_productos").count()
print("silver_productos:", filas_prod, "filas")

# MARKDOWN ********************

# ## Paso 2 - tipo_cambio: MERGE por fecha (acumula historia)
# 
# `overwriteSchema` NO se usa aqui: es un MERGE, la tabla conserva las fechas anteriores. Si el
# mismo dia se corre dos veces, el MERGE actualiza esa fecha (idempotente). Los feriados siguen
# sin valor: rellenarlos es una decision de Finanzas, no del ingeniero.

# CELL ********************

T_TC = f"{LH_SILVER}.silver_tipo_cambio"
df_tc = (spark.read.table(f"{LH_BRONZE}.bronze_tipo_cambio")
    .select(
        F.col("moneda_origen").cast(StringType()).alias("moneda_origen"),
        F.col("moneda_destino").cast(StringType()).alias("moneda_destino"),
        F.col("valor").cast(DoubleType()).alias("valor"),
        F.to_date(F.col("fecha")).alias("fecha"))
    .filter(F.col("fecha").isNotNull() & F.col("valor").isNotNull())
    .dropDuplicates(["fecha", "moneda_origen", "moneda_destino"]))
filas_leidas_tc = df_tc.count()

if spark.catalog.tableExists(T_TC):
    alinear_a_destino(df_tc, T_TC).createOrReplaceTempView("origen_tc")
    spark.sql(f"""
        MERGE INTO {T_TC} AS destino
        USING origen_tc AS origen
        ON destino.fecha = origen.fecha
           AND destino.moneda_origen = origen.moneda_origen
           AND destino.moneda_destino = origen.moneda_destino
        WHEN MATCHED THEN UPDATE SET destino.valor = origen.valor
        WHEN NOT MATCHED THEN INSERT *
    """)
else:
    df_tc.write.format("delta").mode("overwrite").saveAsTable(T_TC)

fechas_tc = spark.read.table(T_TC).select("fecha").distinct().count()
print("silver_tipo_cambio: fechas distintas acumuladas =", fechas_tc)

# MARKDOWN ********************

# ## Paso 3 - Contrato de salida

# CELL ********************

resultado = {
    "notebook": "nb_job_silver_productos_tc",
    "id_ejecucion": str(id_ejecucion),
    "fecha_proceso": str(f_proceso),
    "filas_leidas": int(filas_leidas_prod + filas_leidas_tc),
    "filas_escritas": int(filas_prod + filas_leidas_tc),
    "filas_cuarentena": 0,
    "productos": int(filas_prod),
    "fechas_tipo_cambio": int(fechas_tc),
}
print(json.dumps(resultado, indent=2))
notebookutils.notebook.exit(json.dumps(resultado))
