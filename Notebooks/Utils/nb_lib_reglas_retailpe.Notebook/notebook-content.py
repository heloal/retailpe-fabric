# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
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

# # nb_lib_reglas_retailpe - Sesion 12 (libreria, NO se ejecuta sola)
# 
# **Data Wizard Academy - Microsoft Fabric Track - RetailPe**
# 
# Este notebook no lee ni escribe tablas: solo **define funciones**. Otros notebooks lo cargan con
# 
# ```
# %run nb_lib_reglas_retailpe
# ```
# 
# y a partir de ahi pueden llamar a las funciones como si estuvieran escritas en ellos.
# 
# Por que existe: hasta la Sesion 10 la logica de R1/R2, las fechas mezcladas y la deduplicacion
# vivian dentro de `nb_job_silver_ventas`. Para **probar** esa logica (Sesion 12) habia que
# copiarla en otro notebook, y una prueba sobre una copia no prueba nada: si alguien cambia el
# original, la prueba sigue pasando. Ahora la logica vive en UN lugar y la usan el notebook job
# y el notebook de pruebas unitarias.
# 
# | Funcion | Regla / origen |
# |---|---|
# | `resolver_fecha_proceso` | S10 - fecha congelada en hora de Lima |
# | `alinear_a_destino` | S7/S10 - evita DELTA_FAILED_TO_MERGE_FIELDS |
# | `normalizar_fecha` | S10 - fechas en 3 formatos (Caso, seccion 4) |
# | `deduplicar_ultima_version` | S10 - duplicados de Bronze antes del MERGE |
# | `etiquetar_r1_r2` / `separar_validas` | S6 - R1 antes que R2, con motivo |
# 
# Regla de la libreria: **funciones puras** (reciben DataFrames/columnas y devuelven
# DataFrames/columnas). Nada de `print` de resultados, nada de escrituras. Asi se pueden probar
# con DataFrames de 3 filas en segundos.


# CELL ********************

from datetime import datetime
from zoneinfo import ZoneInfo

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import StringType

ZONA_HORARIA = "America/Lima"

R1_CODIGO = "R1_FECHA_O_MONTO_NULO"
R1_DETALLE = "La venta no tiene monto o no tiene fecha de venta registrada"
R2_CODIGO = "R2_MONTO_NO_POSITIVO"
R2_DETALLE = "El monto de venta es menor o igual a cero"


def resolver_fecha_proceso(valor, zona=ZONA_HORARIA):
    """'' o None -> hoy en la zona dada. 'yyyy-MM-dd' -> esa fecha. Otro texto -> ValueError."""
    if valor is None or str(valor).strip() == "":
        return datetime.now(ZoneInfo(zona)).date()
    return datetime.strptime(str(valor).strip(), "%Y-%m-%d").date()


def alinear_a_destino(df, tabla):
    """Castea df a los tipos de la tabla destino, agrega como NULL las columnas que falten y
    devuelve las columnas en el orden del destino."""
    destino = spark.read.table(tabla).schema
    for campo in destino:
        if campo.name in df.columns:
            df = df.withColumn(campo.name, F.col(campo.name).cast(campo.dataType))
        else:
            df = df.withColumn(campo.name, F.lit(None).cast(campo.dataType))
    return df.select(*[c.name for c in destino])


def normalizar_fecha(columna):
    """Texto con fecha en 'yyyy-MM-dd', 'dd/MM/yyyy' o 'yyyy-MM-dd HH:mm:ss' -> date.
    Lo que no calce queda NULL (y R1 lo manda a cuarentena). Requiere
    spark.sql.legacy.timeParserPolicy = CORRECTED en la sesion."""
    texto = F.substring(F.trim(F.col(columna).cast(StringType())), 1, 10)
    return F.coalesce(F.to_date(texto, "yyyy-MM-dd"), F.to_date(texto, "dd/MM/yyyy"))


def deduplicar_ultima_version(df, llave, columna_orden=None):
    """Una fila por llave: la de columna_orden mas reciente (NULL al final).
    Si columna_orden no existe en df, se queda con una fila cualquiera por llave."""
    if columna_orden and columna_orden in df.columns:
        orden = F.col(columna_orden).desc_nulAls_last()
    else:
        orden = F.col(llave)
    w = Window.partitionBy(llave).orderBy(orden)
    return df.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").drop("_rn")


def etiquetar_r1_r2(df):
    """Agrega motivo_rechazo_codigo y motivo_rechazo_detalle. R1 se evalua ANTES que R2."""
    viola_r1 = F.col("monto").isNull() | F.col("fecha_venta").isNull()
    viola_r2 = (~viola_r1) & (F.col("monto") <= 0)
    return (df
        .withColumn("motivo_rechazo_codigo",
            F.when(viola_r1, R1_CODIGO).when(viola_r2, R2_CODIGO))
        .withColumn("motivo_rechazo_detalle",
            F.when(viola_r1, R1_DETALLE).when(viola_r2, R2_DETALLE)))


def separar_validas(df_etiquetado):
    """(validas sin columnas de motivo, rechazadas con motivo)."""
    validas = (df_etiquetado.filter(F.col("motivo_rechazo_codigo").isNull())
        .drop("motivo_rechazo_codigo", "motivo_rechazo_detalle"))
    rechazadas = df_etiquetado.filter(F.col("motivo_rechazo_codigo").isNotNull())
    return validas, rechazadas

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
