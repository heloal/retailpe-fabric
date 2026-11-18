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

# # nb_job_silver_clientes - Sesion 10 (notebook de PRODUCCION)
# 
# **Data Wizard Academy - Microsoft Fabric Track - RetailPe**
# 
# Version de produccion del SCD Tipo 2 de la Sesion 7 (R5: *"El cliente conserva su historial al
# cambiar de datos"*). En la Sesion 7 los cambios venian de un `createDataFrame` de demo; aqui
# vienen de la **foto completa** que `pl_bronze_metadata` deja cada noche en
# `lh_bronze.bronze_clientes` (columnas reales: `id_cliente`, `nombre`, `email`, `ciudad`,
# `fecha_registro`, `fecha_actualizacion`).
# 
# | Caso | Que se hace en `silver_clientes` |
# |---|---|
# | Cliente nuevo | INSERT version vigente |
# | Cambio de `nombre` o `ciudad` | cerrar version actual + INSERT version nueva (patron staged S7) |
# | Cliente que ya no esta en la foto | cerrar version actual (baja) |
# | Sin cambios | nada |
# 
# **Proteccion clave:** si `bronze_clientes` llega vacia (la Copy fallo a medias), el notebook se
# detiene. Sin esa proteccion "cerraria" a los 380k clientes en una sola noche.

# MARKDOWN ********************

# ## Celda de parametros (Toggle parameter cell)

# PARAMETERS CELL ********************

fecha_proceso = ""
id_ejecucion = "manual"
max_pct_bajas = 20

# MARKDOWN ********************

# ## Paso 0 - Configuracion
# 
# Atributos que generan nueva version: `nombre` y `ciudad`. La comparacion es **null-safe**
# (`eqNullSafe`): en la Sesion 7 se uso `!=`, y `NULL != 'Lima'` da NULL (no TRUE), asi que un
# cliente sin ciudad que pasa a tener ciudad no generaba version nueva.

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

T_BRONZE = f"{LH_BRONZE}.bronze_clientes"
T_SILVER = f"{LH_SILVER}.silver_clientes"
ATRIBUTOS_SCD2 = ["nombre", "ciudad"]
FIN_ABIERTO = date(9999, 12, 31)

f_proceso = resolver_fecha_proceso(fecha_proceso)
print("id_ejecucion:", id_ejecucion, "| fecha_proceso:", f_proceso)

# MARKDOWN ********************

# ## Paso 1 - Leer la foto de Bronze y protegerse de una foto vacia
# 
# Tambien se deduplica por `id_cliente` (la fila con `fecha_actualizacion` mas reciente): el MERGE
# falla si dos filas del origen corresponden al mismo cliente.

# CELL ********************

df_foto = spark.read.table(T_BRONZE)
filas_leidas = df_foto.count()
if filas_leidas == 0:
    raise RuntimeError("bronze_clientes esta vacia: se detiene para no dar de baja a todos los clientes")

if "fecha_actualizacion" in df_foto.columns:
    orden = F.col("fecha_actualizacion").desc_nulls_last()
else:
    orden = F.col("id_cliente")
df_foto = (df_foto.withColumn("_rn", F.row_number().over(Window.partitionBy("id_cliente").orderBy(orden)))
    .filter("_rn = 1").drop("_rn")
    .select("id_cliente", "nombre", "ciudad"))
print("Clientes en la foto de hoy:", df_foto.count())

# MARKDOWN ********************

# ## Paso 2 - Carga inicial (solo si silver_clientes no existe)
# 
# En un ambiente nuevo (por ejemplo Prod, Sesion 13) no hay historia: todos entran como version
# vigente. Si la tabla ya existe, esta celda no hace nada.

# CELL ********************

carga_inicial = not spark.catalog.tableExists(T_SILVER)
if carga_inicial:
    (df_foto
        .withColumn("fecha_inicio_vigencia", F.lit(f_proceso))
        .withColumn("fecha_fin_vigencia", F.lit(FIN_ABIERTO))
        .withColumn("es_actual", F.lit(True))
        .write.format("delta").mode("overwrite").saveAsTable(T_SILVER))
    print("Carga inicial de silver_clientes:", spark.read.table(T_SILVER).count(), "filas")
else:
    print("silver_clientes ya existe: se aplica SCD2 incremental")

# MARKDOWN ********************

# ## Paso 3 - Staged SCD2 (mismo patron de la Sesion 7)
# 
# - `cerrar`: `merge_key = id_cliente` -> hace match con la version actual y la cierra.
# - `insertar`: `merge_key = NULL` -> nunca hace match -> siempre INSERT de la version nueva.
# 
# `df_foto` se alinea al tipo de `id_cliente` de Silver antes del join (si en clase quedo `string`
# y Bronze trae `int`, el join no fallaria pero compararia mal).

# CELL ********************

filas_nuevas = filas_cambios = filas_bajas = 0

if not carga_inicial:
    tipo_id = dict((f.name, f.dataType) for f in spark.read.table(T_SILVER).schema)["id_cliente"]
    df_origen = df_foto.withColumn("id_cliente", F.col("id_cliente").cast(tipo_id))
    df_actual = spark.read.table(T_SILVER).filter("es_actual = true")

    cambio = None
    for a in ATRIBUTOS_SCD2:
        dif = ~F.col(f"s.{a}").eqNullSafe(F.col(f"t.{a}"))
        cambio = dif if cambio is None else (cambio | dif)

    base = (df_origen.alias("s")
        .join(df_actual.alias("t"), on="id_cliente", how="left")
        .withColumn("es_nuevo", F.col("t.es_actual").isNull())
        .withColumn("hubo_cambio", F.col("t.es_actual").isNotNull() & cambio))

    cols = [F.col("s.id_cliente").alias("id_cliente")] + [F.col(f"s.{a}").alias(a) for a in ATRIBUTOS_SCD2]
    cerrar = base.filter("hubo_cambio").select(F.col("s.id_cliente").cast(StringType()).alias("merge_key"), *cols)
    insertar = base.filter("hubo_cambio OR es_nuevo").select(F.lit(None).cast(StringType()).alias("merge_key"), *cols)

    filas_nuevas = base.filter("es_nuevo").count()
    filas_cambios = base.filter("hubo_cambio").count()
    cerrar.unionByName(insertar).createOrReplaceTempView("staged_clientes")
    print("Clientes nuevos:", filas_nuevas, "| con cambio (nueva version):", filas_cambios)

# MARKDOWN ********************

# ## Paso 4 - MERGE SCD2
# 
# El `ON` incluye `destino.es_actual = true`: sin eso tambien se "cerrarian" versiones historicas
# ya cerradas (error comun de la Sesion 7). `merge_key` es string para que el `ON` compare
# `CAST(id_cliente AS STRING)` sin depender del tipo numerico.

# CELL ********************

if not carga_inicial and (filas_nuevas + filas_cambios) > 0:
    spark.sql(f"""
        MERGE INTO {T_SILVER} AS destino
        USING staged_clientes AS origen
        ON CAST(destino.id_cliente AS STRING) = origen.merge_key AND destino.es_actual = true
        WHEN MATCHED THEN UPDATE SET
            destino.fecha_fin_vigencia = DATE('{f_proceso}'),
            destino.es_actual = false
        WHEN NOT MATCHED THEN INSERT
            (id_cliente, nombre, ciudad, fecha_inicio_vigencia, fecha_fin_vigencia, es_actual)
        VALUES
            (origen.id_cliente, origen.nombre, origen.ciudad, DATE('{f_proceso}'), DATE('9999-12-31'), true)
    """)
    print("MERGE SCD2 aplicado")

# MARKDOWN ********************

# ## Paso 5 - Bajas: clientes que ya no estan en la foto
# 
# En la Sesion 5 los deletes venian marcados (`operacion = 'D'`). Con una foto completa, un
# cliente **ausente** es una baja: se cierra su version vigente (nunca se borra la historia).
# 
# Guarda de seguridad: si las bajas superan `max_pct_bajas` % de los clientes vigentes, el notebook
# falla en vez de aplicarlas. Una foto incompleta parece "muchas bajas"; es mejor despertar a
# alguien que destruir la dimension.

# CELL ********************

if not carga_inicial:
    vigentes = spark.read.table(T_SILVER).filter("es_actual = true")
    ids_foto = df_foto.select(F.col("id_cliente").cast(StringType()).alias("k"))
    bajas = (vigentes.select(F.col("id_cliente").cast(StringType()).alias("merge_key"))
        .join(ids_foto, F.col("merge_key") == F.col("k"), "left_anti"))
    filas_bajas = bajas.count()
    n_vigentes = vigentes.count()
    pct = (filas_bajas * 100.0 / n_vigentes) if n_vigentes else 0
    print(f"Bajas detectadas: {filas_bajas} de {n_vigentes} vigentes ({pct:.1f}%)")
    if pct > float(max_pct_bajas):
        raise RuntimeError(f"Bajas = {pct:.1f}% > {max_pct_bajas}%: posible foto incompleta, no se aplican")
    if filas_bajas > 0:
        bajas.createOrReplaceTempView("bajas_clientes")
        spark.sql(f"""
            MERGE INTO {T_SILVER} AS destino
            USING bajas_clientes AS origen
            ON CAST(destino.id_cliente AS STRING) = origen.merge_key AND destino.es_actual = true
            WHEN MATCHED THEN UPDATE SET
                destino.fecha_fin_vigencia = DATE('{f_proceso}'),
                destino.es_actual = false
        """)

# MARKDOWN ********************

# ## Paso 6 - Verificacion: un cliente = una sola version vigente
# 
# Si esta consulta devuelve filas, la dimension esta rota (dos versiones `es_actual = true`) y la
# Sesion 8 duplicaria ventas en Gold. Se falla aqui, no en el reporte.

# CELL ********************

duplicados_vigentes = (spark.read.table(T_SILVER).filter("es_actual = true")
    .groupBy("id_cliente").count().filter("count > 1").count())
if duplicados_vigentes > 0:
    raise RuntimeError(f"{duplicados_vigentes} clientes con mas de una version vigente")

total_versiones = spark.read.table(T_SILVER).count()
print("Versiones totales en silver_clientes:", total_versiones, "| vigentes duplicadas: 0")

# MARKDOWN ********************

# ## Paso 7 - Contrato de salida (mismas claves que todos los notebooks job)

# CELL ********************

resultado = {
    "notebook": "nb_job_silver_clientes",
    "id_ejecucion": str(id_ejecucion),
    "fecha_proceso": str(f_proceso),
    "filas_leidas": int(filas_leidas),
    "filas_escritas": int(filas_nuevas + filas_cambios + filas_bajas),
    "filas_cuarentena": 0,
    "clientes_nuevos": int(filas_nuevas),
    "clientes_con_cambio": int(filas_cambios),
    "clientes_baja": int(filas_bajas),
    "carga_inicial": bool(carga_inicial),
}
print(json.dumps(resultado, indent=2))
notebookutils.notebook.exit(json.dumps(resultado))
