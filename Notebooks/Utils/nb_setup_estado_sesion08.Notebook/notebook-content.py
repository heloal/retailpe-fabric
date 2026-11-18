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

# # nb_setup_estado_sesion08 - deja los datos como al final de la Sesion 8
# 
# **Data Wizard Academy - RetailPe** (notebook de preparacion, NO es material de clase)
# 
# Construye, desde Bronze, el estado de datos con el que termina la Sesion 8:
# 
# - `lh_silver`: silver_ventas (R1, R2, R7, MERGE), silver_ventas_cuarentena, silver_clientes (SCD2),
#   silver_productos (R3), silver_tipo_cambio, silver_tiendas.
# - `lh_gold`: dim_tiempo, dim_cliente, dim_producto, dim_tienda (con fila -1 DESCONOCIDO) y hechos_ventas.
# 
# Requisito: `pl_bronze_metadata` ejecutado (las 5 tablas bronze_* en lh_bronze).
# Es idempotente: se puede correr las veces que haga falta y deja el mismo resultado.
# Los notebooks de laboratorio de cada sesion (carpetas Sesion-01 ... Sesion-08) son el material de
# clase; este notebook solo garantiza que todos los alumnos partan del mismo estado de datos.

# MARKDOWN ********************

# ## Parte 1 - Silver: ventas (R1, R2, R7, cuarentena, MERGE)

# CELL ********************

fecha_proceso = ""
fecha_inicio_backfill = "2000-01-01"   # carga historica completa
dias_tolerancia = 3
id_ejecucion = "setup_sesion08"
simular_fallo = False

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

spark.conf.set("spark.sql.legacy.timeParserPolicy", "CORRECTED")

T_BRONZE = f"{LH_BRONZE}.bronze_ventas"
T_SILVER = f"{LH_SILVER}.silver_ventas"
T_CUARENTENA = f"{LH_SILVER}.silver_ventas_cuarentena"

# CELL ********************

if dias_tolerancia is None or int(dias_tolerancia) < 0:
    raise ValueError("dias_tolerancia debe ser un entero >= 0")

f_fin = resolver_fecha_proceso(fecha_proceso)
if str(fecha_inicio_backfill).strip() != "":
    f_inicio = resolver_fecha_proceso(fecha_inicio_backfill)
    modo = "BACKFILL"
else:
    f_inicio = f_fin - timedelta(days=int(dias_tolerancia))
    modo = "DIARIO (R7)"

if f_inicio > f_fin:
    raise ValueError(f"fecha_inicio_backfill ({f_inicio}) es posterior a fecha_proceso ({f_fin})")

print("id_ejecucion:", id_ejecucion)
print("Modo:", modo, "| ventana:", f_inicio, "->", f_fin)

# CELL ********************

df_bronze = spark.read.table(T_BRONZE)
filas_bronze = df_bronze.count()

if simular_fallo:
    raise RuntimeError("Fallo simulado a proposito (parametro simular_fallo=True) - Sesion 11")

texto_fecha = F.trim(F.col("fecha_venta").cast(StringType()))
fecha_normalizada = F.coalesce(
    F.to_date(F.substring(texto_fecha, 1, 10), "yyyy-MM-dd"),
    F.to_date(F.substring(texto_fecha, 1, 10), "dd/MM/yyyy"),
)

df_tipado = (df_bronze
    .withColumn("monto", F.col("monto").cast(DoubleType()))
    .withColumn("fecha_venta", fecha_normalizada))

# Deduplicar por id_venta: una fila por venta (la ultima actualizacion gana)
if "fecha_actualizacion" in df_tipado.columns:
    orden = F.col("fecha_actualizacion").desc_nulls_last()
else:
    orden = F.col("id_venta")
w_dedup = Window.partitionBy("id_venta").orderBy(orden)
df_unico = (df_tipado.withColumn("_rn", F.row_number().over(w_dedup))
    .filter("_rn = 1").drop("_rn"))

print("Filas en bronze_ventas:", filas_bronze, "| despues de deduplicar:", df_unico.count())

# CELL ********************

df_alcance = df_unico.filter(
    F.col("fecha_venta").between(F.lit(f_inicio), F.lit(f_fin)) | F.col("fecha_venta").isNull())

viola_r1 = F.col("monto").isNull() | F.col("fecha_venta").isNull()
viola_r2 = (~viola_r1) & (F.col("monto") <= 0)

df_etiquetado = (df_alcance
    .withColumn("motivo_rechazo_codigo",
        F.when(viola_r1, "R1_FECHA_O_MONTO_NULO").when(viola_r2, "R2_MONTO_NO_POSITIVO"))
    .withColumn("motivo_rechazo_detalle",
        F.when(viola_r1, "La venta no tiene monto o no tiene fecha de venta registrada")
         .when(viola_r2, "El monto de venta es menor o igual a cero")))

df_validas = (df_etiquetado.filter(F.col("motivo_rechazo_codigo").isNull())
    .drop("motivo_rechazo_codigo", "motivo_rechazo_detalle"))
df_rechazadas = (df_etiquetado.filter(F.col("motivo_rechazo_codigo").isNotNull())
    .withColumn("fecha_deteccion", F.current_timestamp())
    .withColumn("id_ejecucion", F.lit(str(id_ejecucion))))

filas_leidas = df_alcance.count()
filas_validas = df_validas.count()
filas_rechazadas = df_rechazadas.count()
print("En alcance:", filas_leidas, "| validas:", filas_validas, "| a cuarentena:", filas_rechazadas)

# CELL ********************

if spark.catalog.tableExists(T_CUARENTENA):
    df_rech_alineado = alinear_a_destino(df_rechazadas, T_CUARENTENA)
    df_rech_alineado.createOrReplaceTempView("origen_cuarentena")
    spark.sql(f"""
        MERGE INTO {T_CUARENTENA} AS destino
        USING origen_cuarentena AS origen
        ON destino.id_venta = origen.id_venta
        WHEN MATCHED THEN UPDATE SET
            destino.motivo_rechazo_codigo  = origen.motivo_rechazo_codigo,
            destino.motivo_rechazo_detalle = origen.motivo_rechazo_detalle,
            destino.id_ejecucion           = origen.id_ejecucion
        WHEN NOT MATCHED THEN INSERT *
    """)
else:
    df_rechazadas.write.format("delta").mode("overwrite").saveAsTable(T_CUARENTENA)

print("Filas en cuarentena (total):", spark.read.table(T_CUARENTENA).count())

# CELL ********************

df_validas.select("id_venta").distinct().createOrReplaceTempView("ids_validos_hoy")

filas_graduadas = spark.sql(f"""
    SELECT COUNT(*) AS n FROM {T_CUARENTENA} c
    WHERE c.id_venta IN (SELECT id_venta FROM ids_validos_hoy)
""").collect()[0]["n"]

spark.sql(f"""
    MERGE INTO {T_CUARENTENA} AS destino
    USING ids_validos_hoy AS origen
    ON destino.id_venta = origen.id_venta
    WHEN MATCHED THEN DELETE
""")
print("Ventas graduadas (salen de cuarentena):", filas_graduadas)

# CELL ********************

if spark.catalog.tableExists(T_SILVER):
    df_validas_alineado = alinear_a_destino(df_validas, T_SILVER)
    df_validas_alineado.createOrReplaceTempView("origen_ventas")
    spark.sql(f"""
        MERGE INTO {T_SILVER} AS destino
        USING origen_ventas AS origen
        ON destino.id_venta = origen.id_venta
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)
else:
    df_validas.write.format("delta").mode("overwrite").saveAsTable(T_SILVER)

filas_silver_total = spark.read.table(T_SILVER).count()
print("silver_ventas (total):", filas_silver_total)

# CELL ********************

resultado = {
    "notebook": "nb_job_silver_ventas",
    "id_ejecucion": str(id_ejecucion),
    "fecha_proceso": str(f_fin),
    "ventana": f"{f_inicio}..{f_fin}",
    "filas_leidas": int(filas_leidas),
    "filas_escritas": int(filas_validas),
    "filas_cuarentena": int(filas_rechazadas),
    "filas_graduadas": int(filas_graduadas),
    "filas_silver_total": int(filas_silver_total),
}
print(json.dumps(resultado, indent=2))
print('--- fin de bloque ---')

# MARKDOWN ********************

# ## Parte 2 - Silver: clientes (SCD Tipo 2)

# CELL ********************

fecha_proceso = ""
id_ejecucion = "setup_sesion08"
max_pct_bajas = 20

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

# CELL ********************

duplicados_vigentes = (spark.read.table(T_SILVER).filter("es_actual = true")
    .groupBy("id_cliente").count().filter("count > 1").count())
if duplicados_vigentes > 0:
    raise RuntimeError(f"{duplicados_vigentes} clientes con mas de una version vigente")

total_versiones = spark.read.table(T_SILVER).count()
print("Versiones totales en silver_clientes:", total_versiones, "| vigentes duplicadas: 0")

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
print('--- fin de bloque ---')

# MARKDOWN ********************

# ## Parte 3 - Silver: productos (R3) y tipo de cambio

# CELL ********************

fecha_proceso = ""
id_ejecucion = "setup_sesion08"

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
print('--- fin de bloque ---')

# MARKDOWN ********************

# ## Parte 4 - Gold: star schema (Sesion 8)

# CELL ********************

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from datetime import date

# CELL ********************

df_bronze_tiendas = spark.read.table("lh_bronze.bronze_tiendas")

df_silver_tiendas = (df_bronze_tiendas
    .withColumn("id_tienda", F.col("id_tienda").cast("int"))
    .withColumn("nombre", F.col("nombre").cast("string"))
    .withColumn("ciudad", F.col("ciudad").cast("string")))

(df_silver_tiendas.write.format("delta").mode("overwrite")
    .saveAsTable("lh_silver.silver_tiendas"))

spark.sql("SELECT * FROM lh_silver.silver_tiendas").show()

# CELL ********************

df_dim_tiempo = spark.sql("""
    SELECT explode(sequence(to_date('2024-01-01'), to_date('2027-12-31'), interval 1 day)) AS fecha
""")

df_dim_tiempo = (df_dim_tiempo
    .withColumn("sk_tiempo", F.date_format("fecha", "yyyyMMdd").cast("int"))
    .withColumn("anio", F.year("fecha"))
    .withColumn("mes", F.month("fecha"))
    .withColumn("trimestre", F.quarter("fecha"))
    .withColumn("nombre_mes", F.date_format("fecha", "MMMM"))
    .withColumn("dia_semana", F.dayofweek("fecha"))
    .withColumn("nombre_dia_semana", F.date_format("fecha", "EEEE"))
    .withColumn("es_fin_de_semana", F.col("dia_semana").isin(1, 7)))

(df_dim_tiempo.write.format("delta").mode("overwrite")
    .saveAsTable("lh_gold.dim_tiempo"))

spark.sql("SELECT COUNT(*) AS filas_dim_tiempo FROM lh_gold.dim_tiempo").show()
df_dim_tiempo.show(5)

# CELL ********************

df_clientes_vigentes = spark.read.table("lh_silver.silver_clientes").filter("es_actual = true")

w_cliente = Window.orderBy("id_cliente")
df_dim_cliente = df_clientes_vigentes.withColumn("sk_cliente", F.row_number().over(w_cliente))

fila_desconocido_cliente = spark.createDataFrame(
    [(-1, "DESCONOCIDO", "N/A", "N/A", date(1900, 1, 1), date(9999, 12, 31), True)],
    ["sk_cliente", "id_cliente", "nombre", "ciudad",
     "fecha_inicio_vigencia", "fecha_fin_vigencia", "es_actual"]
)

df_dim_cliente_final = df_dim_cliente.select(
    "sk_cliente", "id_cliente", "nombre", "ciudad",
    "fecha_inicio_vigencia", "fecha_fin_vigencia", "es_actual"
).unionByName(fila_desconocido_cliente)

(df_dim_cliente_final.write.format("delta").mode("overwrite")
    .saveAsTable("lh_gold.dim_cliente"))

spark.sql("SELECT COUNT(*) AS filas_dim_cliente FROM lh_gold.dim_cliente").show()

# CELL ********************

df_productos_silver = spark.read.table("lh_silver.silver_productos")

w_producto = Window.orderBy("id_producto")
df_dim_producto = df_productos_silver.withColumn("sk_producto", F.row_number().over(w_producto))

fila_desconocido_producto = spark.createDataFrame(
    [(-1, -1, "DESCONOCIDO", "Sin categoria", 0.0)],
    ["sk_producto", "id_producto", "nombre", "categoria", "precio_usd"]
)

df_dim_producto_final = df_dim_producto.select(
    "sk_producto", "id_producto", "nombre", "categoria", "precio_usd"
).unionByName(fila_desconocido_producto)

(df_dim_producto_final.write.format("delta").mode("overwrite")
    .saveAsTable("lh_gold.dim_producto"))

spark.sql("SELECT COUNT(*) AS filas_dim_producto FROM lh_gold.dim_producto").show()

# CELL ********************

df_tiendas_silver = spark.read.table("lh_silver.silver_tiendas")

w_tienda = Window.orderBy("id_tienda")
df_dim_tienda = df_tiendas_silver.withColumn("sk_tienda", F.row_number().over(w_tienda))

fila_desconocido_tienda = spark.createDataFrame(
    [(-1, -1, "DESCONOCIDO", "N/A")],
    ["sk_tienda", "id_tienda", "nombre", "ciudad"]
)

df_dim_tienda_final = df_dim_tienda.select(
    "sk_tienda", "id_tienda", "nombre", "ciudad"
).unionByName(fila_desconocido_tienda)

(df_dim_tienda_final.write.format("delta").mode("overwrite")
    .saveAsTable("lh_gold.dim_tienda"))

spark.sql("SELECT COUNT(*) AS filas_dim_tienda FROM lh_gold.dim_tienda").show()

# CELL ********************

df_silver_ventas = spark.read.table("lh_silver.silver_ventas")
df_dim_producto_lookup = spark.read.table("lh_gold.dim_producto").filter("sk_producto != -1")
df_dim_tienda_lookup = spark.read.table("lh_gold.dim_tienda").filter("sk_tienda != -1")
df_dim_cliente_lookup = spark.read.table("lh_gold.dim_cliente").filter("es_actual = true AND sk_cliente != -1")
df_dim_tiempo_lookup = spark.read.table("lh_gold.dim_tiempo").select("sk_tiempo", "fecha")

# NOTA: silver_ventas no tiene id_producto (fuera del alcance actual de bronze_ventas /
# dbo.ventas -- ver Codigo-Metadata-Driven-Sesion-03.pdf). Por eso el join contra
# dim_producto queda documentado como pendiente para cuando ventas incorpore el detalle
# de linea de producto; hoy se construye el hecho con las llaves que SI existen en
# silver_ventas: id_tienda, id_cliente, fecha_venta.
df_hechos_ventas = (df_silver_ventas.alias("v")
    .join(df_dim_tienda_lookup.alias("t"), on=F.col("v.id_tienda") == F.col("t.id_tienda"), how="left")
    .join(df_dim_cliente_lookup.alias("c"), on=F.col("v.id_cliente") == F.col("c.id_cliente"), how="left")
    .join(df_dim_tiempo_lookup.alias("f"), on=F.col("v.fecha_venta") == F.col("f.fecha"), how="left")
    .select(
        "v.id_venta", "v.monto", "v.fecha_venta",
        F.coalesce(F.col("t.sk_tienda"), F.lit(-1)).alias("sk_tienda"),
        F.coalesce(F.col("c.sk_cliente"), F.lit(-1)).alias("sk_cliente"),
        F.col("f.sk_tiempo"),
    ))

# CELL ********************

total_hechos = df_hechos_ventas.count()
sin_tienda = df_hechos_ventas.filter("sk_tienda = -1").count()
sin_cliente = df_hechos_ventas.filter("sk_cliente = -1").count()
sin_tiempo = df_hechos_ventas.filter("sk_tiempo IS NULL").count()

print("Total lineas de venta:", total_hechos)
print("Sin tienda valida (R4, asignadas a sk_tienda = -1):", sin_tienda)
print("Sin cliente vigente (asignadas a sk_cliente = -1):", sin_cliente)
print("Sin fecha dentro del calendario de dim_tiempo:", sin_tiempo)

# CELL ********************

(df_hechos_ventas.write.format("delta").mode("overwrite")
    .saveAsTable("lh_gold.hechos_ventas"))

spark.sql("SELECT COUNT(*) AS filas_hechos_ventas FROM lh_gold.hechos_ventas").show()

# CELL ********************

for tabla in ["dim_tiempo", "dim_cliente", "dim_producto", "dim_tienda", "hechos_ventas"]:
    n = spark.sql(f"SELECT COUNT(*) AS n FROM lh_gold.{tabla}").collect()[0]["n"]
    print(f"lh_gold.{tabla}: {n} filas")

spark.sql("""
    SELECT h.id_venta, h.monto, d_t.ciudad AS ciudad_tienda, d_c.nombre AS cliente, f.anio, f.mes
    FROM lh_gold.hechos_ventas h
    LEFT JOIN lh_gold.dim_tienda d_t ON h.sk_tienda = d_t.sk_tienda
    LEFT JOIN lh_gold.dim_cliente d_c ON h.sk_cliente = d_c.sk_cliente
    LEFT JOIN lh_gold.dim_tiempo f ON h.sk_tiempo = f.sk_tiempo
    LIMIT 10
""").show(truncate=False)
