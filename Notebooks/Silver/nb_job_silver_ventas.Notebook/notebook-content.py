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

# # nb_job_silver_ventas - Sesion 10 (notebook de PRODUCCION)
# 
# **Data Wizard Academy - Microsoft Fabric Track - RetailPe**
# 
# Este notebook NO reemplaza a `nb_silver_ventas_idempotencia` (Sesiones 4-7): ese es el
# notebook de **laboratorio**, con celdas de demo que insertan filas simuladas (`V-88213`,
# `V-90101`...), renombran tablas (`ALTER TABLE ... RENAME`) y prueban errores a proposito. Si
# un pipeline lo corriera cada noche, volveria a insertar las filas de demo y fallaria en el
# `RENAME` la segunda noche.
# 
# Aqui esta **la misma logica de negocio, consolidada y sin demos**, lista para que
# `pl_maestro_retailpe` la ejecute todas las noches:
# 
# | Regla | Sesion | Que hace aqui |
# |---|---|---|
# | R7 - llegada tardia (3 dias) | S5 | procesa la ventana `fecha_proceso - 3 dias .. fecha_proceso` |
# | R1 - sin monto o sin fecha | S6 | va a `silver_ventas_cuarentena` con motivo |
# | R2 - monto <= 0 | S6 | va a `silver_ventas_cuarentena` con motivo |
# | Graduacion | S6 | si una venta en cuarentena llega corregida, sale de cuarentena |
# | MERGE (upsert) | S7 | inserta ventas nuevas y actualiza las corregidas en `silver_ventas` |
# 
# **Contrato de salida:** al final devuelve un JSON con `filas_leidas`, `filas_escritas` y
# `filas_cuarentena` (la Sesion 11 lo guarda en el log de `wh_control`).
# 
# **Antes de correr:** adjunta `lh_bronze` y `lh_silver`. Todas las tablas van calificadas.
# 
# > **Sesion 12 - refactor:** la logica de fechas, deduplicacion y R1/R2 se movio a
# > `nb_lib_reglas_retailpe` (se carga con `%run`). Las reglas no cambiaron: el notebook produce
# > exactamente el mismo resultado que en la Sesion 10. Lo que cambio es que ahora esa logica tiene
# > pruebas unitarias (`nb_test_unitarios_reglas`).


# MARKDOWN ********************

# ## Celda de parametros
# 
# En Fabric: menu `...` de la celda siguiente -> **Toggle parameter cell** (debe verse la
# etiqueta *Parameters*). El pipeline sobrescribe estos valores; si corres el notebook a mano,
# se usan estos por defecto.
# 
# - `fecha_proceso = ""` -> hoy (hora de Lima). Para reprocesar un dia: `"2026-08-15"`.
# - `fecha_inicio_backfill = ""` -> ventana normal R7. Para una carga historica: `"2026-08-01"`.
# - `simular_fallo = False` -> la Sesion 11 lo pone en `True` para probar las alertas.

# PARAMETERS CELL ********************

fecha_proceso = ""
fecha_inicio_backfill = ""
dias_tolerancia = 3
id_ejecucion = "manual"
simular_fallo = False

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Paso 0 - Cargar la libreria de reglas y configurar
# 
# La celda siguiente contiene SOLO `%run nb_lib_reglas_retailpe` (la magia `%run` va sola en su
# celda). Trae `resolver_fecha_proceso`, `alinear_a_destino`, `normalizar_fecha`,
# `deduplicar_ultima_version`, `etiquetar_r1_r2` y `separar_validas`.
# 
# En el pipeline funciona igual: el notebook job ejecuta la libreria antes de seguir. Por eso
# `nb_lib_reglas_retailpe` debe existir en el MISMO workspace (y desplegarse junto con el job en la
# Sesion 13).

# CELL ********************

%run nb_lib_reglas_retailpe

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import json
from datetime import date, timedelta

LH_BRONZE = "lh_bronze"
LH_SILVER = "lh_silver"

spark.conf.set("spark.sql.legacy.timeParserPolicy", "CORRECTED")

T_BRONZE = f"{LH_BRONZE}.bronze_ventas"
T_SILVER = f"{LH_SILVER}.silver_ventas"
T_CUARENTENA = f"{LH_SILVER}.silver_ventas_cuarentena"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Paso 1 - Resolver la ventana de proceso (R7)
# 
# R7: *"Ventas de dias anteriores pueden llegar hasta 3 dias tarde"*. Cada noche se reprocesa
# la ventana completa, no solo "hoy". Con `fecha_inicio_backfill` la ventana se amplia para una
# carga historica (la primera corrida del pipeline, o despues de un incidente).

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

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Paso 2 - Leer Bronze, tipar y normalizar la fecha
# 
# `bronze_ventas` llega de Azure SQL con `fecha_venta` como **texto en formatos mezclados**
# (`2026-08-05`, `05/08/2026`, `2026-08-05 14:22:01`). Se prueban los formatos conocidos y se
# toma el primero que funcione; lo que no se pueda leer queda NULL y cae en R1 (cuarentena), no
# se descarta en silencio.
# 
# Bronze tambien trae **duplicados** (problema documentado en el Caso, seccion 4). Un MERGE falla
# si dos filas del origen apuntan al mismo `id_venta`, asi que se deja una fila por venta: la de
# `fecha_actualizacion` mas reciente.
# 
# **Ojo:** los ids NO se castean a mano. `alinear_a_destino` los lleva al tipo que ya tiene
# `silver_ventas` (castear un id alfanumerico a int lo convierte en NULL sin avisar).

# CELL ********************

df_bronze = spark.read.table(T_BRONZE)
filas_bronze = df_bronze.count()

if simular_fallo:
    raise RuntimeError("Fallo simulado a proposito (parametro simular_fallo=True) - Sesion 11")

df_tipado = (df_bronze
    .withColumn("monto", F.col("monto").cast("double"))
    .withColumn("fecha_venta", normalizar_fecha("fecha_venta")))

# Una fila por venta: la ultima actualizacion gana (MERGE exige una fila de origen por llave)
df_unico = deduplicar_ultima_version(df_tipado, "id_venta", "fecha_actualizacion")

print("Filas en bronze_ventas:", filas_bronze, "| despues de deduplicar:", df_unico.count())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Paso 3 - Acotar a la ventana y etiquetar R1 / R2
# 
# Entran al proceso las ventas con fecha dentro de la ventana **y** las ventas sin fecha (no se
# pueden ubicar en ninguna ventana; si se filtraran por fecha desaparecerian para siempre). R1 se
# evalua antes que R2, igual que en la Sesion 6.

# CELL ********************

df_alcance = df_unico.filter(
    F.col("fecha_venta").between(F.lit(f_inicio), F.lit(f_fin)) | F.col("fecha_venta").isNull())

df_validas, df_rechazadas = separar_validas(etiquetar_r1_r2(df_alcance))
df_rechazadas = (df_rechazadas
    .withColumn("fecha_deteccion", F.current_timestamp())
    .withColumn("id_ejecucion", F.lit(str(id_ejecucion))))

filas_leidas = df_alcance.count()
filas_validas = df_validas.count()
filas_rechazadas = df_rechazadas.count()
print("En alcance:", filas_leidas, "| validas:", filas_validas, "| a cuarentena:", filas_rechazadas)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Paso 4 - Cuarentena idempotente (MERGE por id_venta)
# 
# En la Sesion 6 la cuarentena se reescribia completa. En produccion eso no sirve: cada noche se
# reprocesa la ventana R7 y la misma venta mala se volveria a agregar 4 noches seguidas. Con
# `MERGE` por `id_venta`, una venta rechazada existe **una sola vez** en cuarentena; si vuelve a
# fallar, solo se actualiza el motivo y el `id_ejecucion`.
# 
# Si la tabla aun no existe (ambiente nuevo), se crea con la primera escritura.

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

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Paso 5 - Graduacion automatica
# 
# Una venta que estaba en cuarentena y hoy llega **valida** (el origen la corrigio) sale de
# cuarentena. Es la version automatica de la "graduacion" de la Sesion 6 (alli fue manual, fila
# por fila). Se hace ANTES del MERGE a Silver para que nunca exista en las dos tablas a la vez
# durante mucho tiempo.

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

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Paso 6 - MERGE a silver_ventas (S7, ahora parametrizado)
# 
# `UPDATE SET *` / `INSERT *` funcionan porque `alinear_a_destino` dejo el origen con exactamente
# las columnas y tipos del destino. Correr el notebook dos veces con los mismos parametros deja el
# mismo resultado: **idempotente**, que es la condicion para que el pipeline pueda reintentarlo.

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

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Paso 7 - Contrato de salida para el pipeline
# 
# `notebookutils.notebook.exit()` devuelve un texto al pipeline. En la actividad Notebook se lee
# con `@activity('nb_silver_ventas').output.result.exitValue` (verificalo en la pestaña *Output*
# del run). Todos los notebooks job devuelven las MISMAS tres claves numericas; la Sesion 11 las
# registra en `wh_control`.
# 
# `exit()` debe ser la **ultima** celda: lo que este debajo no se ejecuta.

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
notebookutils.notebook.exit(json.dumps(resultado))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
