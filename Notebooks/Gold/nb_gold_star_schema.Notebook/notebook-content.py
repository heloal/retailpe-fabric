# Fabric notebook source

# METADATA ********************

# META {
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "f3a7d915-6de2-4c17-b508-80edd65afbbe",
# META       "default_lakehouse_name": "lh_gold",
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

# # Sesion 8 - Parte 3: Construccion del star schema en Gold
# 
# **Data Wizard Academy - Microsoft Fabric Track**
# 
# Notebook **NUEVO**. Es la primera vez que el proyecto RetailPe tiene una capa Gold --
# verificado: la Sesion 1 solo creo `lh_bronze` y `lh_silver` (Parte E del lab de la
# Sesion 1); la decision "Lakehouse o Warehouse para Gold" quedo como ejercicio
# justificado por escrito en ese notebook, pero el item **nunca se creo**. Hoy se crea
# `lh_gold` (Lakehouse, consistente con la justificacion de la Sesion 1: Direct Lake
# sobre Power BI no requiere Warehouse) y se materializa el star schema completo.
# 
# **Antes de correr este notebook:** `New item -> Lakehouse -> lh_gold` en
# `RetailPe-DEV`. Adjunta a este notebook DOS lakehouses: `lh_silver` (default, para
# leer las fuentes limpias) y `lh_gold` (adicional, para escribir las tablas
# dimensionales y de hechos). Con ambos adjuntos, Fabric permite referenciar tablas de un
# lakehouse no-default con el prefijo `nombre_lakehouse.tabla` tanto en PySpark como en
# Spark SQL -- exactamente como se usa en las celdas de escritura de este notebook.
# 
# ## Diseño ya decidido en la Parte 2 (teoria, slides de la Sesion 8)
# 
# - **Grano (R6):** una fila de `hechos_ventas` = una linea de venta (mismo grano que
#   `silver_ventas`, sin agregar).
# - **Dimensiones:** `dim_cliente` (desde `silver_clientes`, solo `es_actual = true`),
#   `dim_producto` (desde `silver_productos`), `dim_tienda` (desde `silver_tiendas` --
#   minimo, ver Paso 0), `dim_tiempo` (calendario generado, no viene de ninguna fuente).
#   `dim_fecha` **no** existe todavia como tabla de negocio propia -- `dim_tiempo` cumple
#   ese rol.
# - **Surrogate keys:** `sk_cliente`, `sk_producto`, `sk_tienda`, `sk_tiempo` -- generadas
#   en Gold, independientes del id de negocio del sistema origen, y obligatorias para que
#   SCD2 funcione (un `sk_cliente` distinto por cada version historica de
#   `silver_clientes`, no uno solo por `id_cliente`).
# - **R4 (integridad referencial):** el `LEFT JOIN` de `hechos_ventas` contra las
#   dimensiones ES la validacion. Una `sk_producto` o `sk_tienda` nula tras el join
#   expone una venta sin producto/tienda valido. Decision tomada hoy: esas ventas se
#   asignan a una fila **"Desconocido"** con `sk = -1`, sembrada en cada dimension --
#   patron estandar de modelado dimensional para no descartar la venta ni romper el
#   join.
# 
# ## Honestidad de alcance
# 
# Agregados y metricas materializadas (ticket promedio, ventas por mes) son Sesion 9,
# no hoy. Direct Lake / conexion a Power BI tambien es Sesion 9. Hoy el notebook termina
# cuando las 5 tablas Gold existen, con integridad referencial validada y expuesta.

# CELL ********************

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from datetime import date

# MARKDOWN ********************

# ---
# ## Paso 0 - `silver_tiendas`: limpieza minima, primera vez
# 
# `tiendas` esta activa desde la Sesion 3 (`bronze_tiendas`, CSV), pero a diferencia de
# `ventas`/`clientes`/`productos`/`tipo_cambio`, nunca tuvo su propio notebook de Silver
# dedicado -- honestamente, porque el CSV (`id_tienda, nombre, ciudad`) ya llega
# razonablemente limpio y no amerita una sesion completa. Aun asi, el principio Medallion
# exige que pase por Silver antes de llegar a Gold: aqui se hace el paso minimo (cast de
# tipos, sin reglas de negocio adicionales porque ninguna aplica) para no saltar
# directamente de Bronze a Gold.

# CELL ********************

df_bronze_tiendas = spark.read.table("lh_bronze.bronze_tiendas")

df_silver_tiendas = (df_bronze_tiendas
    .withColumn("id_tienda", F.col("id_tienda").cast("int"))
    .withColumn("nombre", F.col("nombre").cast("string"))
    .withColumn("ciudad", F.col("ciudad").cast("string")))

(df_silver_tiendas.write.format("delta").mode("overwrite")
    .saveAsTable("lh_silver.silver_tiendas"))

spark.sql("SELECT * FROM lh_silver.silver_tiendas").show()

# MARKDOWN ********************

# ---
# ## Paso 1 - `dim_tiempo`: calendario generado
# 
# `dim_tiempo` no proviene de ninguna fuente de RetailPe -- es una dimension estandar en
# modelado dimensional, generada directamente en Gold con un rango de fechas que cubre
# holgadamente el horizonte del proyecto.

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

# MARKDOWN ********************

# > **Nota:** `sk_tiempo` se construye como `yyyyMMdd` (entero), no con
# > `monotonically_increasing_id()` -- es el unico caso donde la surrogate key tiene
# > significado de negocio (una convencion muy comun para dimensiones de tiempo,
# > porque permite filtrar por rango de fechas sin hacer join). Para `dim_cliente`,
# > `dim_producto` y `dim_tienda` si se usa una superficie generada, sin significado de
# > negocio -- ver Paso 2.

# MARKDOWN ********************

# ---
# ## Paso 2 - `dim_cliente`: solo la version vigente, mas la fila "Desconocido"
# 
# `silver_clientes` (Sesion 7, SCD Tipo 2) tiene el historial COMPLETO -- multiples filas
# por `id_cliente` cuando hubo cambios. La dimension activa de Gold usa unicamente
# `es_actual = true`: el estado vigente de cada cliente. El historial completo sigue
# disponible en `silver_clientes` para analisis futuros del tipo "ventas por version
# historica del cliente" (ese analisis exigiria un join distinto, por rango de fecha,
# que no se construye hoy).

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

# MARKDOWN ********************

# ---
# ## Paso 3 - `dim_producto`

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

# MARKDOWN ********************

# ---
# ## Paso 4 - `dim_tienda`

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

# MARKDOWN ********************

# ---
# ## Paso 5 - `hechos_ventas`: el grano es una linea de venta (R6)
# 
# `silver_ventas` (Sesion 7, MERGE) ya tiene el grano correcto -- una fila por linea de
# venta, sin agregar. Aqui solo se traen las surrogate keys de cada dimension mediante
# `LEFT JOIN` por la llave de negocio. Un `LEFT JOIN` (no `INNER JOIN`) es deliberado:
# una venta sin producto o tienda valido NO debe desaparecer silenciosamente -- debe
# seguir existiendo en `hechos_ventas`, apuntando a la fila "Desconocido" (`sk = -1`) de
# la dimension correspondiente. Esa es, exactamente, la validacion de integridad
# referencial que pide R4.

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

# MARKDOWN ********************

# ### Verificar violaciones de R4 antes de decidir el tratamiento final

# CELL ********************

total_hechos = df_hechos_ventas.count()
sin_tienda = df_hechos_ventas.filter("sk_tienda = -1").count()
sin_cliente = df_hechos_ventas.filter("sk_cliente = -1").count()
sin_tiempo = df_hechos_ventas.filter("sk_tiempo IS NULL").count()

print("Total lineas de venta:", total_hechos)
print("Sin tienda valida (R4, asignadas a sk_tienda = -1):", sin_tienda)
print("Sin cliente vigente (asignadas a sk_cliente = -1):", sin_cliente)
print("Sin fecha dentro del calendario de dim_tiempo:", sin_tiempo)

# MARKDOWN ********************

# ### Decision tomada hoy sobre R4
# 
# Las ventas sin tienda o cliente valido **no se excluyen** de `hechos_ventas`: ya
# quedaron apuntando a `sk_tienda = -1` / `sk_cliente = -1` (fila "Desconocido") gracias
# al `coalesce` del paso anterior. Es la decision explicita que R4 exige tomar: "un LEFT
# JOIN que produce nulls expone que ventas no tienen producto/tienda validos -- decidir
# si van a una fila 'Desconocido' o se excluyen". Hoy: van a "Desconocido". Una venta sin
# `sk_tiempo` (fecha fuera del rango 2024-01-01 a 2027-12-31 de `dim_tiempo`) si se
# reporta como error de datos -- no deberia ocurrir con el rango generado, y si ocurre
# amerita revisar `dim_tiempo`, no ocultarla con un sk ficticio de tiempo.

# CELL ********************

(df_hechos_ventas.write.format("delta").mode("overwrite")
    .saveAsTable("lh_gold.hechos_ventas"))

spark.sql("SELECT COUNT(*) AS filas_hechos_ventas FROM lh_gold.hechos_ventas").show()

# MARKDOWN ********************

# ---
# ## Paso 6 - Verificacion final: el star schema completo

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

# MARKDOWN ********************

# ---
# ## Checklist de la Sesion 8 - Parte 3
# 
# - [ ] `lh_gold` existe (creado hoy -- no existia desde la Sesion 1).
# - [ ] `dim_tiempo`, `dim_cliente`, `dim_producto`, `dim_tienda` y `hechos_ventas`
#       existen en `lh_gold`, todas en formato Delta.
# - [ ] `dim_cliente` usa solo `es_actual = true` de `silver_clientes` (SCD2), mas la fila
#       "Desconocido" `sk_cliente = -1`.
# - [ ] `hechos_ventas` tiene el mismo numero de filas que `silver_ventas` -- ninguna
#       venta se descarto por R4; las que no tenian tienda/cliente valido quedaron con
#       `sk = -1`.
# - [ ] Puedes explicar por que `hechos_ventas` usa surrogate keys (`sk_tienda`,
#       `sk_cliente`) y no los ids de negocio (`id_tienda`, `id_cliente`) directamente.
# - [ ] Puedes explicar por que `dim_producto` no se joinea todavia contra
#       `hechos_ventas` (silver_ventas no trae `id_producto` en su forma actual).
# 
# **Honestidad de alcance:** no hay agregados materializados (ticket promedio, ventas
# por mes) ni conexion a Power BI / Direct Lake -- eso es Sesion 9.

# MARKDOWN ********************

# ---
# ## Sesion 11 - Continuacion: contrato de salida para el log
# 
# **Data Wizard Academy - Microsoft Fabric Track**
# 
# Desde la Sesion 10 este notebook corre dentro de `pl_maestro_retailpe`. Hoy se le agrega UNA
# celda: devuelve el mismo JSON que los notebooks job (`filas_leidas`, `filas_escritas`,
# `filas_cuarentena`), para que el pipeline lo registre en `wh_control.dbo.log_actividad`.
# 
# Las violaciones de R4 (ventas sin tienda / sin cliente / sin fecha, calculadas arriba en este
# mismo notebook) viajan en el detalle: asi quedan en el log de cada noche y se puede ver su
# tendencia sin abrir el notebook.
# 
# `exit()` va siempre en la ultima celda: lo que este debajo no se ejecuta. Si corres el notebook a
# mano, la ejecucion se detiene aqui (es normal).

# CELL ********************

import json

filas_silver_ventas = spark.read.table("lh_silver.silver_ventas").count()
filas_hechos = spark.read.table("lh_gold.hechos_ventas").count()

resultado = {
    "notebook": "nb_gold_star_schema",
    "filas_leidas": int(filas_silver_ventas),
    "filas_escritas": int(filas_hechos),
    "filas_cuarentena": 0,
    "r4_sin_tienda": int(sin_tienda),
    "r4_sin_cliente": int(sin_cliente),
    "r4_sin_tiempo": int(sin_tiempo),
}
print(json.dumps(resultado, indent=2))
notebookutils.notebook.exit(json.dumps(resultado))
