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

# # Sesion 4 - Notebooks, Spark e Idempotencia
# 
# **Data Wizard Academy - Microsoft Fabric Track**
# 
# Notebook de laboratorio. Adjunta el Lakehouse `lh_bronze` como default antes de ejecutar (icono Lakehouses en el panel izquierdo -> Add). Escribimos explicitamente en `lh_silver` usando el nombre de tabla completo cuando haga falta.
# 
# Tabla destino: **silver_ventas_stage** en `lh_silver` -- version de practica mecanica de idempotencia, NO la capa Silver curada final (esa la crea la Sesion 6 como `silver_ventas`).
# 
# Duracion en clase: ~15 min (Parte 1) + ~15 min (Parte 2) + ~10 min (Parte 3) + ~15 min (Parte 4).

# MARKDOWN ********************

# ---
# ## Parte 0 - Verificacion previa
# 
# Confirma que `bronze_ventas` existe y tiene filas antes de continuar (viene de la Sesion 3).

# CELL ********************

df_check = spark.read.table("lh_bronze.bronze_ventas")
print("Filas en bronze_ventas:", df_check.count())
df_check.printSchema()

# MARKDOWN ********************

# ---
# ## Parte 1 - El problema: append ciego duplica (15 min)
# 
# **Guion:** escribimos la transformacion Bronze->Silver con `.mode("append")` -- la forma "obvia" de escribir que casi todos usan al empezar. La corremos dos veces y contamos filas.

# MARKDOWN ********************

# ### Paso 1 - Leer Bronze y transformar

# CELL ********************

from pyspark.sql import functions as F

df = spark.read.table("lh_bronze.bronze_ventas")
df_silver = (df
    .withColumn("monto", F.col("monto").cast("double"))
    .withColumn("fecha_venta", F.to_date("fecha_venta"))
)
df_silver.printSchema()

# MARKDOWN ********************

# ### Paso 2 - Escribir con append (a proposito, sin control)

# CELL ********************

(df_silver
   .write.format("delta")
   .mode("append")
   .saveAsTable("silver_ventas_stage"))

# MARKDOWN ********************

# ### Paso 3 - Contar filas

# CELL ********************

spark.sql("SELECT COUNT(*) AS filas FROM silver_ventas_stage").show()

# MARKDOWN ********************

# ### Paso 4 - Volver a correr la celda de escritura
# 
# Sube dos celdas y vuelve a ejecutar el `write.mode("append")` del Paso 2. Luego repite el conteo del Paso 3 en la celda de abajo.

# CELL ********************

# Repite el conteo despues de correr el Paso 2 una segunda vez
spark.sql("SELECT COUNT(*) AS filas FROM silver_ventas_stage").show()

# Ojo -- esto es a proposito: el conteo se duplico.
# append ciego = duplicados garantizados cada vez que el notebook se re-ejecuta.

# MARKDOWN ********************

# > **Checkpoint para el aula:** *"Si este pipeline corriera automaticamente cada noche y fallara a la mitad, Fabric lo reintentaria. ¿Que le pasaria a esta tabla?"*

# MARKDOWN ********************

# ---
# ## Parte 2 - La correccion: overwrite idempotente con replaceWhere (15 min)
# 
# **Guion:** vaciamos la tabla de prueba, activamos `partitionOverwriteMode=dynamic` y reescribimos usando `replaceWhere` acotado a la fecha procesada.

# MARKDOWN ********************

# ### Paso 1 - Vaciar la tabla de prueba

# CELL ********************

spark.sql("DROP TABLE IF EXISTS silver_ventas_stage")

# MARKDOWN ********************

# ### Paso 2 - Reescribir con overwrite dinamico por particion

# CELL ********************

spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

fecha_proceso = "2026-07-29"   # en la Parte 4 esto se vuelve un parametro

df_dia = df_silver.filter(F.col("fecha_venta") == fecha_proceso)

(df_dia
   .write.format("delta")
   .mode("overwrite")
   .option("replaceWhere", f"fecha_venta = '{fecha_proceso}'")
   .saveAsTable("silver_ventas_stage"))

spark.sql("SELECT COUNT(*) AS filas FROM silver_ventas_stage").show()

# MARKDOWN ********************

# ### Paso 3 - Correr dos veces seguidas y comparar
# 
# Ejecuta la celda del Paso 2 una segunda vez sin cambiar nada. El conteo debe quedar **identico** — esa comparacion (duplicados en Parte 1, mismo numero aqui) es la leccion central de la sesion.

# MARKDOWN ********************

# ---
# ## Parte 3 - La misma logica en Spark SQL (10 min)
# 
# **Guion:** reproducimos la transformacion como SQL puro con `INSERT OVERWRITE` acotado a la misma particion, y comparamos el conteo contra la version PySpark.

# CELL ********************

# MAGIC %%sql
# MAGIC INSERT OVERWRITE TABLE silver_ventas_stage
# MAGIC SELECT
# MAGIC   id_venta, id_tienda, id_cliente,
# MAGIC   CAST(monto AS DOUBLE) AS monto,
# MAGIC   CAST(fecha_venta AS DATE) AS fecha_venta
# MAGIC FROM lh_bronze.bronze_ventas
# MAGIC WHERE fecha_venta = '2026-07-29';

# CELL ********************

# Comparar: debe coincidir exactamente con el conteo de la Parte 2
spark.sql("SELECT COUNT(*) AS filas FROM silver_ventas_stage").show()

# MARKDOWN ********************

# **Tip:** si tu equipo tiene analistas que solo conocen SQL, esta version es la que ellos pueden auditar. No hace falta elegir una sola sintaxis para siempre.

# MARKDOWN ********************

# ---
# ## Parte 4 - Parametrizar el notebook para backfills (15 min)
# 
# **Guion:** convertimos la fecha fija en un rango parametrizado, para que el mismo notebook sirva de corrida diaria y de backfill sin ramas especiales.

# MARKDOWN ********************

# ### Paso 1 - Celda de parametros
# 
# En Fabric: boton **⋮** de esta celda -> **Toggle parameter cell**. Un pipeline podra sobreescribir estas variables al invocar el notebook (lo veremos en la Sesion 10).

# CELL ********************

# Celda marcada como "parameters"
fecha_inicio = "2026-07-25"
fecha_fin    = "2026-07-29"

# MARKDOWN ********************

# ### Paso 2 - Reescribir la transformacion usando el rango

# CELL ********************

df_rango = df_silver.filter(
    F.col("fecha_venta").between(fecha_inicio, fecha_fin)
)

(df_rango
   .write.format("delta")
   .mode("overwrite")
   .option("replaceWhere",
           f"fecha_venta >= '{fecha_inicio}' AND fecha_venta <= '{fecha_fin}'")
   .saveAsTable("silver_ventas_stage"))

spark.sql("SELECT COUNT(*) AS filas FROM silver_ventas_stage").show()

# MARKDOWN ********************

# ### Paso 3 - Probar como corrida diaria y como backfill
# 
# Corre la celda de parametros con `fecha_inicio = fecha_fin` (un solo dia), ejecuta el Paso 2 y anota el conteo. Luego cambia a un rango de 5 dias, vuelve a correr el Paso 2 y confirma que re-ejecutar la MISMA celda de nuevo no cambia el conteo final en ningun caso.

# MARKDOWN ********************

# **Nota:** en la Sesion 10 (Orquestacion) este mismo notebook se invoca desde un pipeline que le pasa `fecha_inicio`/`fecha_fin` automaticamente. Hoy lo corremos a mano -- la parametrizacion ya queda lista.

# MARKDOWN ********************

# ---
# ## Checklist de este laboratorio
# 
# - [ ] `silver_ventas_stage` existe en `lh_silver` con datos de RetailPe
# - [ ] Demostrado en vivo: `append` duplico filas, `replaceWhere` no
# - [ ] La version Spark SQL de la Parte 3 coincide con la version PySpark de la Parte 2
# - [ ] El notebook acepta `fecha_inicio`/`fecha_fin` como parametros y funciona para un dia o un rango
# - [ ] Notebook commiteado al repo de Git del workspace
# 
# **Honestidad sobre el alcance:** este notebook NO limpia nulos, no normaliza texto ni deduplica por clave de negocio -- eso llega en las Sesiones 5 y 6. `silver_ventas_stage` es deliberadamente una tabla de practica de idempotencia, no la capa Silver curada final.
# 
# **Commit al repo:** `git commit -m "S4: notebook idempotente Bronze->Silver (replaceWhere + parametros)"`

# MARKDOWN ********************

# ---
# ## Sesion 5 - Continuacion: llegada tardia y reglas de negocio en Silver
# 
# **Data Wizard Academy - Microsoft Fabric Track**
# 
# Este notebook es el MISMO de la Sesion 4 -- no se creo uno nuevo. Todo lo de arriba (Partes 1 a 4, la tabla `silver_ventas_stage` escrita con `replaceWhere` de forma idempotente) sigue vigente y no se toca: el caso RetailPe es un solo proyecto que avanza sesion a sesion, no una coleccion de ejercicios sueltos (ver Caso-de-uso-RetailPe, seccion 7).
# 
# Hoy extendemos esta misma tabla con un problema real de negocio documentado en la seccion 5 del caso ("Reglas de negocio acordadas"):
# 
# - **PARTE A - R7 (llegada tardia):** una venta ocurrida hace 3 dias puede "aparecer" recien hoy en el origen. Sin una ventana de tolerancia, un watermark ingenuo la pierde para siempre.
# 
# **Fuera de alcance hoy** (se ubican, no se implementan): R1 y R2 (datos sucios: monto/fecha nulo, monto no positivo) se formalizan con su propia tabla de cuarentena en la Sesion 6; R3 (categoria de producto) es de la fuente `productos`; R4 (integridad referencial venta-producto-tienda) se valida en Gold; R5 (SCD Tipo 2 de clientes) y R6 (grano de la tabla de hechos) llegan con el modelado de la Sesion 7-8.
# 
# **CDC de clientes (Parte B del temario de hoy):** esa si es una fuente nueva (`clientes`) que no existia en la Sesion 4, asi que vive en su PROPIO notebook: `nb_silver_clientes_cdc.ipynb`, en esta misma carpeta.

# MARKDOWN ********************

# ---
# ## PARTE A - Llegada tardia (R7)
# 
# **Regla de negocio R7:** *"Ventas de dias anteriores pueden llegar hasta 3 dias tarde"* -> tratamiento: *"Ventana de tolerancia; se reprocesa ese dia"*.

# MARKDOWN ********************

# ---
# ## Parte 5 - El problema: una venta que llega tarde (15 min)
# 
# **Guion:** un watermark ingenuo (`WHERE fecha_venta > @ultimo_watermark`) pierde ventas cuya fecha de negocio ya quedo "detras" del watermark cuando llegan tarde al origen.

# MARKDOWN ********************

# ### Paso 1 - Verificar el punto de partida

# CELL ********************

df_check = spark.read.table("silver_ventas_stage")
print("Filas en silver_ventas_stage (viene de la Sesion 4, arriba en este mismo notebook):", df_check.count())
df_check.printSchema()

# MARKDOWN ********************

# ### Paso 2 - Simular la llegada tardia (R7)

# CELL ********************

from pyspark.sql import functions as F
from datetime import date, timedelta

df_bronze = spark.read.table("lh_bronze.bronze_ventas")
hoy = date.today()
fecha_tardia = hoy - timedelta(days=3)

# R7: fila que representa una venta ocurrida hace 3 dias pero que recien
# "aparece" en el sistema origen en esta corrida (falla de red en tienda, etc.)
fila_tardia = spark.createDataFrame([
    ("V-88213", "T-014", "C-4021", 145.50, fecha_tardia)
], ["id_venta", "id_tienda", "id_cliente", "monto", "fecha_venta"])

df_bronze_simulado = df_bronze.unionByName(fila_tardia)
print("Filas totales con la venta tardia simulada (R7):", df_bronze_simulado.count())

# MARKDOWN ********************

# ### Paso 3 - Ver por que un watermark ingenuo la perderia (R7)

# CELL ********************

# Simulamos que el watermark de wh_control ya avanzo hasta "ayer"
ultimo_watermark = hoy - timedelta(days=1)

perdidas = df_bronze_simulado.filter(F.col("fecha_venta") <= ultimo_watermark)
print("Filas que un pipeline 'WHERE fecha_venta > watermark' NUNCA volveria a leer:", perdidas.count())
perdidas.select("id_venta", "fecha_venta").show()

# Ojo -- esto es a proposito: la venta tardia queda del lado "ya leido" del watermark.
# Sin la ventana de tolerancia de R7, se pierde para siempre.

# MARKDOWN ********************

# > **Checkpoint para el aula:** *"Que pasaria si esta fila llega 5 dias tarde en vez de 3? Que dice R7 al respecto?"*

# MARKDOWN ********************

# ---
# ## Parte 6 - La correccion: ventana de tolerancia de R7 (20 min)
# 
# **Guion:** en vez de leer y reescribir solo "hoy", ampliamos la ventana de lectura y de replaceWhere en Silver a los ultimos N dias (R7 = 3), confiando en la idempotencia de la Sesion 4 para que no duplique.

# MARKDOWN ********************

# ### Paso 1 - Definir la ventana segun R7

# CELL ********************

# R7: ventana de tolerancia de 3 dias
DIAS_TOLERANCIA_R7 = 3

fecha_fin = hoy
fecha_inicio = ultimo_watermark - timedelta(days=DIAS_TOLERANCIA_R7)

print("Ventana a reprocesar (R7):", fecha_inicio, "->", fecha_fin)

# MARKDOWN ********************

# ### Paso 2 - Transformar y filtrar por la ventana completa

# CELL ********************

df_silver = (df_bronze_simulado
    .withColumn("monto", F.col("monto").cast("double"))
    .withColumn("fecha_venta", F.to_date("fecha_venta"))
)

# R7: filtramos por TODA la ventana de tolerancia, no solo por "hoy"
df_ventana = df_silver.filter(
    F.col("fecha_venta").between(fecha_inicio, fecha_fin)
)
print("Filas dentro de la ventana de tolerancia (R7):", df_ventana.count())

# MARKDOWN ********************

# ### Paso 3 - Reescribir la ventana completa con replaceWhere (R7)

# CELL ********************

spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

# R7: replaceWhere acotado al RANGO de la ventana de tolerancia, no a un solo dia
(df_ventana
   .write.format("delta")
   .mode("overwrite")
   .option("replaceWhere",
           f"fecha_venta >= '{fecha_inicio}' AND fecha_venta <= '{fecha_fin}'")
   .saveAsTable("silver_ventas_stage"))

spark.sql("SELECT COUNT(*) AS filas FROM silver_ventas_stage").show()

# MARKDOWN ********************

# ### Paso 4 - Verificar que la venta tardia quedo reflejada

# CELL ********************

spark.sql("""
  SELECT * FROM silver_ventas_stage WHERE id_venta = 'V-88213'
""").show()

# MARKDOWN ********************

# ### Paso 5 - Correr dos veces y confirmar que no duplica

# CELL ********************

# Vuelve a ejecutar la celda de escritura del Paso 3 (sin cambiar nada) y
# despues corre esta celda de nuevo. El conteo debe quedar identico.
spark.sql("SELECT COUNT(*) AS filas FROM silver_ventas_stage").show()

# MARKDOWN ********************

# **Nota:** esto NO es "detectar" magicamente que algo llego tarde. Es aceptar reprocesar una ventana fija de N dias (R7 = 3) en cada corrida y confiar en que `replaceWhere` (Sesion 4) no duplique lo que no cambio.

# MARKDOWN ********************

# ---
# ### Fin de la Parte A - llegada tardia (R7)
# 
# Con esto, `silver_ventas_stage` queda correcta frente a ventas que llegan hasta 3 dias tarde, sin duplicar nada al reprocesar. R1 y R2 (datos sucios) NO se tocan hoy -- se formalizan de cero en la Sesion 6, con su propia tabla de cuarentena.

# MARKDOWN ********************

# ---
# ## Checklist de la Sesion 5 (Parte A)
# 
# - [ ] `silver_ventas_stage` refleja la venta tardia simulada (`id_venta = 'V-88213'`) sin duplicar el resto (R7)
# - [ ] Re-ejecutar la escritura de la ventana no cambia el conteo total de `silver_ventas_stage`
# - [ ] Notebook commiteado al repo de Git del workspace
# 
# **Commit al repo:** `git commit -m "S5 Parte A: ventana de tolerancia R7 sobre silver_ventas_stage"`

# MARKDOWN ********************

# ---
# ## Sesion 6 - Continuacion: limpieza con cuarentena
# 
# **Data Wizard Academy - Microsoft Fabric Track**
# 
# Este notebook sigue siendo el MISMO de las Sesiones 4 y 5 -- no se crea uno nuevo. RetailPe es un solo
# proyecto que avanza sesion a sesion (Caso-de-uso-RetailPe, seccion 7): las Partes 1 a 6 de arriba (idempotencia,
# ventana de tolerancia R7) siguen intactas y no se tocan.
# 
# Hoy construimos DE CERO el framework formal de cuarentena para `silver_ventas_stage`. Las Reglas de negocio
# R1 y R2 (documentadas en la seccion 5 del caso) dicen que una venta sin monto/fecha, o con monto no positivo,
# no debe entrar a Silver sin control -- debe ir a una tabla de cuarentena dedicada, auditable y reprocesable.

# MARKDOWN ********************

# ---
# ## Parte 7 - Formalizando la tabla de cuarentena (15 min)
# 
# **Regla de negocio R1:** *"Una venta sin monto o sin fecha es invalida"* -> tratamiento: *"Va a cuarentena con
# el motivo del rechazo"*.
# 
# **Regla de negocio R2:** *"El monto de venta debe ser mayor a cero"* -> tratamiento: *"Va a cuarentena; se
# revisa con Comercial"*.
# 
# Hoy formalizamos el destino de R1 y R2 en **`silver_ventas_cuarentena`**, una tabla dedicada, auditable y
# reprocesable (no una tabla de practica), con columnas:
# 
# - Todas las columnas originales de la venta (`id_venta`, `id_tienda`, `id_cliente`, `monto`, `fecha_venta`).
# - `motivo_rechazo_codigo` -- valor **categorico**, no texto libre: `R1_FECHA_O_MONTO_NULO` o `R2_MONTO_NO_POSITIVO`.
# - `motivo_rechazo_detalle` -- texto humano, para que Comercial entienda el motivo sin tener que conocer el codigo.
# - `fecha_deteccion` -- timestamp de CUANDO el pipeline detecto el rechazo (no la fecha de la venta).
# - `id_ejecucion` -- identificador de la corrida que la metio a cuarentena, para trazabilidad de quien/cuando.

# MARKDOWN ********************

# ### Paso 1 - Generar un identificador de ejecucion

# CELL ********************

import uuid

# Identificador simple de esta corrida -- suficiente para trazabilidad (quien/cuando metio la fila a cuarentena).
# Un MERGE o un orquestador real (Sesion 7 en adelante) lo generaria automaticamente por corrida.
id_ejecucion = str(uuid.uuid4())
print("id_ejecucion de esta corrida:", id_ejecucion)

# MARKDOWN ********************

# ### Paso 2 - Simular un backfill con datos sucios
# 
# Simulamos un nuevo lote de `bronze_ventas` que trae, junto con ventas validas, dos filas invalidas: una que
# viola R1 (monto nulo) y otra que viola R2 (monto negativo) -- el mismo tipo de dato sucio que RetailPe
# documenta en la seccion 4 del caso. Como en la Sesion 5 (Parte 6, R7), casteamos los tipos explicitamente
# (`monto` a `double`, `fecha_venta` a `date`) ANTES de unir con `unionByName` -- el mismo cuidado de esquema
# que evita el bug silencioso de mezclar columnas por posicion.

# CELL ********************

df_bronze_nuevo_lote = spark.createDataFrame([
    ("V-90101", "T-021", "C-6002", 210.00, hoy),
    ("V-90102", "T-021", "C-6003", None,   hoy),    # R1: monto nulo
    ("V-90103", "T-021", "C-6004", -15.00, hoy),    # R2: monto <= 0
], ["id_venta", "id_tienda", "id_cliente", "monto", "fecha_venta"])

df_bronze_nuevo_lote = (df_bronze_nuevo_lote
    .withColumn("monto", F.col("monto").cast("double"))
    .withColumn("fecha_venta", F.to_date("fecha_venta"))
)
print("Filas en el nuevo lote simulado:", df_bronze_nuevo_lote.count())

# MARKDOWN ********************

# ### Paso 3 - Antipatron vs. patron correcto: evaluar R1 y R2 por separado
# 
# **Antipatron (lo que NO hay que hacer):** un solo `filter` con un OR que junta todas las condiciones de
# rechazo. Funciona para separar validas de invalidas, pero **no dice POR QUE** se rechazo cada fila -- hay que
# volver a mirar los valores a mano para saber si fue R1 o R2.

# CELL ********************

# ANTIPATRON -- un solo filtro opaco, no distingue el motivo
condicion_invalida_opaca = (
    F.col("monto").isNull() | F.col("fecha_venta").isNull() | (F.col("monto") <= 0)
)
df_cuarentena_opaca = df_bronze_nuevo_lote.filter(condicion_invalida_opaca)
print("Filas rechazadas (antipatron, SIN saber por que cada una):", df_cuarentena_opaca.count())

# MARKDOWN ********************

# **Patron correcto:** cada regla se evalua por separado, en el orden correcto (R1 antes que R2, porque
# `monto <= 0` sobre un `None` devuelve `NULL`, no `True`), y solo despues se etiqueta y se une todo en el
# DataFrame de cuarentena.

# CELL ********************

# PATRON CORRECTO -- cada regla evaluada y etiquetada por separado
viola_r1 = F.col("monto").isNull() | F.col("fecha_venta").isNull()
viola_r2 = (~viola_r1) & (F.col("monto") <= 0)   # ya sabemos que monto no es nulo aqui

df_etiquetado = (df_bronze_nuevo_lote
    .withColumn("motivo_rechazo_codigo",
        F.when(viola_r1, "R1_FECHA_O_MONTO_NULO")
         .when(viola_r2, "R2_MONTO_NO_POSITIVO")
         .otherwise(None))
    .withColumn("motivo_rechazo_detalle",
        F.when(viola_r1, "La venta no tiene monto o no tiene fecha de venta registrada")
         .when(viola_r2, "El monto de venta es menor o igual a cero")
         .otherwise(None))
)

df_validas_nuevo_lote = (df_etiquetado
    .filter(F.col("motivo_rechazo_codigo").isNull())
    .drop("motivo_rechazo_codigo", "motivo_rechazo_detalle"))

df_cuarentena_nueva = (df_etiquetado
    .filter(F.col("motivo_rechazo_codigo").isNotNull())
    .withColumn("fecha_deteccion", F.current_timestamp())
    .withColumn("id_ejecucion", F.lit(id_ejecucion)))

print("Validas:", df_validas_nuevo_lote.count(), "| Cuarentena (con motivo categorico):", df_cuarentena_nueva.count())
df_cuarentena_nueva.select("id_venta", "monto", "motivo_rechazo_codigo", "motivo_rechazo_detalle", "id_ejecucion").show(truncate=False)

# MARKDOWN ********************

# ### Paso 4 - Escribir la tabla de cuarentena formal y reescribir lo valido en Silver

# CELL ********************

# silver_ventas_cuarentena: tabla FORMAL, construida de cero a partir del lote sucio simulado de hoy
# (no hay migracion de ninguna tabla de practica -- esta es la primera escritura de esta tabla).
(df_cuarentena_nueva
   .write.format("delta")
   .mode("overwrite")
   .saveAsTable("silver_ventas_cuarentena"))

# Las filas validas del nuevo lote se agregan a silver_ventas_stage (misma mecanica idempotente de replaceWhere,
# acotada al dia de hoy porque este lote no es un backfill de R7, es una corrida normal del dia).
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
(df_validas_nuevo_lote
   .write.format("delta")
   .mode("overwrite")
   .option("replaceWhere", f"fecha_venta = '{hoy}'")
   .saveAsTable("silver_ventas_stage"))

spark.sql("SELECT COUNT(*) AS filas FROM silver_ventas_cuarentena").show()
spark.sql("SELECT COUNT(*) AS filas FROM silver_ventas_stage").show()

# MARKDOWN ********************

# ---
# ## Parte 8 - Reporte para Comercial: la cuarentena como bandeja de trabajo (10 min)
# 
# **Guion:** R2 dice explicitamente *"se revisa con Comercial"* -- eso solo es posible si existe un reporte
# que Comercial pueda leer sin tener que abrir un notebook. `motivo_rechazo_codigo`, al ser categorico, permite
# un `groupBy` directo. La cuarentena no es un basurero: es una bandeja de trabajo con reporte.

# MARKDOWN ********************

# ### Paso 1 - Reporte agregado por motivo de rechazo

# CELL ********************

reporte_comercial = (spark.read.table("silver_ventas_cuarentena")
    .groupBy("motivo_rechazo_codigo")
    .agg(F.count("*").alias("cantidad_ventas"), F.max("fecha_deteccion").alias("ultima_deteccion")))

reporte_comercial.orderBy(F.desc("cantidad_ventas")).show(truncate=False)

# Version SQL equivalente, para analistas que solo conocen SQL (mismo espiritu que la Parte 3 de la Sesion 4)
spark.sql("""
  SELECT motivo_rechazo_codigo, COUNT(*) AS cantidad_ventas, MAX(fecha_deteccion) AS ultima_deteccion
  FROM silver_ventas_cuarentena
  GROUP BY motivo_rechazo_codigo
  ORDER BY cantidad_ventas DESC
""").show(truncate=False)

# MARKDOWN ********************

# ---
# ## Parte 9 - Demo de graduacion: una fila corregida sale de cuarentena (15 min)
# 
# **Guion:** una venta que estaba en cuarentena por R1 (monto nulo) llega CORREGIDA en una nueva ejecucion de
# Bronze -- el origen finalmente completo el dato. El pipeline debe: (a) insertar la fila corregida en
# `silver_ventas_stage`, y (b) quitar la fila vieja de `silver_ventas_cuarentena` para que no quede duplicada
# ni fantasma.
# 
# **Honestidad sobre el alcance:** un `MERGE` completo y automatizado (que resuelva esto en una sola sentencia
# declarativa) es tema de la Sesion 7. Hoy lo hacemos con logica explicita, paso a paso -- leer cuarentena,
# filtrar la fila ya resuelta, sobreescribir la tabla de cuarentena sin ella.

# MARKDOWN ********************

# ### Paso 1 - Simular la llegada de la fila corregida

# CELL ********************

# V-90102 estaba en cuarentena por R1 (monto nulo). Llega corregida: el origen ya completo el monto.
fila_corregida = spark.createDataFrame([
    ("V-90102", "T-021", "C-6003", 76.50, hoy),
], ["id_venta", "id_tienda", "id_cliente", "monto", "fecha_venta"])

# Mismo cuidado de tipos que en toda la Sesion 5: castear explicitamente antes de unionByName
fila_corregida = (fila_corregida
    .withColumn("monto", F.col("monto").cast("double"))
    .withColumn("fecha_venta", F.to_date("fecha_venta")))

fila_corregida.show()

# MARKDOWN ********************

# ### Paso 2 - Insertar la fila corregida en silver_ventas_stage

# CELL ********************

df_ventas_stage_actual = spark.read.table("silver_ventas_stage")
df_ventas_stage_con_graduada = df_ventas_stage_actual.unionByName(fila_corregida)

spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
(df_ventas_stage_con_graduada
   .write.format("delta")
   .mode("overwrite")
   .option("replaceWhere", f"fecha_venta = '{hoy}'")
   .saveAsTable("silver_ventas_stage"))

spark.sql("SELECT * FROM silver_ventas_stage WHERE id_venta = 'V-90102'").show()

# MARKDOWN ********************

# ### Paso 3 - Quitar la fila graduada de la cuarentena

# CELL ********************

df_cuarentena_actual = spark.read.table("silver_ventas_cuarentena")
filas_antes = df_cuarentena_actual.count()

# Filtramos explicitamente la fila ya resuelta -- esto NO es un MERGE, es logica manual paso a paso.
# La Sesion 7 automatiza esto con una sentencia MERGE declarativa.
df_cuarentena_sin_graduada = df_cuarentena_actual.filter(F.col("id_venta") != "V-90102")

(df_cuarentena_sin_graduada
   .write.format("delta")
   .mode("overwrite")
   .saveAsTable("silver_ventas_cuarentena"))

filas_despues = spark.read.table("silver_ventas_cuarentena").count()
print("Filas en cuarentena antes:", filas_antes, "| despues de graduar V-90102:", filas_despues)
spark.sql("SELECT * FROM silver_ventas_cuarentena WHERE id_venta = 'V-90102'").show()
# Debe devolver 0 filas: V-90102 ya no esta en cuarentena, esta en silver_ventas_stage.

# MARKDOWN ********************

# > **Checkpoint para el aula:** *"Que pasaria si, en vez de filtrar por id_venta, hubieramos sobreescrito
# `silver_ventas_cuarentena` completa con otro DataFrame distinto del Paso 4 de la Parte 7, olvidando las filas
# que ya estaban ahi? Que perderiamos, y por que el orden de las escrituras en un notebook con estado importa tanto
# como el orden de las reglas de negocio?"*

# MARKDOWN ********************

# ---
# ## Honestidad sobre el alcance de la Sesion 6
# 
# **R3 (categoria de producto):** conceptualmente es el MISMO patron de limpieza de hoy (etiquetar, separar,
# resolver) pero aplicado sobre `bronze_productos` -- una fuente que, segun `control_ingesta_retailpe.xlsx`,
# sigue **INACTIVA** (`activo = NO`) en el pipeline real. Por eso R3 se menciona hoy solo como preview conceptual
# en las slides, y NO se implementa en este notebook contra una tabla que todavia no existe.
# 
# **Alertas automaticas cuando la cuarentena crece demasiado:** eso es un framework de observabilidad completo
# -- tema de las Sesiones 10 a 13 (Produccion / Observabilidad). Hoy la revision de la cuarentena es MANUAL: el
# reporte de la Parte 8 es lo que un analista de Comercial revisaria a mano, no algo que dispare una alerta
# por si solo.
# 
# **Lo que SI queda formalizado hoy:** `silver_ventas_cuarentena` con columnas categoricas (`motivo_rechazo_codigo`),
# trazabilidad (`fecha_deteccion`, `id_ejecucion`), un reporte agregado para Comercial, y un proceso explicito de
# graduacion (aunque manual, no vía MERGE) para que una fila corregida en el origen deje de estar fantasma en
# dos tablas a la vez.

# MARKDOWN ********************

# ---
# ## Checklist de la Sesion 6
# 
# - [ ] `silver_ventas_cuarentena` existe con columnas `motivo_rechazo_codigo`, `motivo_rechazo_detalle`,
#       `fecha_deteccion`, `id_ejecucion`
# - [ ] `motivo_rechazo_codigo` toma solo valores `R1_FECHA_O_MONTO_NULO` o `R2_MONTO_NO_POSITIVO` (categorico,
#       no texto libre)
# - [ ] El reporte `groupBy("motivo_rechazo_codigo").count()` corre y muestra cantidades por motivo
# - [ ] La demo de graduacion deja a `V-90102` en `silver_ventas_stage` y YA NO en `silver_ventas_cuarentena`
# - [ ] Puedes explicar, sin notas, por que evaluar R1 antes que R2 evita el bug de `NULL <= 0`
# - [ ] Notebook commiteado al repo de Git del workspace
# 
# **Commit al repo:** `git commit -m "S6: framework formal de cuarentena (motivo categorico, trazabilidad, reporte y graduacion) sobre silver_ventas_stage"`

# MARKDOWN ********************

# ---

# MARKDOWN ********************

# ## Sesion 7 - Continuacion: MERGE y SCD Tipo 2
# 
# **Data Wizard Academy - Microsoft Fabric Track**
# 
# Este notebook sigue siendo el MISMO de las Sesiones 4, 5 y 6 -- no se crea uno nuevo. Las Partes 1 a 11
# de arriba (idempotencia, ventana de tolerancia R7, cuarentena formal con `silver_ventas_cuarentena`)
# siguen intactas y no se tocan.
# 
# Hoy cerramos el bloque "2 - Nucleo de Ingenieria" (Sesiones 4 a 7). Hasta ahora, `silver_ventas_stage`
# se mantuvo con logica manual paso a paso: leer todo, filtrar, castear, `replaceWhere`/`overwrite`,
# `unionByName`, un filtro explicito para graduar filas de cuarentena. Hoy reemplazamos TODO eso por una
# sola sentencia `MERGE INTO` declarativa, y retiramos el sufijo `_stage`: la tabla pasa a llamarse
# **`silver_ventas`**, porque hoy se vuelve el patron de escritura real de produccion, ya no una practica.
# 
# La Parte B de la Sesion 7 (SCD Tipo 2 sobre clientes, regla **R5**) vive en el otro notebook canonico,
# `nb_silver_clientes_cdc.ipynb`, que a su vez se extiende hoy de la misma forma.

# MARKDOWN ********************

# ---
# ## PARTE A - MERGE declarativo sobre ventas (consolida S4 + S5 + S6)
# 
# Todo lo aprendido en las tres sesiones anteriores converge hoy en un solo patron de produccion:
# 
# - **Sesion 4 (idempotencia):** reescribir con `replaceWhere` para que correr el pipeline dos veces no duplique filas.
# - **Sesion 5 (R7, llegada tardia):** filtrar el origen a una ventana de tolerancia de 3 dias antes de reescribir.
# - **Sesion 6 (cuarentena):** etiquetar R1/R2 por separado, escribir `silver_ventas_cuarentena`, y graduar (sacar de
#   cuarentena) una fila que llega corregida insertandola a mano en Silver.
# 
# Un `MERGE INTO` declarativo colapsa la parte de INSERT/UPDATE de ese trabajo en una sola sentencia: en un solo
# statement, inserta ventas nuevas (`WHEN NOT MATCHED THEN INSERT`) y actualiza ventas que llegaron corregidas
# (`WHEN MATCHED THEN UPDATE`, la graduacion automatica que en la Sesion 6 hacíamos con un filtro manual). La
# clave de match es `id_venta`. La ventana de tolerancia de R7 sigue aplicando IGUAL que antes: se filtra el
# DataFrame de origen a esa ventana ANTES del MERGE, no dentro de el. La cuarentena (`silver_ventas_cuarentena`)
# se sigue poblando con la MISMA logica de etiquetado R1/R2 de la Sesion 6 -- eso no cambia hoy, solo cambia
# el paso de escritura a Silver.

# MARKDOWN ********************

# ### Paso 1 - Retirar el sufijo `_stage`: `silver_ventas_stage` -> `silver_ventas`
# 
# Desde la Sesion 4, la tabla se llamo `silver_ventas_stage` a proposito, como senal de que todavia era
# una version de practica. Hoy el MERGE declarativo es el patron real de produccion, asi que renombramos
# la tabla UNA sola vez -- todo el codigo de aqui en adelante (y de la capa Gold, Sesiones 8-9) usa
# `silver_ventas`, sin sufijo.

# CELL ********************

spark.sql("ALTER TABLE silver_ventas_stage RENAME TO silver_ventas")
spark.sql("SELECT COUNT(*) AS filas FROM silver_ventas").show()

# MARKDOWN ********************

# ### Paso 2 - Simular el lote de hoy: una venta nueva y una venta corregida
# 
# Simulamos un lote de `bronze_ventas` con dos casos que un `MERGE` real tiene que distinguir: una venta
# **nueva** (`id_venta` que no existe todavia en `silver_ventas`) y una venta que **ya existe pero llego
# corregida** (mismo `id_venta` que `V-90101`, insertada en la Parte 7 de la Sesion 6, ahora con el monto
# correcto). Sin `MERGE`, distinguir estos dos casos exigia el `unionByName` + filtro manual de la Sesion 6;
# con `MERGE`, la clave `id_venta` lo resuelve solo.

# CELL ********************

df_bronze_lote_merge = spark.createDataFrame([
    ("V-90101", "T-021", "C-6002", 235.75, hoy),   # ya existe (Sesion 6, Parte 7) -- llega CORREGIDA
    ("V-90201", "T-014", "C-4021", 89.90,  hoy),   # id_venta nuevo -- INSERT
], ["id_venta", "id_tienda", "id_cliente", "monto", "fecha_venta"])

df_bronze_lote_merge = (df_bronze_lote_merge
    .withColumn("monto", F.col("monto").cast("double"))
    .withColumn("fecha_venta", F.to_date("fecha_venta")))
df_bronze_lote_merge.show()

# MARKDOWN ********************

# ### Paso 3 - R1/R2 siguen igual: etiquetar antes de tocar Silver
# 
# El MERGE de hoy reemplaza el paso de ESCRITURA a Silver, no el de etiquetado -- eso sigue siendo
# exactamente el patron de la Sesion 6: evaluar R1 antes que R2 (para no toparse con el bug de
# `NULL <= 0`), etiquetar, y separar validas de sucias. Las filas sucias se suman a la cuarentena existente
# en vez de sobreescribirla -- perder lo que ya estaba en cuarentena seria repetir exactamente el error que
# el checkpoint de la Sesion 6 (celda 77) advertia.

# CELL ********************

viola_r1 = F.col("monto").isNull() | F.col("fecha_venta").isNull()
viola_r2 = (~viola_r1) & (F.col("monto") <= 0)

df_etiquetado_merge = (df_bronze_lote_merge
    .withColumn("motivo_rechazo_codigo",
        F.when(viola_r1, "R1_FECHA_O_MONTO_NULO")
         .when(viola_r2, "R2_MONTO_NO_POSITIVO")
         .otherwise(None))
    .withColumn("motivo_rechazo_detalle",
        F.when(viola_r1, "La venta no tiene monto o no tiene fecha de venta registrada")
         .when(viola_r2, "El monto de venta es menor o igual a cero")
         .otherwise(None)))

df_validas_lote_merge = (df_etiquetado_merge
    .filter(F.col("motivo_rechazo_codigo").isNull())
    .drop("motivo_rechazo_codigo", "motivo_rechazo_detalle"))

df_cuarentena_lote_merge = (df_etiquetado_merge
    .filter(F.col("motivo_rechazo_codigo").isNotNull())
    .withColumn("fecha_deteccion", F.current_timestamp())
    .withColumn("id_ejecucion", F.lit(str(uuid.uuid4()))))

# Cuarentena: se SUMA a lo que ya existia, nunca se sobreescribe con solo lo nuevo (leccion de la Sesion 6)
df_cuarentena_actualizada = (spark.read.table("silver_ventas_cuarentena")
    .unionByName(df_cuarentena_lote_merge))
(df_cuarentena_actualizada.write.format("delta").mode("overwrite")
   .saveAsTable("silver_ventas_cuarentena"))
print("Validas para el MERGE:", df_validas_lote_merge.count(), "| Nuevas en cuarentena:", df_cuarentena_lote_merge.count())

# MARKDOWN ********************

# ### Paso 4 - R7 tambien aplica: filtrar a la ventana de tolerancia ANTES del MERGE
# 
# El mismo patron de R7 de la Sesion 5 sigue vigente: un `MERGE` no reemplaza la necesidad de acotar QUE
# filas del origen se consideran en esta corrida. Se filtra `df_validas_lote_merge` a la ventana
# `[fecha_inicio, fecha_fin]` (las mismas variables `DIAS_TOLERANCIA_R7`, `fecha_inicio`, `fecha_fin`
# definidas en la Sesion 5, Parte 6) ANTES de pasarlo al MERGE -- exactamente igual que antes se filtraba
# antes de escribir con `replaceWhere`.

# CELL ********************

df_validas_lote_merge = df_validas_lote_merge.filter(
    (F.col("fecha_venta") >= F.lit(fecha_inicio)) & (F.col("fecha_venta") <= F.lit(fecha_fin))
)
print("Filas dentro de la ventana de tolerancia R7 (listas para el MERGE):", df_validas_lote_merge.count())
df_validas_lote_merge.createOrReplaceTempView("origen_ventas_merge")

# MARKDOWN ********************

# ### Paso 5 - El MERGE declarativo: INSERT + UPDATE en una sola sentencia
# 
# Esto es EXACTAMENTE lo que en la Sesion 6 se hizo a mano con el filtro `id_venta != 'V-90102'` (graduacion)
# y con `unionByName` (insertar la corregida), y en la Sesion 4/5 se hizo con `replaceWhere`. Hoy se colapsa
# en una sola sentencia Spark SQL: `WHEN MATCHED` cubre la fila que ya existia y llego corregida (graduacion
# automatica, sin anti-join manual); `WHEN NOT MATCHED` cubre la fila nueva.

# CELL ********************

spark.sql("""
MERGE INTO silver_ventas AS destino
USING origen_ventas_merge AS origen
ON destino.id_venta = origen.id_venta
WHEN MATCHED THEN
  UPDATE SET
    destino.monto = origen.monto,
    destino.fecha_venta = origen.fecha_venta,
    destino.id_tienda = origen.id_tienda,
    destino.id_cliente = origen.id_cliente
WHEN NOT MATCHED THEN
  INSERT (id_venta, id_tienda, id_cliente, monto, fecha_venta)
  VALUES (origen.id_venta, origen.id_tienda, origen.id_cliente, origen.monto, origen.fecha_venta)
""")

# MARKDOWN ********************

# ### Paso 6 - Verificar: la fila corregida se actualizo, la fila nueva se inserto

# CELL ********************

spark.sql("""
  SELECT * FROM silver_ventas WHERE id_venta IN ('V-90101', 'V-90201')
""").show()
# V-90101 debe mostrar monto = 235.75 (UPDATE, no una fila duplicada)
# V-90201 debe existir como fila nueva (INSERT)
spark.sql("SELECT COUNT(*) AS filas_silver_ventas FROM silver_ventas").show()

# MARKDOWN ********************

# > **Honestidad sobre el alcance:** este `MERGE` no cubre todos los casos de un pipeline de produccion
# > completo. No hay `WHEN NOT MATCHED BY SOURCE` (borrar en Silver una fila que desaparecio del origen --
# > no es un caso de negocio de RetailPe para ventas). El `UPDATE SET` de hoy reemplaza todas las columnas de
# > negocio de la fila (no hay reglas de "solo actualizar si cambio algo"); eso SI se vuelve relevante en la
# > Parte B de hoy, con SCD Tipo 2 sobre clientes.

# MARKDOWN ********************

# ---
# ## Checklist de la Sesion 7 (Parte A)
# 
# - [ ] `silver_ventas_stage` ya no existe -- fue renombrada a `silver_ventas`
# - [ ] El `MERGE INTO silver_ventas` corre sin error y deja `V-90101` con `monto = 235.75` (UPDATE, sin duplicar)
# - [ ] `V-90201` existe en `silver_ventas` como fila nueva (INSERT)
# - [ ] `silver_ventas_cuarentena` sigue poblandose con la misma logica R1/R2 de la Sesion 6, ahora SUMADA a lo que ya existia
# - [ ] Puedes explicar por que la ventana de R7 se filtra ANTES del MERGE, no dentro de el
# - [ ] Puedes explicar que reemplaza el MERGE de hoy de cada sesion anterior (S4 replaceWhere, S5 ventana R7, S6 graduacion manual)
# - [ ] Notebook commiteado al repo de Git del workspace
# 
# **Continua en:** `nb_silver_clientes_cdc.ipynb` -- Parte B de la Sesion 7, SCD Tipo 2 sobre `silver_clientes` (regla R5).
# 
# **Commit al repo:** `git commit -m "S7 Parte A: MERGE declarativo sobre silver_ventas -- retira sufijo _stage, colapsa insercion/graduacion de S4-S6"`
