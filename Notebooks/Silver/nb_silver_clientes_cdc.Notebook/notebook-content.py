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

# # Sesion 5 - Dimension clientes: punto de partida CDC
# 
# **Data Wizard Academy - Microsoft Fabric Track**
# 
# Este notebook es el punto de partida de la dimension clientes. Continua en la Sesion 7 con MERGE y SCD Tipo 2 -- hoy solo resolvemos el estado actual correcto a partir de un feed CDC desordenado, sin guardar historial.
# 
# En produccion, `clientes` llega como "Azure SQL + feed CDC" (Caso-de-uso-RetailPe, seccion 4) con el problema documentado: *"CDC desordenado, deletes tardios"*. No conectamos un CDC real de SQL Server ni Debezium hoy -- demasiado para una sesion de 3 horas. Simulamos ese feed directamente con `spark.createDataFrame(...)`. El foco de hoy es la LOGICA de resolver el estado vigente, no la plomeria de captura.
# 
# Adjunta el Lakehouse `lh_silver` como default antes de ejecutar (icono Lakehouses en el panel izquierdo -> Add).
# 
# Tabla destino: **silver_clientes_stage** -- version de practica, distinta de `silver.clientes`, que la Sesion 7 construira con MERGE + SCD Tipo 2.
# 
# Duracion en clase: ~15 min (Parte 1) + ~20 min (Parte 2).

# MARKDOWN ********************

# ---
# ## PARTE B - CDC simulado sobre clientes
# 
# La regla de negocio detras de esta fuente es **R5**: *"El cliente conserva su historial al cambiar de datos"* -> tratamiento: *"Dimension con SCD Tipo 2"*. HOY no implementamos SCD Tipo 2 todavia -- eso es la Sesion 7. Hoy solo resolvemos "cual es el registro vigente correcto", no el historial.

# MARKDOWN ********************

# ---
# ## Parte 1 - Simulando un feed CDC de clientes (15 min)
# 
# **Guion:** en vez de un snapshot completo, recibimos solo las filas que cambiaron, marcadas con `operacion` (I/U/D) y ordenadas por `secuencia` -- no necesariamente en el orden en que llegan al notebook.

# MARKDOWN ********************

# ### Paso 1 - Crear el feed CDC simulado

# CELL ********************

columnas_cdc = ["id_cliente", "nombre", "ciudad", "operacion", "fecha_cambio", "secuencia"]

feed_cdc = spark.createDataFrame([
    ("C-4021", "Maria Quispe",     "Arequipa", "I", "2026-07-20", 101),
    ("C-4021", "Maria Quispe P.",  "Lima",     "U", "2026-07-21", 102),  # llega DESORDENADO
    ("C-4021", "Maria Quispe P.",  "Arequipa", "U", "2026-07-22", 104),
    ("C-4021", "Maria Quispe P.",  "Arequipa", "D", "2026-07-25", 109),  # delete tardio
    ("C-7789", "Jose Ramirez",     "Trujillo", "I", "2026-07-19", 201),
    ("C-7789", "Jose Ramirez H.",  "Trujillo", "U", "2026-07-23", 205),
    ("C-9042", "Ana Torres",       "Cusco",    "I", "2026-07-18", 301),
], columnas_cdc)

feed_cdc.orderBy("id_cliente", "secuencia").show(truncate=False)

# MARKDOWN ********************

# ### Paso 2 - Confirmar el desorden a simple vista
# 
# Para `C-4021`, la fila con `secuencia=102` (ciudad=Lima) es un cambio ANTERIOR al de `secuencia=104` (ciudad=Arequipa) en terminos de negocio -- pero en un feed real pudo haber llegado despues. Si nos quedaramos con "lo ultimo en llegar" sin mirar la secuencia, arriesgariamos pisar un dato nuevo con uno viejo.

# MARKDOWN ********************

# > **Checkpoint para el aula:** *"Por que no podemos usar simplemente `fecha_cambio` para decidir cual fila es la mas reciente?"*

# MARKDOWN ********************

# ---
# ## Parte 2 - Resolver el estado vigente con Window + row_number() (20 min)
# 
# **Guion:** usamos una funcion de ventana para quedarnos, por cliente, con la fila de mayor `secuencia` -- y excluimos del snapshot vigente a quien su ultimo cambio haya sido un delete.

# MARKDOWN ********************

# ### Paso 1 - Particionar por cliente y ordenar por secuencia

# CELL ********************

from pyspark.sql import functions as F
from pyspark.sql import Window

w = Window.partitionBy("id_cliente").orderBy(F.col("secuencia").desc())

df_con_rn = feed_cdc.withColumn("rn", F.row_number().over(w))
df_con_rn.orderBy("id_cliente", "rn").show(truncate=False)

# MARKDOWN ********************

# ### Paso 2 - Quedarse con la fila mas reciente por cliente

# CELL ********************

df_mas_reciente = df_con_rn.filter(F.col("rn") == 1).drop("rn")
df_mas_reciente.orderBy("id_cliente").show(truncate=False)

# MARKDOWN ********************

# ### Paso 3 - Excluir a los clientes cuyo ultimo estado es un delete

# CELL ********************

df_vigente = df_mas_reciente.filter(F.col("operacion") != "D")

print("Clientes vigentes:", df_vigente.count())
df_vigente.select("id_cliente", "nombre", "ciudad", "secuencia").show()

# MARKDOWN ********************

# ### Paso 4 - Escribir en silver_clientes_stage

# CELL ********************

(df_vigente
   .write.format("delta")
   .mode("overwrite")
   .saveAsTable("silver_clientes_stage"))

# MARKDOWN ********************

# ### Paso 5 - Verificar los dos casos criticos

# CELL ********************

spark.sql("""
  SELECT * FROM silver_clientes_stage WHERE id_cliente = 'C-4021'
""").show()

# Debe devolver 0 filas: C-4021 fue eliminado (su ultimo cambio, secuencia=109, es D).
# De no haber sido eliminado, su ciudad vigente seria Arequipa (secuencia=104), NO Lima (secuencia=102).

# MARKDOWN ********************

# **Ojo -- error comun:** si hubieras ordenado por `fecha_cambio` en vez de `secuencia`, o te hubieras quedado con "la ultima fila del DataFrame" sin usar `Window`, el resultado habria sido incorrecto para `C-4021` -- Spark no garantiza el orden de lectura de un DataFrame sin un `orderBy` explicito.

# MARKDOWN ********************

# ---
# ## Checklist de este notebook
# 
# - [ ] `silver_clientes_stage` existe en `lh_silver` con, como maximo, una fila por `id_cliente`
# - [ ] `C-4021` (cuyo ultimo cambio fue un delete) NO aparece en `silver_clientes_stage`
# - [ ] `C-4021`, de haber quedado vigente, mostraria ciudad = Arequipa (secuencia 104), no Lima (secuencia 102)
# - [ ] Notebook commiteado al repo de Git del workspace
# 
# **Honestidad sobre el alcance:** `silver_clientes_stage` NO guarda historial de cambios -- cada corrida sobreescribe el estado vigente completo. No hay columnas de vigencia ni manejo de tipos de atributo SCD. Eso llega en la Sesion 7 con MERGE y SCD Tipo 2 sobre `silver.clientes` (R5). Hoy solo resolvemos "cual es el estado ACTUAL correcto de cada cliente dado un feed desordenado con deletes?", sin guardar historial.
# 
# **Commit al repo:** `git commit -m "S5 Parte B: nb_silver_clientes_cdc -- estado vigente con Window/row_number (punto de partida de R5, SCD2 en Sesion 7)"`

# MARKDOWN ********************

# ---

# MARKDOWN ********************

# ## Sesion 7 - Continuacion: SCD Tipo 2 sobre clientes
# 
# **Data Wizard Academy - Microsoft Fabric Track**
# 
# Este notebook sigue siendo el MISMO de la Sesion 5 -- no se crea uno nuevo. La Parte 1 y 2 de arriba
# (feed CDC simulado, resolucion del estado vigente con `Window` + `row_number()`) siguen intactas y no
# se tocan.
# 
# La Sesion 5 dejo resuelto "cual es el registro vigente correcto" de cada cliente a partir de un feed CDC
# desordenado, escrito en `silver_clientes_stage` -- pero SIN guardar historial: cada corrida sobreescribe
# el estado completo. Un detalle importante para hoy: el ultimo evento de `C-4021` (Maria Quispe) en el
# feed de la Sesion 5 fue un **delete** (`secuencia=109`), asi que `C-4021` NO aparece en
# `silver_clientes_stage` -- quedo fuera del alcance de esa tabla por diseño.
# 
# Regla de negocio **R5**: *"El cliente conserva su historial al cambiar de datos"* -> tratamiento:
# *"Dimension con SCD Tipo 2"*. Hoy construimos **`silver_clientes`** (retirando el sufijo `_stage`, mismo
# criterio que `silver_ventas` en `nb_silver_ventas_idempotencia.ipynb`) como una dimension SCD Tipo 2 real,
# usando `MERGE INTO`.

# MARKDOWN ********************

# ---
# ## PARTE B - SCD Tipo 2 sobre clientes (R5)
# 
# Columnas de vigencia que agrega el diseño de hoy sobre las columnas de negocio existentes
# (`id_cliente`, `nombre`, `ciudad`):
# 
# - `fecha_inicio_vigencia` -- fecha desde la que esa version de la fila es valida.
# - `fecha_fin_vigencia` -- fecha hasta la que fue valida (`9999-12-31` mientras siga vigente).
# - `es_actual` -- `true` si es la version vigente hoy, `false` si es una version historica.
# 
# **Honestidad sobre el alcance:** esto NO cubre deletes tardios combinados con SCD2 -- ya vimos deletes en
# la Sesion 5 (Parte 1-2), pero combinarlos con historial (¿que significa "cerrar" la version vigente de un
# cliente que se elimina?) es mas complejo y queda fuera de esta sesion. Es un preview de lo que un pipeline
# de produccion completo necesitaria.

# MARKDOWN ********************

# ### Paso 1 - Carga inicial de `silver_clientes` desde el estado vigente de la Sesion 5
# 
# `silver_clientes_stage` tiene, hoy, dos clientes vigentes: `C-7789` (Jose Ramirez H., Trujillo) y `C-9042`
# (Ana Torres, Cusco). `C-4021` no aparece -- fue eliminado. La carga inicial de la dimension SCD2 parte de
# ESTE estado: cada fila entra como la primera version vigente (`es_actual = true`).

# CELL ********************

from datetime import date

hoy_clientes = date.today()
FECHA_FIN_ABIERTA = date(9999, 12, 31)

df_clientes_vigentes_s5 = spark.read.table("silver_clientes_stage")

df_carga_inicial_scd2 = (df_clientes_vigentes_s5
    .withColumn("fecha_inicio_vigencia", F.lit(hoy_clientes))
    .withColumn("fecha_fin_vigencia", F.lit(FECHA_FIN_ABIERTA))
    .withColumn("es_actual", F.lit(True))
    .select("id_cliente", "nombre", "ciudad", "fecha_inicio_vigencia", "fecha_fin_vigencia", "es_actual"))

(df_carga_inicial_scd2.write.format("delta").mode("overwrite")
   .saveAsTable("silver_clientes"))
spark.sql("SELECT * FROM silver_clientes ORDER BY id_cliente").show(truncate=False)

# MARKDOWN ********************

# ### Paso 2 - Maria Quispe (C-4021) regresa como clienta: un nuevo evento CDC
# 
# `C-4021` no esta en `silver_clientes` -- fue eliminada en el feed de la Sesion 5. Simulamos que hoy vuelve
# a comprar en RetailPe (reactivacion): llega un evento CDC nuevo para `C-4021`, en **Arequipa** (la misma
# ciudad con la que aparecio por primera vez en el feed de la Sesion 5). Con `MERGE`, un cliente que no
# tiene ninguna version vigente se resuelve con `WHEN NOT MATCHED THEN INSERT` -- el mismo mecanismo que ya
# usamos para ventas nuevas en la Parte A.

# CELL ********************

cambios_clientes = spark.createDataFrame([
    ("C-4021", "Maria Quispe", "Arequipa"),
], ["id_cliente", "nombre", "ciudad"])

# 'staged': una fila para CERRAR la version vigente actual si el atributo cambio (merge_key = id real),
# y una fila para INSERTAR la version nueva (merge_key = NULL -- nunca hace match, siempre es INSERT)
df_actual_vigente = spark.read.table("silver_clientes").filter("es_actual = true")

staged_base = (cambios_clientes.alias("s")
    .join(df_actual_vigente.alias("t"), on="id_cliente", how="left")
    .withColumn("hubo_cambio",
        F.col("t.id_cliente").isNotNull() & (F.col("t.ciudad") != F.col("s.ciudad"))))

cerrar = (staged_base.filter("hubo_cambio")
    .select(F.col("t.id_cliente").alias("merge_key"),
            F.col("s.id_cliente").alias("id_cliente"), F.col("s.nombre").alias("nombre"), F.col("s.ciudad").alias("ciudad")))

insertar = (staged_base.filter("hubo_cambio OR t.id_cliente IS NULL")
    .select(F.lit(None).cast("string").alias("merge_key"),
            F.col("s.id_cliente").alias("id_cliente"), F.col("s.nombre").alias("nombre"), F.col("s.ciudad").alias("ciudad")))

staged_cambios = cerrar.unionByName(insertar)
staged_cambios.createOrReplaceTempView("staged_cambios_clientes")
staged_cambios.show()

# MARKDOWN ********************

# ### Paso 3 - Ejecutar el MERGE de SCD2 (cerrar version vieja + insertar version nueva)
# 
# Un `WHEN MATCHED` solo puede ejecutar UNA accion por fila -- por eso el patron `staged` de arriba separa
# el cierre (fila con `merge_key` real, hace match y dispara `UPDATE`) de la insercion (fila con
# `merge_key = NULL`, nunca hace match y dispara `INSERT`). Es la sintaxis estándar de "cerrar + insertar"
# para SCD Tipo 2 con `MERGE INTO` en Delta Lake.

# CELL ********************

spark.sql("""
MERGE INTO silver_clientes AS destino
USING staged_cambios_clientes AS origen
ON destino.id_cliente = origen.merge_key AND destino.es_actual = true
WHEN MATCHED THEN
  UPDATE SET
    destino.fecha_fin_vigencia = current_date(),
    destino.es_actual = false
WHEN NOT MATCHED THEN
  INSERT (id_cliente, nombre, ciudad, fecha_inicio_vigencia, fecha_fin_vigencia, es_actual)
  VALUES (origen.id_cliente, origen.nombre, origen.ciudad, current_date(), DATE('9999-12-31'), true)
""")
spark.sql("SELECT * FROM silver_clientes WHERE id_cliente = 'C-4021'").show(truncate=False)
# C-4021 reactivada: una sola fila nueva, es_actual = true, ciudad = Arequipa

# MARKDOWN ********************

# ### Paso 4 - Maria Quispe cambia de ciudad: Arequipa -> Lima
# 
# Ahora el caso central de R5: un cambio de atributo sobre un cliente que YA tiene una version vigente.
# Repetimos el mismo patron `staged` + `MERGE`, esta vez con `hubo_cambio = true` para `C-4021` -- debe
# CERRAR la fila de Arequipa (`es_actual = false`, `fecha_fin_vigencia` poblada) e INSERTAR una fila nueva
# en Lima (`es_actual = true`).

# CELL ********************

cambios_clientes_2 = spark.createDataFrame([
    ("C-4021", "Maria Quispe P.", "Lima"),
], ["id_cliente", "nombre", "ciudad"])

df_actual_vigente_2 = spark.read.table("silver_clientes").filter("es_actual = true")

staged_base_2 = (cambios_clientes_2.alias("s")
    .join(df_actual_vigente_2.alias("t"), on="id_cliente", how="left")
    .withColumn("hubo_cambio",
        F.col("t.id_cliente").isNotNull() & (F.col("t.ciudad") != F.col("s.ciudad"))))

cerrar_2 = (staged_base_2.filter("hubo_cambio")
    .select(F.col("t.id_cliente").alias("merge_key"),
            F.col("s.id_cliente").alias("id_cliente"), F.col("s.nombre").alias("nombre"), F.col("s.ciudad").alias("ciudad")))

insertar_2 = (staged_base_2.filter("hubo_cambio OR t.id_cliente IS NULL")
    .select(F.lit(None).cast("string").alias("merge_key"),
            F.col("s.id_cliente").alias("id_cliente"), F.col("s.nombre").alias("nombre"), F.col("s.ciudad").alias("ciudad")))

staged_cambios_2 = cerrar_2.unionByName(insertar_2)
staged_cambios_2.createOrReplaceTempView("staged_cambios_clientes_2")

spark.sql("""
MERGE INTO silver_clientes AS destino
USING staged_cambios_clientes_2 AS origen
ON destino.id_cliente = origen.merge_key AND destino.es_actual = true
WHEN MATCHED THEN
  UPDATE SET destino.fecha_fin_vigencia = current_date(), destino.es_actual = false
WHEN NOT MATCHED THEN
  INSERT (id_cliente, nombre, ciudad, fecha_inicio_vigencia, fecha_fin_vigencia, es_actual)
  VALUES (origen.id_cliente, origen.nombre, origen.ciudad, current_date(), DATE('9999-12-31'), true)
""")

# MARKDOWN ********************

# ### Paso 5 - Verificar: estado actual vs. historial completo

# CELL ********************

# Estado actual -- lo que cualquier reporte de Gold deberia consumir
spark.sql("""
  SELECT id_cliente, nombre, ciudad, fecha_inicio_vigencia, fecha_fin_vigencia, es_actual
  FROM silver_clientes WHERE es_actual = true ORDER BY id_cliente
""").show(truncate=False)

# CELL ********************

# Historial completo de Maria Quispe -- deben aparecer DOS filas
spark.sql("""
  SELECT id_cliente, nombre, ciudad, fecha_inicio_vigencia, fecha_fin_vigencia, es_actual
  FROM silver_clientes WHERE id_cliente = 'C-4021' ORDER BY fecha_inicio_vigencia
""").show(truncate=False)
# Fila 1: ciudad = Arequipa, es_actual = false, fecha_fin_vigencia = hoy (version cerrada)
# Fila 2: ciudad = Lima, es_actual = true, fecha_fin_vigencia = 9999-12-31 (version vigente)

# MARKDOWN ********************

# > **Ojo -- error comun:** si el `ON` del MERGE no incluyera `destino.es_actual = true`, una fila ya
# > CERRADA de un cambio anterior tambien podria hacer match y quedar sobreescrita -- el filtro de vigencia
# > en el `ON` es lo que garantiza que solo se cierra la version vigente actual, nunca el historial ya
# > congelado.

# MARKDOWN ********************

# ---
# ## Checklist de la Sesion 7 (Parte B)
# 
# - [ ] `silver_clientes` existe (sin sufijo `_stage`) con columnas `fecha_inicio_vigencia`,
#       `fecha_fin_vigencia`, `es_actual`
# - [ ] `C-4021` (Maria Quispe, eliminada en la Sesion 5) aparece reactivada y con DOS filas de historial
# - [ ] La fila vieja de `C-4021` tiene `ciudad = Arequipa`, `es_actual = false`, `fecha_fin_vigencia` poblada
# - [ ] La fila nueva de `C-4021` tiene `ciudad = Lima`, `es_actual = true`, `fecha_fin_vigencia = 9999-12-31`
# - [ ] `C-7789` y `C-9042` (sin cambios) siguen con una sola fila vigente cada uno
# - [ ] Puedes explicar por que `destino.es_actual = true` en el `ON` del MERGE es obligatorio
# - [ ] Notebook commiteado al repo de Git del workspace
# 
# **Honestidad sobre el alcance:** un delete tardio combinado con SCD2 (¿que pasa si `C-4021` se elimina
# de nuevo, ahora que SI tiene historial en `silver_clientes`?) no se implementa hoy -- queda como preview
# de lo que un pipeline de produccion completo necesitaria.
# 
# **Que sigue:** el bloque "2 - Nucleo de Ingenieria" (Sesiones 4-7) queda cerrado. `silver_ventas` y
# `silver_clientes`, ya con MERGE y SCD2, son el insumo limpio que el siguiente bloque, "Modelado para
# Consumo" (Sesiones 8-9: capa Gold, star schema, grano, surrogate keys), va a consumir.
# 
# **Commit al repo:** `git commit -m "S7 Parte B: nb_silver_clientes_cdc -- SCD Tipo 2 sobre silver_clientes via MERGE (R5)"`
