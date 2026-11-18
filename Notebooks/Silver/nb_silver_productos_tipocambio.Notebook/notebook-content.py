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

# # Sesion 8 - Parte 1: Silver minimo para productos y tipo_cambio
# 
# **Data Wizard Academy - Microsoft Fabric Track**
# 
# Notebook **NUEVO**. A diferencia de `nb_silver_ventas_idempotencia.ipynb` y
# `nb_silver_clientes_cdc.ipynb` (que EXTIENDEN notebooks de las Sesiones 4-7), este
# notebook no tiene un antecesor que extender: `productos` y `tipo_cambio` estuvieron
# `activo = NO` en `control_ingesta_retailpe.xlsx` desde la Sesion 3 y JAMAS pasaron por
# Bronze ni Silver. Las Sesiones 4-7 solo trabajaron `ventas` y `clientes` en Silver a
# proposito, porque ahi se enseñaban tecnicas nuevas (idempotencia, CDC, cuarentena,
# MERGE/SCD2) -- pero el principio Medallion exige que TODA fuente pase por las 3 capas,
# sin excepcion.
# 
# ## Que paso antes de este notebook (Parte 0, portal de Fabric -- sin celdas de codigo)
# 
# 1. Se cambio `activo` de `NO` a `SI` para `ING003` (productos) e `ING004` (tipo_cambio)
#    en `control_ingesta_retailpe.xlsx`, hoja `ingestas`.
# 2. Se corrio de nuevo `pl_cargar_control` (recarga el Excel a `wh_control`, Upsert).
# 3. Se corrio de nuevo `pl_bronze_metadata` -- el `Lookup_Ingestas` (que filtra
#    `WHERE activo = 'SI'`) ahora tambien trae estas 2 fuentes.
# 4. Se verifico en `lh_bronze`: `bronze_productos` y `bronze_tipo_cambio` existen con
#    datos reales de la API.
# 
# **Nota tecnica importante (verificada leyendo `Codigo-Metadata-Driven-Sesion-03.pdf`):**
# el `Switch_TipoOrigen` de `pl_bronze_metadata`, tal como quedo construido en la Sesion 3,
# solo tenia los casos `SQL` y `CSV` -- `productos` y `tipo_cambio` tienen
# `tipo_origen = API_REST` en `control_conexiones`. Por eso Parte 0 tambien agrega un
# tercer caso `API_REST` al Switch (con un `Copy_API_Generico` parametrizado igual que
# los otros dos: `Source` = REST, URL relativa `@{item().objeto_origen}`, `Destination`
# = `@{item().tabla_destino}` en `lh_bronze`). Esto NO modifica los casos `SQL`/`CSV` ya
# construidos -- solo agrega el caso que faltaba para que el pipeline metadata-driven
# soporte, de verdad, las 5 fuentes que el Excel describe.
# 
# ## Fuentes de hoy (schema real, leido de `function_app.py`, sin inventar columnas)
# 
# - **`bronze_productos`** viene de `GET /api/productos`, que responde
#   `{"data": [...], "total": N}`. Cada producto trae `id_producto` (int), `nombre`
#   (string), `categoria` (string, **nullable** -- hay un producto real con
#   `categoria = None`), `precio_usd` (float). Un producto (`Cafetera Electrica`) ademas
#   trae una clave opcional `descuento` que no aparece en los demas -- Bronze la copia tal
#   cual (columna con nulos donde no aplica).
# - **`bronze_tipo_cambio`** viene de `GET /api/tipo_cambio`, que responde UN SOLO objeto
#   (no una lista): `{"moneda_origen": "USD", "moneda_destino": "PEN", "valor": <float>,
#   "fecha": "2026-08-15"}`. Cada corrida del pipeline trae la tasa del dia -- por eso
#   `bronze_tipo_cambio` acumula, corrida tras corrida, una fila por fecha en la que se
#   ejecuto la ingesta (Full/Overwrite en Bronze significa "reemplaza con lo que trajo
#   esta corrida", asi que en la practica valdria la pena, en produccion, acumular con
#   append -- hoy trabajamos con lo que la API entrega en el momento del laboratorio).
# 
# ## Reglas de negocio de hoy (Caso-de-uso-RetailPe, secciones 4 y 5)
# 
# - Seccion 4: productos trae *"Nulos, categorias inconsistentes"*; tipo_cambio trae
#   *"Dias faltantes (feriados)"*.
# - **R3:** "Un producto sin categoria se acepta" -> tratamiento: "Se imputa como
#   'Sin categoria'"

# MARKDOWN ********************

# ---
# ## Parte 1.A - Silver de productos

# CELL ********************

from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, StringType, DoubleType

df_bronze_productos = spark.read.table("lh_bronze.bronze_productos")
df_bronze_productos.printSchema()
df_bronze_productos.show(10, truncate=False)

# MARKDOWN ********************

# ### Paso 1 - Cast de tipos segun el schema real de la Azure Function
# 
# `id_producto` llega como entero, `precio_usd` como decimal -- casteamos explicitamente
# en vez de confiar en la inferencia automatica del conector REST del pipeline (el mismo
# criterio de todas las sesiones anteriores: nunca confiar en un tipo inferido para una
# columna que participa en joins o agregaciones).

# CELL ********************

df_productos_tipado = (df_bronze_productos
    .withColumn("id_producto", F.col("id_producto").cast(IntegerType()))
    .withColumn("nombre", F.col("nombre").cast(StringType()))
    .withColumn("categoria", F.col("categoria").cast(StringType()))
    .withColumn("precio_usd", F.col("precio_usd").cast(DoubleType())))

# MARKDOWN ********************

# ### Paso 2 - R3: categoria nula o vacia -> "Sin categoria"
# 
# `function_app.py` tiene, a proposito, un producto (`Mochila Urbana`, `id_producto = 7`)
# con `categoria = None`. R3 es explicita: no se rechaza, no va a cuarentena -- se imputa.
# A diferencia de R1/R2 en ventas (Sesion 6), R3 nunca genera una fila de cuarentena: es
# una regla de LIMPIEZA, no de VALIDACION.

# CELL ********************

df_silver_productos = df_productos_tipado.withColumn(
    "categoria",
    F.when(F.col("categoria").isNull() | (F.trim(F.col("categoria")) == ""), "Sin categoria")
     .otherwise(F.col("categoria"))
)

df_silver_productos.filter(F.col("categoria") == "Sin categoria").show(truncate=False)

# MARKDOWN ********************

# ### Paso 3 - Escribir `silver_productos` (overwrite directo, primera vez)
# 
# A diferencia de `silver_ventas` (MERGE, Sesion 7) o `silver_clientes` (SCD2, Sesion 7),
# `silver_productos` se escribe hoy por PRIMERA VEZ -- no hay historial previo que
# preservar ni fila que actualizar. `overwrite` directo es correcto aqui; en sesiones
# futuras, si `productos` necesitara historial de cambios de precio, se aplicaria el mismo
# patron staged + MERGE que la Sesion 7 uso para clientes -- eso queda fuera de alcance
# de hoy (no hay regla de negocio de RetailPe que lo pida todavia).

# CELL ********************

(df_silver_productos.write.format("delta").mode("overwrite")
    .saveAsTable("silver_productos"))

spark.sql("SELECT COUNT(*) AS filas_silver_productos FROM silver_productos").show()
spark.sql("SELECT categoria, COUNT(*) AS n FROM silver_productos GROUP BY categoria").show()

# MARKDOWN ********************

# ---
# ## Parte 1.B - Silver de tipo_cambio

# CELL ********************

df_bronze_tipo_cambio = spark.read.table("lh_bronze.bronze_tipo_cambio")
df_bronze_tipo_cambio.printSchema()
df_bronze_tipo_cambio.show(truncate=False)

# MARKDOWN ********************

# ### Paso 4 - Cast de tipos
# 
# Mismo criterio que en productos: `valor` a `double`, `fecha` a `date` (la API lo entrega
# como string `"2026-08-15"`), `moneda_origen`/`moneda_destino` como string.

# CELL ********************

df_tipo_cambio_tipado = (df_bronze_tipo_cambio
    .withColumn("moneda_origen", F.col("moneda_origen").cast(StringType()))
    .withColumn("moneda_destino", F.col("moneda_destino").cast(StringType()))
    .withColumn("valor", F.col("valor").cast(DoubleType()))
    .withColumn("fecha", F.to_date(F.col("fecha"))))

# MARKDOWN ********************

# ### Paso 5 - Detectar dias faltantes (feriados) SIN resolverlos hoy
# 
# El Caso de uso es explicito: `tipo_cambio` trae "Dias faltantes (feriados)" como
# problema conocido. Hoy solo **detectamos** el hueco -- comparamos el rango de fechas
# esperado (desde la fecha minima hasta la fecha maxima que trajo la API) contra las
# fechas que realmente existen en `silver_tipo_cambio`, y mostramos cuantas faltan.
# 
# **Honestidad de alcance:** resolver el hueco con forward-fill (rellenar un feriado con
# el ultimo tipo de cambio conocido, tecnica estandar para series de tiempo con
# gaps) es una implementacion real de varias lineas (generar el calendario completo,
# LEFT JOIN, `last()` con una ventana ordenada por fecha e ignorando nulos) que no
# amerita resolverse a medias en una sesion dedicada a otra cosa. Se menciona como
# preview de una tecnica futura, no se implementa a fondo hoy.

# CELL ********************

fila_rango = df_tipo_cambio_tipado.agg(
    F.min("fecha").alias("fecha_min"), F.max("fecha").alias("fecha_max")
).collect()[0]
fecha_min, fecha_max = fila_rango["fecha_min"], fila_rango["fecha_max"]

df_calendario_esperado = spark.sql(f"""
    SELECT explode(sequence(to_date('{fecha_min}'), to_date('{fecha_max}'), interval 1 day)) AS fecha
""")

df_dias_faltantes = df_calendario_esperado.join(
    df_tipo_cambio_tipado.select("fecha").distinct(), on="fecha", how="left_anti"
)

print("Fechas esperadas:", df_calendario_esperado.count())
print("Fechas con tipo de cambio real:", df_tipo_cambio_tipado.select("fecha").distinct().count())
print("Dias faltantes detectados (posibles feriados):", df_dias_faltantes.count())
df_dias_faltantes.orderBy("fecha").show(20)

# MARKDOWN ********************

# > **Preview (no implementado hoy):** para rellenar cada dia faltante con el ultimo
# > tipo de cambio conocido, el patron seria generar el calendario completo, hacer
# > `LEFT JOIN` contra `silver_tipo_cambio`, y usar
# > `F.last("valor", ignorenulls=True).over(Window.orderBy("fecha").rowsBetween(Window.unboundedPreceding, 0))`
# > para propagar el ultimo valor no nulo hacia adelante. Queda fuera de alcance de la
# > Sesion 8.

# MARKDOWN ********************

# ### Paso 6 - Escribir `silver_tipo_cambio` (overwrite directo, primera vez)

# CELL ********************

(df_tipo_cambio_tipado.write.format("delta").mode("overwrite")
    .saveAsTable("silver_tipo_cambio"))

spark.sql("SELECT COUNT(*) AS filas_silver_tipo_cambio FROM silver_tipo_cambio").show()
spark.sql("SELECT * FROM silver_tipo_cambio ORDER BY fecha").show(truncate=False)

# MARKDOWN ********************

# ---
# ## Checklist de la Sesion 8 - Parte 1
# 
# - [ ] `bronze_productos` y `bronze_tipo_cambio` existen en `lh_bronze` (verificado en
#       Parte 0, portal de Fabric).
# - [ ] `silver_productos` existe con `categoria` nunca nula ni vacia (R3 aplicada: "Sin
#       categoria" donde correspondia).
# - [ ] `silver_tipo_cambio` existe con `valor` tipo `double` y `fecha` tipo `date`.
# - [ ] El conteo de dias faltantes (feriados) se calculo y se muestra explicitamente --
#       no se resolvio a fondo hoy, a proposito (honestidad de alcance).
# - [ ] Puedes explicar por que R3 es una regla de LIMPIEZA (imputa) y no de VALIDACION
#       (no genera fila de cuarentena), a diferencia de R1/R2 en ventas.
# 
# Con `silver_productos`, `silver_tipo_cambio`, `silver_ventas`, `silver_clientes` y
# `silver_tiendas` ya limpios, la Parte 2 de la Sesion 8 diseña el star schema de Gold, y
# la Parte 3 lo construye en `nb_gold_star_schema.ipynb`.
