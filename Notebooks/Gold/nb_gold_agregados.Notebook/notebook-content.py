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

# # Sesion 9 - Parte 2: Agregados materializados en Gold
# 
# **Data Wizard Academy - Microsoft Fabric Track**
# 
# Notebook **NUEVO** (tema nuevo: agregados). No duplica nada de `nb_gold_star_schema`: ese
# notebook (Sesion 8) construyo el star schema -- `dim_tiempo`, `dim_cliente`, `dim_producto`,
# `dim_tienda` y `hechos_ventas` en `lh_gold`. Este notebook **solo lee** esas tablas y
# materializa, encima de ellas, las tablas de agregados que responden las preguntas de negocio
# del Comercial:
# 
# - *"¿Cuanto vendimos ayer por tienda, categoria y canal?"*
# - *"¿Cual es el ticket promedio y como evoluciona por mes?"*
# 
# **Antes de correr:** adjunta `lh_gold` al notebook (puede ser el default o uno adicional). Para
# el preview en dolares del Paso 7 adjunta tambien `lh_silver`. Todas las tablas se califican
# con su lakehouse (`lh_gold.tabla`, `lh_silver.tabla`) porque el default del notebook puede ser
# otro. En Lakehouse no existe el schema `dbo`: los nombres son planos con guion bajo.
# 
# ## Tablas que se crean hoy (todas en `lh_gold`)
# 
# | Tabla | Grano (una fila =) | Responde |
# |---|---|---|
# | `agg_ventas_diarias_tienda` | un dia x una tienda | "¿Cuanto vendimos ayer por tienda?" |
# | `agg_ventas_mensual_tienda` | un mes x una tienda | ventas y ticket por tienda y mes |
# | `agg_ventas_mensual` | un mes | "¿Ticket promedio y su evolucion por mes?" |
# 
# ## Honestidad de alcance (lee esto antes de prometer nada al negocio)
# 
# - `hechos_ventas` tiene `id_venta`, `monto`, `fecha_venta`, `sk_tienda`, `sk_cliente`, `sk_tiempo`.
#   **No tiene `sk_producto` ni `id_producto`, y tampoco `canal`** (limitacion documentada en la
#   Sesion 8: `silver_ventas` no trae esas columnas). Por lo tanto **"por categoria" y "por canal"
#   NO se pueden responder hoy** -- no se inventan columnas. La celda del Paso 1 lo comprueba
#   contra el esquema real.
# - "Ticket" aqui = monto promedio por `id_venta` (grano R6: una fila = una linea de venta). No
#   existe un identificador de ticket separado.
# - Direct Lake y el modelo semantico NO se hacen en este notebook: son pasos de PORTAL de Fabric
#   (ver el lab de la Sesion 9). RLS/OLS es Sesion 15; orquestacion es Sesion 10; alertas, Sesion 11.

# CELL ********************

from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, DoubleType, StringType

# MARKDOWN ********************

# ---
# ## Paso 0 - Verificar que el star schema de la Sesion 8 existe
# 
# Si alguna tabla no existe, esta celda falla aqui (con un mensaje claro de Spark) en vez de
# fallar a mitad de la construccion de los agregados.

# CELL ********************

tablas_gold = ["dim_tiempo", "dim_cliente", "dim_producto", "dim_tienda", "hechos_ventas"]

for t in tablas_gold:
    n = spark.read.table(f"lh_gold.{t}").count()
    print(f"lh_gold.{t}: {n} filas")

spark.read.table("lh_gold.hechos_ventas").printSchema()

# MARKDOWN ********************

# ---
# ## Paso 1 - ¿Que preguntas puede responder `hechos_ventas` HOY?
# 
# Antes de construir nada se comprueba contra el esquema real. Esto evita la peor conversacion
# posible con el negocio: prometer "ventas por categoria" y descubrir en la demo que la columna
# nunca existio.

# CELL ********************

cols_hechos = spark.read.table("lh_gold.hechos_ventas").columns
print("Columnas de hechos_ventas:", cols_hechos)

puede_canal = "canal" in cols_hechos
puede_categoria = ("sk_producto" in cols_hechos) or ("id_producto" in cols_hechos)

print("Por tienda:    ", "SI (sk_tienda existe)" if "sk_tienda" in cols_hechos else "NO")
print("Por canal:     ", "SI" if puede_canal else "NO -- hechos_ventas no tiene columna de canal")
print("Por categoria: ", "SI" if puede_categoria else "NO -- hechos_ventas no tiene sk_producto/id_producto")

# MARKDOWN ********************

# ---
# ## Paso 2 - Validar el star schema ANTES de agregar
# 
# Tres validaciones que, si fallan, producen agregados incorrectos sin ningun error visible, y
# que ademas son requisitos de Direct Lake en el modelo semantico:
# 
# 1. **R6 (grano):** `id_venta` no se repite en `hechos_ventas`. Si hubiera duplicados,
#    `ventas_totales` se inflaria.
# 2. **Unicidad de las `sk_*` en cada dimension** (lado "uno" de la relacion). Direct Lake falla
#    la consulta si encuentra duplicados en una columna del lado "uno".
# 3. **Tipos de las llaves:** `hechos_ventas.sk_*` y `dim_*.sk_*` deben ser de la misma familia
#    de tipos. Ojo: `row_number()` produce `int` pero la fila "Desconocido" (`sk = -1`) creada con
#    `createDataFrame` entra como `bigint`, y `unionByName` deja la dimension en `bigint`. Para el
#    modelo semantico `int` y `bigint` son ambos "Numero entero", pero conviene saberlo.

# CELL ********************

df_hechos = spark.read.table("lh_gold.hechos_ventas")

# 1. R6 -- grano: una fila por id_venta
total = df_hechos.count()
distintas = df_hechos.select("id_venta").distinct().count()
print("R6 | filas:", total, "| id_venta distintos:", distintas, "| duplicados:", total - distintas)

# 2. Unicidad de la sk en cada dimension (lado "uno")
for tabla, sk in [("dim_tiempo", "sk_tiempo"), ("dim_tienda", "sk_tienda"), ("dim_cliente", "sk_cliente")]:
    d = spark.read.table(f"lh_gold.{tabla}")
    n, nd = d.count(), d.select(sk).distinct().count()
    print(f"UNICIDAD | {tabla}.{sk}: {n} filas, {nd} distintas ->", "OK" if n == nd else "DUPLICADOS")

# 3. Tipos de las llaves: misma familia (entero) en hechos y dimension
def familia(tipo):
    return "entero" if tipo in ("tinyint", "smallint", "int", "bigint") else tipo

for tabla, sk in [("dim_tiempo", "sk_tiempo"), ("dim_tienda", "sk_tienda"), ("dim_cliente", "sk_cliente")]:
    t_hecho = dict(df_hechos.dtypes)[sk]
    t_dim = dict(spark.read.table(f"lh_gold.{tabla}").dtypes)[sk]
    print(f"TIPOS | hechos_ventas.{sk}={t_hecho} vs {tabla}.{sk}={t_dim} ->",
          "OK" if familia(t_hecho) == familia(t_dim) else "NO COINCIDEN")

# R4 -- cuantas ventas quedaron apuntando a "Desconocido" (sk = -1) y cuantas sin sk_tiempo
print("R4 | ventas con sk_tienda = -1:", df_hechos.filter("sk_tienda = -1").count())
print("R4 | ventas con sk_cliente = -1:", df_hechos.filter("sk_cliente = -1").count())
print("R4 | ventas SIN sk_tiempo (se excluyen de los agregados por fecha):",
      df_hechos.filter("sk_tiempo IS NULL").count())

# MARKDOWN ********************

# ---
# ## Paso 3 - Base de agregacion: hechos + dim_tiempo + dim_tienda
# 
# Se une `hechos_ventas` con `dim_tiempo` (INNER: una venta sin fecha en el calendario no puede
# ubicarse en ningun dia/mes, y se reporta aparte en el Paso 2) y con `dim_tienda` (LEFT: una venta
# de tienda "Desconocido" **no** debe desaparecer -- sigue contando bajo `DESCONOCIDO`, R4).
# 
# Se castea `monto` a `double` explicitamente antes de sumar: nunca se confia en el tipo heredado
# para una columna que se agrega.
# 
# ### Regla de oro de los agregados: se guardan medidas ADITIVAS
# 
# `num_ventas` y `ventas_totales` se pueden sumar entre filas sin error. `ticket_promedio` **no**:
# el promedio de promedios es incorrecto cuando los grupos tienen distinto tamano. Por eso el
# ticket se calcula siempre como `ventas_totales / num_ventas` **dentro de cada grano** -- en la
# tabla (para consumo directo) y en DAX (`DIVIDE([Ventas Totales], [Nº Ventas])`, para cualquier
# combinacion de filtros). Nunca se promedia la columna `ticket_promedio`.

# CELL ********************

df_tiempo = spark.read.table("lh_gold.dim_tiempo").select("sk_tiempo", "fecha", "anio", "mes", "nombre_mes")
df_tienda = (spark.read.table("lh_gold.dim_tienda")
    .select("sk_tienda", F.col("nombre").alias("nombre_tienda"), F.col("ciudad").alias("ciudad_tienda")))

df_base = (df_hechos
    .withColumn("monto", F.col("monto").cast(DoubleType()))
    .join(df_tiempo, on="sk_tiempo", how="inner")
    .join(df_tienda, on="sk_tienda", how="left"))

def agregar(df, claves):
    """Aditivas (num_ventas, ventas_totales) + ticket = total / n dentro del grano."""
    return (df.groupBy(*claves)
        .agg(F.countDistinct("id_venta").alias("num_ventas"),
             F.sum("monto").alias("ventas_totales"))
        .withColumn("ticket_promedio", F.round(F.col("ventas_totales") / F.col("num_ventas"), 2)))

df_diaria_tienda = agregar(df_base,
    ["sk_tiempo", "fecha", "anio", "mes", "sk_tienda", "nombre_tienda", "ciudad_tienda"])

df_mensual_tienda = agregar(df_base,
    ["anio", "mes", "nombre_mes", "sk_tienda", "nombre_tienda", "ciudad_tienda"])

df_mensual_base = agregar(df_base, ["anio", "mes", "nombre_mes"])

print("Filas agg diaria x tienda:", df_diaria_tienda.count())
print("Filas agg mensual x tienda:", df_mensual_tienda.count())
print("Filas agg mensual:", df_mensual_base.count())

# MARKDOWN ********************

# ---
# ## Paso 4 - Evolucion mensual: ventas del mes anterior y variacion %
# 
# "Ticket promedio y su evolucion por mes" necesita comparar cada mes contra el anterior. La
# version ingenua (`lag()` sobre las filas ordenadas) **salta meses sin ventas**: el "anterior"
# seria dos meses atras sin avisar. Aqui se usa un indice de periodo (`anio * 12 + mes`) y un
# self-join contra `periodo + 1`, que deja `NULL` cuando el mes anterior realmente no existe.

# CELL ********************

df_m = df_mensual_base.withColumn("periodo", F.col("anio") * 12 + F.col("mes"))

df_prev = df_m.select(
    (F.col("periodo") + 1).alias("periodo"),
    F.col("ventas_totales").alias("ventas_mes_anterior"),
    F.col("ticket_promedio").alias("ticket_mes_anterior"))

df_mensual = (df_m.join(df_prev, on="periodo", how="left")
    .withColumn("variacion_ventas_pct",
        F.round((F.col("ventas_totales") - F.col("ventas_mes_anterior")) / F.col("ventas_mes_anterior") * 100, 2))
    .drop("periodo"))

df_mensual.orderBy("anio", "mes").show(truncate=False)

# MARKDOWN ********************

# ---
# ## Paso 5 - Materializar en `lh_gold` (overwrite completo, idempotente)
# 
# Los agregados son **derivados**: se recalculan enteros desde `hechos_ventas` en cada corrida.
# Por eso aqui el `overwrite` completo es lo correcto -- no `replaceWhere` ni `append` (un append
# duplicaria los agregados; un `replaceWhere` exigiria que TODAS las filas cumplan la condicion).
# Correr el notebook dos veces deja exactamente el mismo resultado: idempotencia sin logica extra.

# CELL ********************

def escribir_gold(df, nombre):
    (df.write.format("delta").mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(f"lh_gold.{nombre}"))
    return spark.read.table(f"lh_gold.{nombre}").count()

n_diaria = escribir_gold(df_diaria_tienda, "agg_ventas_diarias_tienda")
n_mensual_tienda = escribir_gold(df_mensual_tienda, "agg_ventas_mensual_tienda")
n_mensual = escribir_gold(df_mensual, "agg_ventas_mensual")

print("agg_ventas_diarias_tienda:", n_diaria, "filas")
print("agg_ventas_mensual_tienda:", n_mensual_tienda, "filas")
print("agg_ventas_mensual:", n_mensual, "filas")

# MARKDOWN ********************

# ### Prueba de idempotencia: correr la escritura otra vez
# 
# El conteo debe ser **identico**. (Contraste con la Sesion 4, Parte 1: un `append` ciego duplica
# en cada corrida.)

# CELL ********************

n_diaria_2 = escribir_gold(df_diaria_tienda, "agg_ventas_diarias_tienda")
n_mensual_tienda_2 = escribir_gold(df_mensual_tienda, "agg_ventas_mensual_tienda")
n_mensual_2 = escribir_gold(df_mensual, "agg_ventas_mensual")

print("agg_ventas_diarias_tienda:", n_diaria, "->", n_diaria_2, "OK" if n_diaria == n_diaria_2 else "CAMBIO")
print("agg_ventas_mensual_tienda:", n_mensual_tienda, "->", n_mensual_tienda_2, "OK" if n_mensual_tienda == n_mensual_tienda_2 else "CAMBIO")
print("agg_ventas_mensual:", n_mensual, "->", n_mensual_2, "OK" if n_mensual == n_mensual_2 else "CAMBIO")

# MARKDOWN ********************

# ---
# ## Paso 6 - Responder las dos preguntas del Comercial
# 
# ### 6.A - "¿Cuanto vendimos ayer por tienda?"
# 
# `current_date()` es la fecha real de hoy, pero el dataset del curso es didactico y sus ventas
# tienen fechas fijas: lo mas probable es que "ayer" **no tenga filas**. En vez de devolver una
# tabla vacia en silencio, la celda lo detecta y usa la ultima fecha con ventas, avisandolo. En
# produccion, con el pipeline maestro de la Sesion 10 corriendo cada noche, "ayer" si tendra datos.
# (Spark SQL no tiene `SYSDATETIME()`: se usa `current_date()` / `current_timestamp()`.)
# 
# Recuerda lo del Paso 1: **por categoria y por canal no hay datos** -- esta respuesta es por
# tienda y fecha.

# CELL ********************

df_diaria_gold = spark.read.table("lh_gold.agg_ventas_diarias_tienda")

fecha_ayer = spark.sql("SELECT date_sub(current_date(), 1) AS d").collect()[0]["d"]
hay_ayer = df_diaria_gold.filter(F.col("fecha") == F.lit(fecha_ayer)).limit(1).count() > 0

if hay_ayer:
    fecha_ref = fecha_ayer
    print("Fecha de referencia = ayer:", fecha_ref)
else:
    fecha_ref = df_diaria_gold.agg(F.max("fecha")).collect()[0][0]
    print("No hay ventas con fecha de ayer (", fecha_ayer, ") -- dataset didactico.")
    print("Se usa la ultima fecha con ventas:", fecha_ref)

(df_diaria_gold.filter(F.col("fecha") == F.lit(fecha_ref))
    .select("fecha", "nombre_tienda", "ciudad_tienda", "num_ventas", "ventas_totales", "ticket_promedio")
    .orderBy(F.col("ventas_totales").desc())
    .show(truncate=False))

# MARKDOWN ********************

# ### 6.B - "¿Cual es el ticket promedio y como evoluciona por mes?"
# 
# Primera celda `%%sql` del notebook: la linea `%%sql` va **siempre primera** en su celda (si hay
# un comentario o una linea en blanco antes, Fabric la trata como Python y falla).

# CELL ********************

# MAGIC %%sql
# MAGIC SELECT anio, mes, nombre_mes, num_ventas, ventas_totales, ticket_promedio,
# MAGIC        ventas_mes_anterior, variacion_ventas_pct
# MAGIC FROM lh_gold.agg_ventas_mensual
# MAGIC ORDER BY anio, mes

# MARKDOWN ********************

# ---
# ## Paso 7 - Preview honesto: ventas en dolares (Finanzas)
# 
# La pregunta de Finanzas es *"¿Cuanto vendimos en dolares con el tipo de cambio **del dia de la
# venta**?"*. Eso exige un join por fecha entre `hechos_ventas` y `silver_tipo_cambio`, y hoy **no
# se puede hacer bien**: `silver_tipo_cambio` tiene una fila por cada corrida del pipeline (la API
# devuelve un solo valor por llamada), asi que casi ninguna fecha de venta tiene su tipo de cambio.
# Ademas se **asume** que `monto` esta en soles (PEN) -- el esquema no trae columna de moneda.
# 
# Lo que si se puede mostrar, claramente rotulado como **aproximacion**, es convertir con el tipo
# de cambio mas reciente. No se materializa ninguna tabla: es solo una vista previa.

# CELL ********************

df_tc = (spark.read.table("lh_silver.silver_tipo_cambio")
    .withColumn("valor", F.col("valor").cast(DoubleType()))
    .withColumn("fecha", F.to_date("fecha")))

print("Fechas distintas con tipo de cambio:", df_tc.select("fecha").distinct().count())

fila_tc = df_tc.orderBy(F.col("fecha").desc()).limit(1).collect()

if len(fila_tc) == 0:
    print("silver_tipo_cambio esta vacia -- no hay preview posible.")
else:
    tc_ultimo = fila_tc[0]["valor"]
    print("Tipo de cambio mas reciente (USD->PEN):", tc_ultimo, "| fecha:", fila_tc[0]["fecha"])
    (spark.read.table("lh_gold.agg_ventas_mensual")
        .withColumn("ventas_usd_aprox", F.round(F.col("ventas_totales") / F.lit(tc_ultimo), 2))
        .select("anio", "mes", "ventas_totales", "ventas_usd_aprox")
        .orderBy("anio", "mes")
        .show())
    print("APROXIMACION: usa el tipo de cambio mas reciente, NO el del dia de cada venta.")

# MARKDOWN ********************

# ---
# ## Paso 8 - Reconciliacion: los agregados suman exactamente lo que `hechos_ventas`
# 
# Un agregado que no cuadra con la tabla de hechos es peor que no tener agregado. Se compara, por
# cada tabla `agg_*`, el total de ventas y el numero de ventas contra `hechos_ventas` (solo filas
# con `sk_tiempo`, porque las demas no entran a ningun dia/mes).

# CELL ********************

h = (spark.read.table("lh_gold.hechos_ventas").filter("sk_tiempo IS NOT NULL")
    .agg(F.count("id_venta").alias("n"), F.sum(F.col("monto").cast(DoubleType())).alias("m"))
    .collect()[0])
print("hechos_ventas (con sk_tiempo): n =", h["n"], "| monto =", h["m"])

for tabla in ["agg_ventas_diarias_tienda", "agg_ventas_mensual_tienda", "agg_ventas_mensual"]:
    a = (spark.read.table(f"lh_gold.{tabla}")
        .agg(F.sum("num_ventas").alias("n"), F.sum("ventas_totales").alias("m"))
        .collect()[0])
    ok = (a["n"] == h["n"]) and (abs(a["m"] - h["m"]) < 0.01)
    print(f"{tabla}: n = {a['n']} | monto = {a['m']} ->", "CUADRA" if ok else "NO CUADRA")

# MARKDOWN ********************

# ---
# ## Siguiente paso (PORTAL de Fabric, sin codigo): el modelo semantico Direct Lake
# 
# Con las tablas materializadas, el resto de la Sesion 9 ocurre en el portal -- ver
# `Lab-Sesion-09-Agregados-Gold-y-Direct-Lake.pdf`, Parte 3:
# 
# 1. Crear el modelo semantico **desde `lh_gold`** (Direct Lake) con `hechos_ventas`, `dim_tiempo`,
#    `dim_tienda`, `dim_cliente` (y `agg_ventas_mensual` para validacion cruzada).
# 2. Relaciones del star schema por `sk_*`; `dim_tiempo` marcada como tabla de fechas.
# 3. Medidas DAX: **Ventas Totales, Nº Ventas, Ticket Promedio, Ventas Mes Anterior**.
# 
# `dim_producto` NO se relaciona hoy: `hechos_ventas` no tiene `sk_producto`.
# 
# ## Checklist de la Sesion 9 - Parte 2 (notebook)
# 
# - [ ] Las 5 tablas del star schema existen en `lh_gold` y el Paso 2 no reporta duplicados ni tipos
#       incompatibles.
# - [ ] Sabes explicar por que "por categoria" y "por canal" **no** se pueden responder hoy.
# - [ ] `agg_ventas_diarias_tienda`, `agg_ventas_mensual_tienda` y `agg_ventas_mensual` existen.
# - [ ] Re-ejecutar la escritura (Paso 5) NO cambia los conteos (idempotente).
# - [ ] La reconciliacion (Paso 8) dice CUADRA en las tres tablas.
# - [ ] Puedes explicar por que `ticket_promedio` no se promedia entre filas.
# - [ ] El preview en dolares esta rotulado como aproximacion (tipo de cambio mas reciente).

# MARKDOWN ********************

# ---
# ## Sesion 11 - Continuacion: contrato de salida para el log
# 
# **Data Wizard Academy - Microsoft Fabric Track**
# 
# Misma idea que en `nb_gold_star_schema`: una celda final con el JSON comun. Ademas, la
# reconciliacion del Paso 8 se convierte en una **guarda**: si algun agregado NO CUADRA contra
# `hechos_ventas`, el notebook falla. En la Sesion 9 solo se imprimia "NO CUADRA" y el pipeline
# seguia en verde; en produccion un agregado que no cuadra no debe llegar al modelo semantico (el
# `Refresh_Modelo` del maestro depende de este notebook, asi que no se ejecuta).

# CELL ********************

import json

tablas_agg = ["agg_ventas_diarias_tienda", "agg_ventas_mensual_tienda", "agg_ventas_mensual"]
h = (spark.read.table("lh_gold.hechos_ventas").filter("sk_tiempo IS NOT NULL")
    .agg(F.count("id_venta").alias("n"), F.sum(F.col("monto").cast(DoubleType())).alias("m"))
    .collect()[0])

no_cuadran = []
filas_agg = 0
for tabla in tablas_agg:
    df_t = spark.read.table(f"lh_gold.{tabla}")
    filas_agg += df_t.count()
    a = df_t.agg(F.sum("num_ventas").alias("n"), F.sum("ventas_totales").alias("m")).collect()[0]
    m_h = h["m"] or 0.0
    m_a = a["m"] or 0.0
    if (a["n"] or 0) != h["n"] or abs(m_a - m_h) >= 0.01:
        no_cuadran.append(tabla)

if no_cuadran:
    raise RuntimeError(f"Agregados que NO CUADRAN con hechos_ventas: {no_cuadran}")

resultado = {
    "notebook": "nb_gold_agregados",
    "filas_leidas": int(h["n"]),
    "filas_escritas": int(filas_agg),
    "filas_cuarentena": 0,
    "reconciliacion": "CUADRA",
}
print(json.dumps(resultado, indent=2))
notebookutils.notebook.exit(json.dumps(resultado))
