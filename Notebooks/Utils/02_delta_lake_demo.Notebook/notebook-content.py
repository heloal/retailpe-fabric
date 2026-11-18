# Fabric notebook source

# METADATA ********************

# META {
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "ade212a5-a19a-413c-baa7-11fbd7cfbaf8",
# META       "default_lakehouse_name": "lh_bronze",
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

# # Sesion 2 - Delta Lake: Transaction Log, ACID y Time Travel
# 
# **Data Wizard Academy - Microsoft Fabric Track**
# 
# Notebook de demo en vivo. Adjunta el Lakehouse `lh_silver` antes de ejecutar (icono Lakehouses en el panel izquierdo -> Add).
# 
# Duracion en clase: ~24 min (Parte 2) + ~14 min (Parte 3, RESTORE) + lab guiado despues del descanso.

# MARKDOWN ********************

# ---
# ## Parte 2 - El Transaction Log (24 min)
# 
# **Guion:** primero escribimos una tabla Delta de ventas. Luego abrimos su carpeta `_delta_log` para ver el JSON real del commit. Termina con `DESCRIBE HISTORY`, tu caja negra para cualquier incidente.

# MARKDOWN ********************

# ### Paso 1 - Cargar datos reales de ventas (dbo.ventas, via JDBC)

Mismo metodo de conexion que el notebook de Sesion 1: Service Principal (`sp-retailpe-fabric`), no cuenta de dominio. Usamos la BD real `bdacademy` -- ya no hace falta simular nada, la tabla ya tiene 5000 filas con nulos y formatos de fecha mezclados a proposito.

# CELL ********************

from pyspark.sql import functions as F
import notebookutils
from azure.identity import ClientSecretCredential

TENANT_ID = "d1a7fb8d-5ef8-4536-8e37-229e30665b9b"
CLIENT_ID = "eadbac89-5b87-4e5f-ac15-0e44bd7bec3e"
KEYVAULT_URL = "https://kv-retailpe-hector.vault.azure.net/"

client_secret = notebookutils.credentials.getSecret(KEYVAULT_URL, "sp-retailpe-secret")
cred = ClientSecretCredential(tenant_id=TENANT_ID, client_id=CLIENT_ID, client_secret=client_secret)
token = cred.get_token("https://database.windows.net/.default").token

jdbc_url = "jdbc:sqlserver://datawizard.database.windows.net;database=bdacademy;encrypt=true"

df_ventas_raw = (spark.read.format("jdbc")
    .option("url", jdbc_url)
    .option("dbtable", "dbo.ventas")
    .option("accessToken", token)
    .load())

# fecha_venta llega como texto con formato mezclado (dd/MM/yyyy y yyyy-MM-dd) -- normalizamos
# a proposito para poder trabajar con Time Travel sobre una columna fecha real, no texto.
df_ventas = df_ventas_raw.withColumn(
    "fecha_venta",
    F.coalesce(F.to_date("fecha_venta", "yyyy-MM-dd"), F.to_date("fecha_venta", "dd/MM/yyyy"))
)

print("Filas:", df_ventas.count())
df_ventas.show(5)

# MARKDOWN ********************

# ### Alternativa si la conexion JDBC aun no esta lista
# 
# Solo para no bloquear la demo en vivo si falla algo de ultimo minuto (firewall, credenciales). Usa la BD real siempre que puedas.

# CELL ********************

# random.seed(7)
# filas = []
# tiendas = [1, 2, 3, 4, 5]
# for i in range(1, 2001):
#     filas.append((i, __import__("random").choice(tiendas), round(__import__("random").uniform(20, 500), 2), f"2026-08-{(i % 28) + 1:02d}"))
# df_ventas = spark.createDataFrame(filas, ["id_venta", "id_tienda", "monto", "fecha_venta"])
# df_ventas = df_ventas.withColumn("fecha_venta", F.to_date("fecha_venta"))
# print("Filas generadas (fallback):", df_ventas.count())

# MARKDOWN ********************

# ### Paso 2 - Escribir la tabla Delta (commit 0)

# CELL ********************

(df_ventas.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable("silver.demo_ventas"))

print("Filas escritas:", spark.table("silver.demo_ventas").count())

# MARKDOWN ********************

# **Explicar aqui (mientras corre):** *"Lo que acabo de hacer no escribio directo un archivo Parquet suelto. Delta creo una carpeta `_delta_log` con un JSON que describe esta transaccion. Vamos a verlo."*

# MARKDOWN ********************

# ### Paso 3 - Ver la carpeta _delta_log (la demo visual clave)

# CELL ********************

import notebookutils

# Lista los archivos del _delta_log de la tabla
ruta = "Tables/demo_ventas/_delta_log"
for f in notebookutils.fs.ls(ruta):
    print(f.name)

# MARKDOWN ********************

# **Explicar aqui:** *"Ese `00000000000000000000.json` es el commit 0: la creacion de la tabla. Cada operacion nueva (MERGE, DELETE, otro write) agrega un JSON numerado siguiente. La tabla, en cualquier momento, es la SUMA ORDENADA de esos commits."*

# MARKDOWN ********************

# ### Paso 4 - DESCRIBE HISTORY: tu caja negra

# CELL ********************

spark.sql("DESCRIBE HISTORY silver.demo_ventas").show(truncate=False)

# MARKDOWN ********************

# **Explicar aqui:** *"Columnas clave: `version`, `timestamp`, `operation`, `operationMetrics`. Cuando algo sale mal de noche, esta es la PRIMERA consulta que corres. Te dice que operacion toco la tabla, cuantas filas cambio y cuando."*
# 
# > Checkpoint para el aula: *"En una frase, ¿por que Delta es mas confiable que Parquet suelto?"*

# MARKDOWN ********************

# ---
# ## Parte 3 - ACID y Time Travel (26 min, incluye demo de recuperacion 14 min)
# 
# **Guion:** provocamos un fallo real (overwrite con datos incompletos), confirmamos la perdida con un conteo, y recuperamos con `RESTORE TABLE`. Es el momento mas memorable de la sesion — dale drama.

# MARKDOWN ********************

# ### Paso 1 - Registrar el conteo bueno (antes de romper nada)

# CELL ********************

bueno = spark.table("silver.demo_ventas").count()
print("Conteo BUENO:", bueno)

# MARKDOWN ********************

# ### Paso 2 - Romper la tabla (simular el bug)

# CELL ********************

# Simulamos un overwrite accidental con solo una parte de los datos
parcial = spark.table("silver.demo_ventas").limit(10)

(parcial.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable("silver.demo_ventas"))

print("Conteo TRAS EL BUG:", spark.table("silver.demo_ventas").count())

# MARKDOWN ********************

# **Aqui haz una pausa dramatica.** *"Acaban de ver como en produccion, en un segundo, perdimos casi todas las filas. Sin Delta esto seria irrecuperable a menos que tengan un backup de hace horas. Vamos a recuperarlo con el log."*

# MARKDOWN ********************

# ### Paso 3 - Encontrar la version buena

# CELL ********************

spark.sql("DESCRIBE HISTORY silver.demo_ventas").show(truncate=False)
# Busca la fila ANTERIOR al ultimo WRITE (el overwrite del bug). Anota su 'version'.

# MARKDOWN ********************

# ### Paso 4 - RESTORE: recuperar con Time Travel

# CELL ********************

# Reemplaza N por el numero de version que identificaste arriba (la buena, antes del bug)
N = 0  # <-- AJUSTA este numero segun DESCRIBE HISTORY

spark.sql(f"RESTORE TABLE silver.demo_ventas TO VERSION AS OF {N}")

print("Conteo RESTAURADO:", spark.table("silver.demo_ventas").count())
assert spark.table("silver.demo_ventas").count() == bueno, "Algo no cuadra, revisa la version"

# MARKDOWN ********************

# **Cierre de la demo:** *"Recuperamos la tabla sin backups, solo con el Transaction Log. Esto es Time Travel: su red de seguridad, NO su backup. VACUUM eventualmente borra versiones viejas."*
# 
# ### Bonus - Consultar una version anterior sin restaurar (solo lectura)

# CELL ********************

spark.sql("SELECT * FROM silver.demo_ventas VERSION AS OF 0 LIMIT 5").show()
# tambien funciona: TIMESTAMP AS OF 'yyyy-MM-ddTHH:mm:ss'

# MARKDOWN ********************

# ---
# ## Parte 4 - Schema Enforcement y Evolution (18 min)
# 
# **Guion:** mostrar primero que Delta RECHAZA un esquema incompatible (enforcement = proteccion). Luego mostrar como se evoluciona A PROPOSITO con `mergeSchema`.

# MARKDOWN ********************

# ### Paso 1 - Provocar un rechazo de esquema (enforcement en accion)

# CELL ********************

# Intentamos escribir una columna que no existe en la tabla, SIN mergeSchema
df_extra = df_ventas.withColumn("canal", F.lit("web"))

try:
    (df_extra.write
        .format("delta")
        .mode("append")
        .saveAsTable("silver.demo_ventas"))
except Exception as e:
    print("RECHAZADO (esto es bueno):", str(e)[:200])

# MARKDOWN ********************

# **Explicar aqui:** *"Delta rechazo la escritura. Eso NO es un error del sistema, es Schema Enforcement protegiendonos de un cambio de esquema que no decidimos a proposito."*

# MARKDOWN ********************

# ### Paso 2 - Evolucionar con intencion (mergeSchema)

# CELL ********************

(df_extra.write
    .format("delta")
    .mode("append")
    .option("mergeSchema", "true")
    .saveAsTable("silver.demo_ventas"))

spark.table("silver.demo_ventas").printSchema()

# MARKDOWN ********************

# **Cierre:** *"Ahora la columna `canal` existe. Fue una decision explicita con `mergeSchema=true`, no un accidente. Por eso NO dejamos mergeSchema activo por defecto en todos lados."*

# MARKDOWN ********************

# ---
# ## Parte 5 - Mantenimiento y particionamiento (24 min)
# 
# **Guion:** medir el estado ANTES, correr OPTIMIZE con ZORDER, medir DESPUES. Comparar numero de archivos y tiempo.

# MARKDOWN ********************

# ### Paso 1 - Generar mas escrituras pequenas (simular small files)

# CELL ********************

# Simula el problema real: muchas escrituras incrementales chicas generan muchos archivos
for _dia in range(1, 11):
    lote = df_ventas.sample(0.05).withColumn("id_venta", F.col("id_venta") + 100000 * _dia)
    (lote.write.format("delta").mode("append").saveAsTable("silver.demo_ventas"))
print("Escrituras incrementales completadas")

# MARKDOWN ********************

# ### Paso 2 - Medir el estado inicial (archivos + tiempo de consulta)

# CELL ********************

import time

def contar_archivos_parquet():
    return sum(1 for f in notebookutils.fs.ls("Tables/demo_ventas") if f.name.endswith(".parquet"))

print("Archivos Parquet:", contar_archivos_parquet())

t0 = time.time()
spark.sql("SELECT COUNT(*) FROM silver.demo_ventas WHERE id_tienda = 1").show()
print("Tiempo:", round(time.time() - t0, 2), "s")

# MARKDOWN ********************

# ### Paso 3 - OPTIMIZE con ZORDER

# CELL ********************

spark.sql("OPTIMIZE silver.demo_ventas ZORDER BY (fecha_venta, id_tienda)")

# MARKDOWN ********************

# ### Paso 4 - Volver a medir y comparar

# CELL ********************

print("Archivos Parquet DESPUES:", contar_archivos_parquet())

t0 = time.time()
spark.sql("SELECT COUNT(*) FROM silver.demo_ventas WHERE id_tienda = 1").show()
print("Tiempo DESPUES:", round(time.time() - t0, 2), "s")

# MARKDOWN ********************

# **Cierre de la sesion:** *"Menos archivos, y la consulta filtrada por tienda y fecha suele ser mas rapida gracias al ZORDER. En Bloque 4 (Sesion 12) profundizamos performance. Recuerden: VACUUM libera espacio pero borra el Time Travel mas antiguo que la retencion."*
# 
# ```sql
# -- No ejecutar en la demo (borra historial). Solo mostrar el comando:
# VACUUM silver.demo_ventas RETAIN 168 HOURS;
# ```

# MARKDOWN ********************

# ---
# ## Checklist de esta demo
# 
# - [ ] Tabla `silver.demo_ventas` creada y su `_delta_log` inspeccionado
# - [ ] Fallo provocado + recuperado con `RESTORE TABLE ... VERSION AS OF`
# - [ ] Rechazo de esquema mostrado + evolucionado con `mergeSchema`
# - [ ] `OPTIMIZE ... ZORDER BY` ejecutado y medido antes/despues
# 
# **Commit al repo:** `git commit -m "S2: Delta - Transaction Log, Time Travel, OPTIMIZE"`
