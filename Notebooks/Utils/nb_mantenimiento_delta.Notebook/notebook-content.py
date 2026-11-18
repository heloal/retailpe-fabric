# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
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

# # nb_mantenimiento_delta - Sesion 12 (performance)
# 
# **Data Wizard Academy - Microsoft Fabric Track - RetailPe**
# 
# Cada noche, los `MERGE` de Silver y los `overwrite` de Gold escriben archivos Parquet nuevos.
# Con el tiempo una tabla termina con cientos de archivos pequenos: cada consulta (Spark, SQL
# endpoint, Direct Lake) tiene que abrirlos todos. Este notebook hace el mantenimiento semanal:
# 
# 1. **Mide** cada tabla (`DESCRIBE DETAIL`: numero de archivos y tamano).
# 2. **OPTIMIZE** (compacta archivos pequenos) y opcionalmente **V-Order** (layout de lectura).
# 3. **VACUUM** (borra archivos que ya no usa ninguna version, respetando la retencion).
# 4. **Mide** otra vez y devuelve el contrato de salida.
# 
# Lo ejecuta un pipeline aparte, `pl_mantenimiento_semanal` (domingo 05:00), NO el maestro
# nocturno: el mantenimiento compite por capacidad y no tiene que correr cada noche.
# 
# **Ojo con VACUUM:** borra versiones antiguas. Con `retencion_horas = 168` (7 dias, el minimo por
# defecto de Delta) el time travel de la Sesion 2 sigue funcionando para la ultima semana. No
# bajes la retencion para "ahorrar espacio": romperias el time travel y lecturas en curso.


# MARKDOWN ********************

# ## Celda de parametros (Toggle parameter cell)

# PARAMETERS CELL ********************

id_ejecucion = "manual"
usar_vorder = True
ejecutar_vacuum = True
retencion_horas = 168

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Paso 0 - Tablas a mantener
# 
# Solo las tablas que el pipeline reescribe cada noche. Bronze se excluye: `pl_bronze_metadata`
# hace overwrite completo (cada noche es una sola escritura), su problema no son archivos pequenos.

# CELL ********************

import json, time

TABLAS = [
    "lh_silver.silver_ventas", "lh_silver.silver_ventas_cuarentena", "lh_silver.silver_clientes",
    "lh_silver.silver_productos", "lh_silver.silver_tipo_cambio",
    "lh_gold.hechos_ventas", "lh_gold.dim_cliente", "lh_gold.dim_tienda", "lh_gold.dim_producto",
    "lh_gold.agg_ventas_diarias_tienda", "lh_gold.agg_ventas_mensual_tienda", "lh_gold.agg_ventas_mensual",
]
TABLAS = [t for t in TABLAS if spark.catalog.tableExists(t)]
print(len(TABLAS), "tablas existentes para mantener")

if int(retencion_horas) < 168:
    raise ValueError("retencion_horas < 168: se romperia el time travel de la ultima semana. No se permite aqui.")

def detalle(tabla):
    d = spark.sql(f"DESCRIBE DETAIL {tabla}").select("numFiles", "sizeInBytes").first()
    return int(d["numFiles"]), int(d["sizeInBytes"])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Paso 1 - Medir ANTES
# 
# Regla de performance: **medir antes de optimizar**. Si una tabla tiene 1 archivo, OPTIMIZE no
# hace nada util.

# CELL ********************

antes = {t: detalle(t) for t in TABLAS}
for t, (n, b) in antes.items():
    print(f"{t:45s} archivos={n:5d}  MB={b / 1024 / 1024:8.2f}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Paso 2 - OPTIMIZE (+ V-Order) y VACUUM
# 
# - `OPTIMIZE tabla VORDER`: compacta y reescribe con V-Order (sintaxis de Fabric). V-Order mejora
#   lecturas (Direct Lake, SQL endpoint) a cambio de escrituras algo mas lentas; en workspaces nuevos
#   viene **desactivado por defecto** (Microsoft Learn, "Delta optimization and V-Order"). Gold es
#   de lectura intensiva: es donde mas conviene.
# - `VACUUM tabla RETAIN 168 HOURS`: borra archivos fuera de la retencion.
# 
# Fuera de Fabric (Delta open source) la palabra `VORDER` no existe: por eso es un parametro.

# CELL ********************

tiempos = {}
for t in TABLAS:
    t0 = time.time()
    spark.sql(f"OPTIMIZE {t}" + (" VORDER" if usar_vorder else ""))
    if ejecutar_vacuum:
        spark.sql(f"VACUUM {t} RETAIN {int(retencion_horas)} HOURS")
    tiempos[t] = round(time.time() - t0, 1)
    print(f"{t:45s} mantenida en {tiempos[t]} s")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Paso 3 - Medir DESPUES
# 
# VACUUM no cambia `numFiles` (cuenta solo los archivos de la version actual); lo que libera es
# espacio en OneLake de versiones viejas. OPTIMIZE es el que reduce `numFiles`.

# CELL ********************

despues = {t: detalle(t) for t in TABLAS}
for t in TABLAS:
    print(f"{t:45s} archivos {antes[t][0]:5d} -> {despues[t][0]:5d}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Paso 4 - Historia de la tabla: metricas sin `count()`
# 
# Cada operacion Delta guarda metricas en su historial (`operationMetrics`). Para tablas grandes,
# leer "cuantas filas inserto/actualizo el ultimo MERGE" del historial es gratis; hacer `count()`
# recorre la tabla entera. Los notebooks job usan `count()` porque el volumen del curso es pequeno;
# con ~2M ventas/mes es lo primero que se cambia.

# CELL ********************

hist = (spark.sql("DESCRIBE HISTORY lh_silver.silver_ventas")
    .select("version", "timestamp", "operation", "operationMetrics")
    .orderBy("version", ascending=False).limit(5))
hist.show(truncate=80)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Paso 5 - Contrato de salida (archivos antes / despues)

# CELL ********************

resultado = {
    "notebook": "nb_mantenimiento_delta",
    "filas_leidas": sum(n for n, _ in antes.values()),
    "filas_escritas": sum(n for n, _ in despues.values()),
    "filas_cuarentena": 0,
    "tablas": len(TABLAS),
    "vorder": bool(usar_vorder),
}
print(json.dumps(resultado, indent=2))
notebookutils.notebook.exit(json.dumps(resultado))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
