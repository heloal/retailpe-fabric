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

# # nb_smoke_test_ambiente - Sesion 13 (verificacion post-despliegue)
# 
# **Data Wizard Academy - Microsoft Fabric Track - RetailPe**
# 
# Despues de cada despliegue a produccion (deployment pipeline Dev -> Prod) se corre este notebook
# EN EL WORKSPACE DE PROD. Responde, en un minuto, tres preguntas que no se pueden dejar a la
# intuicion:
# 
# 1. **¿Estoy en el ambiente correcto?** El notebook lee el nombre de su workspace y lo compara con
#    lo esperado. Un notebook de Prod que lee tablas de Dev es el error mas caro de un despliegue.
# 2. **¿Existen todos los objetos que el pipeline nocturno necesita?** Tablas de Bronze, Silver,
#    Gold y los resultados de calidad. Los datos NO viajan con el despliegue (solo el codigo).
# 3. **¿Tienen datos?** Antes de la primera carga en Prod es normal que esten vacias: el notebook lo
#    distingue con el parametro `primera_vez`.
# 
# No escribe nada. Es seguro correrlo las veces que sea necesario.
# 
# **Smoke test** = prueba de humo: no prueba que todo este correcto (eso lo hace
# `nb_test_calidad_retailpe` cada noche), prueba que nada esta **obviamente roto** antes de
# encender el schedule.

# MARKDOWN ********************

# ## Celda de parametros (Toggle parameter cell)

# PARAMETERS CELL ********************

ambiente_esperado = "PROD"
primera_vez = False

# MARKDOWN ********************

# ## Paso 1 - ¿En que workspace estoy?
# 
# `notebookutils.runtime.context` expone el contexto de ejecucion (workspace, notebook). Si por
# cualquier razon la clave no existe en tu runtime, la celda no falla: avisa y continua (el paso 2
# igual es util).

# CELL ********************

import json
from pyspark.sql import functions as F

try:
    ctx = notebookutils.runtime.context
    workspace = ctx.get("currentWorkspaceName", "")
    print("Workspace:", workspace, "| notebook:", ctx.get("currentNotebookName", ""))
except Exception as e:
    workspace = ""
    print("No se pudo leer notebookutils.runtime.context:", repr(e)[:150])

if workspace and ambiente_esperado and ambiente_esperado.upper() not in workspace.upper():
    raise RuntimeError(f"Este notebook corre en '{workspace}', pero se esperaba un workspace de {ambiente_esperado}. "
                       "Revisa el deployment pipeline / el workspace antes de seguir.")
print("Ambiente OK" if workspace else "Ambiente NO verificado (sin contexto)")

# MARKDOWN ********************

# ## Paso 2 - Objetos que el pipeline nocturno necesita
# 
# Tablas que deben existir para que `pl_maestro_retailpe` corra de punta a punta. Las de Bronze las
# crea `pl_bronze_metadata` en su primera corrida; las de Silver/Gold, los notebooks. Por eso, con
# `primera_vez = True`, que falten es una ADVERTENCIA y no un error.

# CELL ********************

ESPERADAS = {
    "lh_bronze": ["bronze_ventas", "bronze_clientes", "bronze_tiendas", "bronze_productos", "bronze_tipo_cambio"],
    "lh_silver": ["silver_ventas", "silver_ventas_cuarentena", "silver_clientes", "silver_productos",
                  "silver_tipo_cambio", "silver_tiendas"],
    "lh_gold": ["dim_tiempo", "dim_cliente", "dim_producto", "dim_tienda", "hechos_ventas",
                "agg_ventas_diarias_tienda", "agg_ventas_mensual_tienda", "agg_ventas_mensual", "dq_resultados"],
}

filas = []
for lh, tablas in ESPERADAS.items():
    for t in tablas:
        nombre = f"{lh}.{t}"
        try:
            existe = spark.catalog.tableExists(nombre)
        except Exception:
            existe = False
        n = spark.read.table(nombre).count() if existe else None
        filas.append((nombre, existe, n))
        print(f"{'OK   ' if existe and n else 'FALTA' if not existe else 'VACIA'}  {nombre:45s} filas={n}")

# MARKDOWN ********************

# ## Paso 3 - Decision
# 
# - Despliegue de un cambio (Prod ya tenia datos): cualquier tabla faltante o vacia = **error**. El
#   schedule no se enciende hasta resolverlo.
# - Primera vez (`primera_vez = True`): se informa lo que falta; despues de la primera corrida del
#   orquestador con `fecha_inicio_backfill`, se vuelve a correr con `primera_vez = False`.

# CELL ********************

faltan = [n for n, e, _ in filas if not e]
vacias = [n for n, e, k in filas if e and not k]
print(f"\nTablas: {len(filas)} | faltan: {len(faltan)} | vacias: {len(vacias)}")

if not primera_vez and (faltan or vacias):
    raise RuntimeError(f"Smoke test FALLIDO. Faltan: {faltan} | Vacias: {vacias}")

resultado = {
    "notebook": "nb_smoke_test_ambiente",
    "workspace": workspace,
    "filas_leidas": len(filas),
    "filas_escritas": len(filas) - len(faltan) - len(vacias),
    "filas_cuarentena": len(faltan) + len(vacias),
    "primera_vez": bool(primera_vez),
}
print(json.dumps(resultado, indent=2))
notebookutils.notebook.exit(json.dumps(resultado))
