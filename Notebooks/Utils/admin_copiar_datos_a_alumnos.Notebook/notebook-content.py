# Fabric notebook source

# METADATA ********************

# META {
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "ade212a5-a19a-413c-baa7-11fbd7cfbaf8",
# META       "default_lakehouse_name": "lh_bronze",
# META       "default_lakehouse_workspace_id": "bfa889cd-7370-4fd7-b815-ade98d5b6d43"
# META     }
# META   }
# META }

# MARKDOWN ********************

# # admin_copiar_datos_a_alumnos (solo del docente, NO se clona)
# 
# Copia las tablas Delta de Bronze, Silver y Gold de esta plantilla a los lakehouses de los
# workspaces de los alumnos (via OneLake / abfss). Deja a cada alumno con exactamente los mismos
# datos del final de la Sesion 8 sin usar la clave de la API. Overwrite completo: re-ejecutar es seguro.

# PARAMETERS CELL ********************

destinos_json = "[]"

# CELL ********************

import json
SRC_WS = "bfa889cd-7370-4fd7-b815-ade98d5b6d43"
SRC = {"lh_bronze": "ade212a5-a19a-413c-baa7-11fbd7cfbaf8", "lh_silver": "0479cc73-85db-4b7e-98c6-f4e0d80d2c8e", "lh_gold": "f3a7d915-6de2-4c17-b508-80edd65afbbe"}
TABLAS = {
    "lh_bronze": ["bronze_ventas", "bronze_clientes", "bronze_tiendas", "bronze_productos", "bronze_tipo_cambio"],
    "lh_silver": ["silver_ventas", "silver_ventas_cuarentena", "silver_clientes", "silver_productos",
                  "silver_tipo_cambio", "silver_tiendas"],
    "lh_gold": ["dim_tiempo", "dim_cliente", "dim_producto", "dim_tienda", "hechos_ventas"],
}
ruta = lambda ws, lh, t: f"abfss://{ws}@onelake.dfs.fabric.microsoft.com/{lh}/Tables/{t}"
resumen = {}
for d in json.loads(destinos_json):
    n = 0
    for capa, tablas in TABLAS.items():
        for t in tablas:
            df = spark.read.format("delta").load(ruta(SRC_WS, SRC[capa], t))
            (df.write.format("delta").mode("overwrite").option("overwriteSchema", "true")
               .save(ruta(d["ws"], d[capa], t)))
            n += 1
    resumen[d["usuario"]] = n
    print(d["usuario"], "->", n, "tablas copiadas")
print(json.dumps(resumen))
