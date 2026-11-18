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
# META       "default_lakehouse_workspace_id": "bfa889cd-7370-4fd7-b815-ade98d5b6d43"
# META     }
# META   }
# META }

# MARKDOWN ********************

# # Sesion 12 - romper un dato a proposito (SOLO DEV)

# CELL ********************

vid = spark.sql("SELECT MIN(id_venta) AS v FROM lh_silver.silver_ventas").first()["v"]
lit = f"'{vid}'" if isinstance(vid, str) else str(vid)
spark.sql(f"UPDATE lh_silver.silver_ventas SET monto = -1 WHERE id_venta = {lit}")
print("Venta rota a proposito (monto = -1):", vid)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
