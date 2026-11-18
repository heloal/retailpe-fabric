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

# # nb_test_calidad_retailpe - Sesion 12 (pruebas de calidad de datos)
# 
# **Data Wizard Academy - Microsoft Fabric Track - RetailPe**
# 
# Pruebas sobre los **datos reales** despues de que el pipeline los proceso. Corre dentro de
# `pl_maestro_retailpe`, despues de `nb_gold_agregados` y ANTES de `Refresh_Modelo`: si una prueba
# **CRITICA** falla, el notebook falla, el modelo no se refresca y el orquestador de la Sesion 11
# manda la alerta [FALLO]. Las de **ADVERTENCIA** se registran pero no detienen nada.
# 
# Cada prueba queda guardada en `lh_gold.dq_resultados` (una fila por prueba por corrida), para ver
# su historia y llevarla a Power BI.
# 
# | # | Prueba | Regla | Severidad |
# |---|---|---|---|
# | 1 | `id_venta` unico en silver_ventas | MERGE / R6 | CRITICO |
# | 2 | silver_ventas sin monto ni fecha nulos | R1 | CRITICO |
# | 3 | silver_ventas sin montos <= 0 | R2 | CRITICO |
# | 4 | ninguna venta a la vez en Silver y en cuarentena | graduacion S6/S10 | ADVERTENCIA |
# | 5 | una sola version vigente por cliente | R5 | CRITICO |
# | 6 | vigencias SCD2 coherentes (inicio <= fin) | R5 | CRITICO |
# | 7 | ningun producto sin categoria | R3 | CRITICO |
# | 8 | grano de hechos_ventas: id_venta unico | R6 | CRITICO |
# | 9 | hechos_ventas tiene las mismas ventas que silver_ventas | reconciliacion | CRITICO |
# | 10 | sk unicas en las 4 dimensiones | Direct Lake / S9 | CRITICO |
# | 11 | % de ventas con tienda desconocida <= umbral | R4 | ADVERTENCIA |
# | 12 | frescura: hay ventas recientes | observabilidad | ADVERTENCIA |


# MARKDOWN ********************

# ## Celda de parametros (Toggle parameter cell)

# PARAMETERS CELL ********************

fecha_proceso = ""
id_ejecucion = "manual"
pct_max_tienda_desconocida = 3.0
dias_frescura = 3

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Paso 0 - Configuracion y "mini framework" de pruebas
# 
# `prueba()` registra el resultado de una verificacion: nombre, regla, severidad, valor observado,
# valor esperado y si paso. Es deliberadamente simple: no hace falta una libreria externa para
# empezar a probar datos (Great Expectations y similares se mencionan en la teoria, no se instalan
# hoy).

# CELL ********************

import json
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from pyspark.sql import functions as F

LH_SILVER = "lh_silver"
LH_GOLD = "lh_gold"
T_RESULTADOS = f"{LH_GOLD}.dq_resultados"

if str(fecha_proceso).strip() == "":
    f_proceso = datetime.now(ZoneInfo("America/Lima")).date()
else:
    f_proceso = datetime.strptime(str(fecha_proceso).strip(), "%Y-%m-%d").date()

resultados = []

def prueba(numero, nombre, regla, severidad, observado, esperado, ok):
    resultados.append({
        "numero": int(numero), "prueba": nombre, "regla": regla, "severidad": severidad,
        "valor_observado": str(observado), "valor_esperado": str(esperado), "ok": bool(ok)})
    print(("OK    " if ok else "FALLA ") + f"[{severidad}] {numero:>2}. {nombre}: observado={observado} esperado={esperado}")

ventas = spark.read.table(f"{LH_SILVER}.silver_ventas")
cuarentena = spark.read.table(f"{LH_SILVER}.silver_ventas_cuarentena")
clientes = spark.read.table(f"{LH_SILVER}.silver_clientes")
productos = spark.read.table(f"{LH_SILVER}.silver_productos")
hechos = spark.read.table(f"{LH_GOLD}.hechos_ventas")
print("id_ejecucion:", id_ejecucion, "| fecha_proceso:", f_proceso)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Paso 1 - Silver: ventas (pruebas 1 a 4)
# 
# Si R1 o R2 fallan aqui, significa que una venta invalida esquivo la cuarentena: hay un bug en el
# notebook job (o alguien escribio en Silver por fuera del pipeline).

# CELL ********************

n_ventas = ventas.count()
dup = n_ventas - ventas.select("id_venta").distinct().count()
prueba(1, "id_venta unico en silver_ventas", "MERGE/R6", "CRITICO", dup, 0, dup == 0)

r1 = ventas.filter(F.col("monto").isNull() | F.col("fecha_venta").isNull()).count()
prueba(2, "silver_ventas sin monto ni fecha nulos", "R1", "CRITICO", r1, 0, r1 == 0)

r2 = ventas.filter(F.col("monto") <= 0).count()
prueba(3, "silver_ventas sin montos <= 0", "R2", "CRITICO", r2, 0, r2 == 0)

ids_c = cuarentena.select(F.col("id_venta").cast("string").alias("k")).distinct()
ambos = ventas.select(F.col("id_venta").cast("string").alias("k")).distinct().join(ids_c, "k").count()
prueba(4, "ninguna venta en Silver y en cuarentena a la vez", "Graduacion", "ADVERTENCIA", ambos, 0, ambos == 0)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Paso 2 - Silver: clientes y productos (pruebas 5 a 7)

# CELL ********************

vig_dup = (clientes.filter("es_actual = true").groupBy("id_cliente").count()
    .filter("count > 1").count())
prueba(5, "una sola version vigente por cliente", "R5", "CRITICO", vig_dup, 0, vig_dup == 0)

vig_mal = clientes.filter(F.col("fecha_inicio_vigencia") > F.col("fecha_fin_vigencia")).count()
prueba(6, "vigencias SCD2 coherentes (inicio <= fin)", "R5", "CRITICO", vig_mal, 0, vig_mal == 0)

sin_cat = productos.filter(F.col("categoria").isNull() | (F.trim(F.col("categoria")) == "")).count()
prueba(7, "ningun producto sin categoria", "R3", "CRITICO", sin_cat, 0, sin_cat == 0)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Paso 3 - Gold (pruebas 8 a 11)
# 
# La prueba 9 compara conjuntos de `id_venta` (no solo conteos): dos tablas pueden tener el mismo
# numero de filas y distintas ventas. La 11 usa un umbral porque R4 decidio, en la Sesion 8,
# conservar las ventas con tienda desconocida (sk = -1): no es un error, pero si crece es una senal.

# CELL ********************

n_hechos = hechos.count()
dup_h = n_hechos - hechos.select("id_venta").distinct().count()
prueba(8, "grano de hechos_ventas: id_venta unico", "R6", "CRITICO", dup_h, 0, dup_h == 0)

ids_s = ventas.select(F.col("id_venta").cast("string").alias("k"))
ids_h = hechos.select(F.col("id_venta").cast("string").alias("k"))
dif = ids_s.exceptAll(ids_h).count() + ids_h.exceptAll(ids_s).count()
prueba(9, "hechos_ventas = silver_ventas (mismas ventas)", "Reconciliacion", "CRITICO", dif, 0, dif == 0)

sk_dup = 0
for tabla, sk in [("dim_tiempo", "sk_tiempo"), ("dim_tienda", "sk_tienda"),
                  ("dim_cliente", "sk_cliente"), ("dim_producto", "sk_producto")]:
    d = spark.read.table(f"{LH_GOLD}.{tabla}")
    sk_dup += d.count() - d.select(sk).distinct().count()
prueba(10, "sk unicas en las 4 dimensiones", "Direct Lake", "CRITICO", sk_dup, 0, sk_dup == 0)

desconocidas = hechos.filter("sk_tienda = -1").count()
pct = round(desconocidas * 100.0 / n_hechos, 2) if n_hechos else 0.0
prueba(11, "% ventas con tienda desconocida", "R4", "ADVERTENCIA", pct, f"<= {pct_max_tienda_desconocida}",
       pct <= float(pct_max_tienda_desconocida))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Paso 4 - Frescura (prueba 12)
# 
# ¿La venta mas reciente de Silver tiene como mucho `dias_frescura` dias respecto a `fecha_proceso`?
# Con el dataset del curso (fechas fijas de agosto 2026) esta prueba **va a fallar** y por eso es
# ADVERTENCIA: no detiene el pipeline, pero queda registrada. En produccion, con datos que llegan
# cada dia, es la alarma de "las tiendas dejaron de enviar ventas".

# CELL ********************

max_fecha = ventas.agg(F.max("fecha_venta")).collect()[0][0]
limite = f_proceso - timedelta(days=int(dias_frescura))
fresco = (max_fecha is not None) and (max_fecha >= limite)
prueba(12, "frescura: venta mas reciente", "Observabilidad", "ADVERTENCIA", max_fecha, f">= {limite}", fresco)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Paso 5 - Guardar resultados en `lh_gold.dq_resultados` (idempotente)
# 
# Se reemplazan las filas de ESTA corrida (`replaceWhere` por `id_ejecucion`): si el pipeline
# reintenta el notebook, no se duplican resultados. `replaceWhere` funciona aqui porque TODAS las
# filas escritas tienen el mismo `id_ejecucion` (la condicion que exige `replaceWhere`).

# CELL ********************

df_res = (spark.createDataFrame(resultados)
    .withColumn("id_ejecucion", F.lit(str(id_ejecucion)))
    .withColumn("fecha_proceso", F.lit(str(f_proceso)))
    .withColumn("ejecutado_en", F.current_timestamp())
    .select("id_ejecucion", "fecha_proceso", "numero", "prueba", "regla", "severidad",
            "valor_observado", "valor_esperado", "ok", "ejecutado_en"))

if spark.catalog.tableExists(T_RESULTADOS):
    (df_res.write.format("delta").mode("overwrite")
        .option("replaceWhere", f"id_ejecucion = '{id_ejecucion}'")
        .saveAsTable(T_RESULTADOS))
else:
    df_res.write.format("delta").mode("overwrite").saveAsTable(T_RESULTADOS)

print("Filas de esta corrida en dq_resultados:",
      spark.read.table(T_RESULTADOS).filter(F.col("id_ejecucion") == str(id_ejecucion)).count())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Paso 6 - Decision y contrato de salida
# 
# Contrato de la Sesion 10 aplicado a pruebas: `filas_leidas` = pruebas ejecutadas,
# `filas_escritas` = pruebas OK, `filas_cuarentena` = pruebas fallidas (de cualquier severidad).
# Si falla alguna CRITICA el notebook lanza error **despues** de guardar los resultados: el detalle
# queda en `dq_resultados` aunque el pipeline se detenga.

# CELL ********************

criticas = [r["prueba"] for r in resultados if not r["ok"] and r["severidad"] == "CRITICO"]
advertencias = [r["prueba"] for r in resultados if not r["ok"] and r["severidad"] == "ADVERTENCIA"]
print(f"\nPruebas: {len(resultados)} | criticas fallidas: {len(criticas)} | advertencias: {len(advertencias)}")

if criticas:
    raise RuntimeError(f"Pruebas CRITICAS de calidad fallidas: {criticas}")

resultado = {
    "notebook": "nb_test_calidad_retailpe",
    "fecha_proceso": str(f_proceso),
    "filas_leidas": len(resultados),
    "filas_escritas": len(resultados) - len(advertencias),
    "filas_cuarentena": len(advertencias),
    "advertencias": advertencias,
}
print(json.dumps(resultado, indent=2))
notebookutils.notebook.exit(json.dumps(resultado))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
