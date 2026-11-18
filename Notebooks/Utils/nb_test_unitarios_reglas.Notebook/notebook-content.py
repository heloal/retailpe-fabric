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

# # nb_test_unitarios_reglas - Sesion 12
# 
# **Data Wizard Academy - Microsoft Fabric Track - RetailPe**
# 
# Pruebas **unitarias**: verifican la LOGICA de las reglas con DataFrames pequenos construidos a
# mano, sin leer ninguna tabla. Responden "¿la funcion hace lo que la regla dice?" en segundos y
# sin depender de los datos de hoy.
# 
# | Tipo de prueba | Que verifica | Cuando corre | Notebook |
# |---|---|---|---|
# | Unitaria | la logica (funciones) con datos inventados | antes de publicar un cambio (Dev, S13) | este |
# | Calidad de datos | los datos reales despues de procesarlos | cada noche, dentro del pipeline | `nb_test_calidad_retailpe` |
# 
# Cada prueba es una funcion `test_...` con `assert`. La ultima celda las ejecuta todas, muestra
# cuales pasaron y **falla** si alguna fallo (para que un pipeline o una persona lo note).
# 
# Las pruebas cubren los casos borde que ya causaron errores en clase: fechas `dd/MM/yyyy`, monto
# exactamente 0, monto nulo y negativo a la vez, duplicados con fecha de actualizacion nula.


# CELL ********************

%run nb_lib_reglas_retailpe

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Configuracion de la prueba
# 
# Mismo ajuste que el notebook job: sin `CORRECTED`, `to_date` puede lanzar `SparkUpgradeException`
# con textos como `2026-08-05 14:22:01`.

# CELL ********************

import datetime as dt
from pyspark.sql.types import StructType, StructField, IntegerType, DoubleType, StringType, TimestampType

spark.conf.set("spark.sql.legacy.timeParserPolicy", "CORRECTED")

ESQUEMA_VENTA = StructType([
    StructField("id_venta", IntegerType()),
    StructField("monto", DoubleType()),
    StructField("fecha_venta", StringType()),
    StructField("fecha_actualizacion", TimestampType()),
])

def ventas(filas):
    """DataFrame de prueba con el esquema de bronze_ventas (solo las columnas que usan las reglas)."""
    return spark.createDataFrame(filas, ESQUEMA_VENTA)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Pruebas de `normalizar_fecha`
# 
# Los tres formatos reales de `dbo.ventas` deben dar la misma fecha; un texto basura debe dar NULL
# (no un error, no una fecha inventada).

# CELL ********************

def test_fecha_tres_formatos():
    df = ventas([(1, 10.0, "2026-08-05", None), (2, 10.0, "05/08/2026", None), (3, 10.0, "2026-08-05 14:22:01", None)])
    r = [x["f"] for x in df.select(normalizar_fecha("fecha_venta").alias("f")).collect()]
    assert r == [dt.date(2026, 8, 5)] * 3, r

def test_fecha_basura_es_null():
    df = ventas([(1, 10.0, "fecha mala", None), (2, 10.0, None, None), (3, 10.0, "  ", None)])
    r = [x["f"] for x in df.select(normalizar_fecha("fecha_venta").alias("f")).collect()]
    assert r == [None, None, None], r

def test_fecha_con_espacios():
    df = ventas([(1, 10.0, "  2026-08-05  ", None)])
    assert df.select(normalizar_fecha("fecha_venta").alias("f")).first()["f"] == dt.date(2026, 8, 5)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Pruebas de R1 / R2 (`etiquetar_r1_r2`, `separar_validas`)
# 
# - Monto 0 es R2 (la regla dice "mayor a cero"); 0.01 es valida.
# - Una venta con monto NULO es R1 aunque tambien tenga otra falla: R1 se evalua primero.
# - Fecha nula con monto negativo: R1 (no R2).

# CELL ********************

def _etiquetas(filas):
    df = ventas(filas).withColumn("fecha_venta", normalizar_fecha("fecha_venta"))
    return {r["id_venta"]: r["motivo_rechazo_codigo"]
            for r in etiquetar_r1_r2(df).select("id_venta", "motivo_rechazo_codigo").collect()}

def test_r2_borde_cero():
    e = _etiquetas([(1, 0.0, "2026-08-05", None), (2, 0.01, "2026-08-05", None), (3, -15.0, "2026-08-05", None)])
    assert e == {1: R2_CODIGO, 2: None, 3: R2_CODIGO}, e

def test_r1_antes_que_r2():
    e = _etiquetas([(1, None, "2026-08-05", None), (2, -5.0, None, None), (3, -5.0, "fecha mala", None)])
    assert e == {1: R1_CODIGO, 2: R1_CODIGO, 3: R1_CODIGO}, e

def test_separar_validas_no_pierde_filas():
    df = ventas([(1, 10.0, "2026-08-05", None), (2, None, "2026-08-05", None), (3, 0.0, "2026-08-05", None)])
    df = df.withColumn("fecha_venta", normalizar_fecha("fecha_venta"))
    validas, rechazadas = separar_validas(etiquetar_r1_r2(df))
    assert validas.count() + rechazadas.count() == 3
    assert "motivo_rechazo_codigo" not in validas.columns
    assert sorted(r["id_venta"] for r in rechazadas.collect()) == [2, 3]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Pruebas de `deduplicar_ultima_version`
# 
# Gana la `fecha_actualizacion` mas reciente; una fecha NULL nunca le gana a una fecha real.

# CELL ********************

def test_dedup_gana_la_ultima():
    t1, t2 = dt.datetime(2026, 8, 5, 10), dt.datetime(2026, 8, 6, 10)
    df = ventas([(1, 100.0, "2026-08-05", t1), (1, 120.0, "2026-08-05", t2), (2, 50.0, "2026-08-05", t1)])
    r = {x["id_venta"]: x["monto"] for x in deduplicar_ultima_version(df, "id_venta", "fecha_actualizacion").collect()}
    assert r == {1: 120.0, 2: 50.0}, r

def test_dedup_null_no_gana():
    df = ventas([(1, 100.0, "2026-08-05", dt.datetime(2026, 8, 5)), (1, 999.0, "2026-08-05", None)])
    r = deduplicar_ultima_version(df, "id_venta", "fecha_actualizacion").first()["monto"]
    assert r == 100.0, r

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Prueba de `resolver_fecha_proceso`
# 
# Texto vacio = hoy en Lima; una fecha valida se respeta; un formato incorrecto debe fallar (es
# mejor un error claro que procesar un dia equivocado).

# CELL ********************

def test_fecha_proceso():
    assert resolver_fecha_proceso("2026-08-15") == dt.date(2026, 8, 15)
    assert resolver_fecha_proceso("") == resolver_fecha_proceso(None)
    try:
        resolver_fecha_proceso("15/08/2026")
        raise AssertionError("debio fallar con un formato distinto de yyyy-MM-dd")
    except ValueError:
        pass

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Ejecutar todas las pruebas
# 
# Recorre todas las funciones `test_...` definidas arriba. Si alguna falla, la celda lanza error al
# final (despues de mostrar el resumen completo), asi se ven TODAS las fallas y no solo la primera.

# CELL ********************

pruebas = {n: f for n, f in sorted(globals().items()) if n.startswith("test_") and callable(f)}
fallidas = []
for nombre, prueba in pruebas.items():
    try:
        prueba()
        print("OK     ", nombre)
    except Exception as e:
        fallidas.append(nombre)
        print("FALLA  ", nombre, "->", repr(e)[:200])

print(f"\n{len(pruebas) - len(fallidas)} de {len(pruebas)} pruebas pasaron")
if fallidas:
    raise AssertionError(f"Pruebas unitarias fallidas: {fallidas}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
