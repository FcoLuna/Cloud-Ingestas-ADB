# Databricks notebook source
# MAGIC %md ###Librerias

# COMMAND ----------

from pyspark.sql.functions import col, expr, trim, lit, split
import json
from datetime import datetime, timedelta
import re

# COMMAND ----------

# MAGIC %md ###Variables y Parametros

# COMMAND ----------

# dbutils.widgets.text("path","abfss://bigdataprd@stbigdataprd02.dfs.core.windows.net/data/trafico/trafico_mediado_fijo/mediacion/meco_call/stage/")
# dbutils.widgets.text("delimitador",";")
# dbutils.widgets.text("encabezado","false")
# dbutils.widgets.text("largo_campos","2,20,3,11,2,20,3,2,20,10,2,6,6,10,1,5,10,10,5,5,5,4,4,4,4,4,40,20,20,18,8,8,2,8,2,6,8,10,6,10")
# dbutils.widgets.text("encoding_in","")
# dbutils.widgets.text("encoding","")
# dbutils.widgets.text("id_ejecucion","20241812113300")
# dbutils.widgets.text("patron_archivos","^DWH_(FU|CRAL)_A_(20250114|20250115)_\\d+_\\d+\\.CDR$")

# COMMAND ----------

print("[INFO] Parametros=====")
path = dbutils.widgets.get("path")
delimitador = dbutils.widgets.get("delimitador")
encabezado = dbutils.widgets.get("encabezado")
largo_campos = dbutils.widgets.get("largo_campos")
encoding_in = dbutils.widgets.get("encoding_in")
encoding = dbutils.widgets.get("encoding")
id_ejecucion = dbutils.widgets.get("id_ejecucion")
patron_archivos = dbutils.widgets.get("patron_archivos")

# COMMAND ----------

patron = re.compile(patron_archivos)

archivos = dbutils.fs.ls(path)

archivos_match = []
archivos_no_match = []

for archivo in archivos:
    if patron.match(archivo.name):
        archivos_match.append(archivo.path)
    else:
        archivos_no_match.append(archivo)

for archivo in archivos_no_match:
    try:
        if archivo.name.startswith("DWH") and not archivo.name.endswith(".csv"):
            dbutils.fs.mv(archivo.path, f"{path}/NO_PROCESADO_{archivo.name}")

    except Exception as e:
        print(f"Error al cambiar nombre del archivo {archivo.path}: {e}")

if not archivos_match:
    dbutils.notebook.exit("No se encontraron archivos que coincidan con el patrón.")


# COMMAND ----------

# MAGIC %md ###Leer archivos sin delimitadores

# COMMAND ----------

print(f"Archivos seleccionados: {archivos_match}")
if archivos_match:
    archivo_ori = spark.read.option("encoding", encoding_in).option("header",encabezado).csv(archivos_match)
else:
    print("No se encontraron archivos para las fechas especificadas.")

# COMMAND ----------

# MAGIC %md ###Separar campos por posiciciones

# COMMAND ----------

field_lengths = [int(numero.strip()) for numero in largo_campos.split(",")]

start = 0
field_expressions = []
for i, length in enumerate(field_lengths):
    field_expressions.append(
        trim(expr(f"substring(_c0, {start + 1}, {length})")).alias(f"field_{i+1}")
    )
    start += length

structured_df = archivo_ori.select(*field_expressions)\
    .withColumn("file_orig",split(col("_metadata.file_name"), "_").getItem(1))\
    .withColumn("file_date",split(col("_metadata.file_name"), "_").getItem(3))\
    .withColumn("file_time",split(col("_metadata.file_name"), "_").getItem(4))\
    .withColumn("file_number",split(split(col("_metadata.file_name"), "_").getItem(5),"\\.").getItem(0))\
    .withColumn("filename_spark",col("_metadata.file_name"))


# COMMAND ----------

fechas_unicas = [row[0] for row in structured_df.select("file_date").distinct().collect()]

for fecha in fechas_unicas:
    df_filtrado = structured_df.filter(col("file_date") == fecha)
    
    carpeta_tmp = f"{path}/DWH_{fecha}_{id_ejecucion}"

    df_filtrado.repartition(1)\
        .write\
        .option("header",encabezado)\
        .option("delimiter",delimitador)\
        .option("encoding", encoding)\
        .mode("overwrite")\
        .csv(f"{carpeta_tmp}")

    arch = [archivo for archivo in dbutils.fs.ls(f"{carpeta_tmp}") if archivo.path.endswith(".csv")][0]
    dbutils.fs.mv(arch.path, f"{carpeta_tmp}.csv")
    dbutils.fs.rm(f"{carpeta_tmp}",True)


# COMMAND ----------

# DBTITLE 1,Borra archivos  CDR unidos
for archivo in archivos_match:
    try:
        dbutils.fs.rm(archivo, recurse=False)
        print(f"Archivo eliminado: {archivo}")
    except Exception as e:
        print(f"Error al eliminar el archivo {archivo}: {e}")
