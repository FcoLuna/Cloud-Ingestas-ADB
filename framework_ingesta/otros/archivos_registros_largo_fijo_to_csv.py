# Databricks notebook source
# MAGIC %md ###Librerias

# COMMAND ----------

from pyspark.sql.functions import col, expr, trim, lit
import json

# COMMAND ----------

# MAGIC %md ###Variables y Parametros

# COMMAND ----------

# dbutils.widgets.text("largo_campos","2,20,3,11,2,20,3,2,20,10,2,6,6,10,1,5,10,10,5,5,5,4,4,4,4,4,40,20,20,18,8,8,2,8,2,6,8,10,6,10")
# dbutils.widgets.text("encabezado","false")
# dbutils.widgets.text("delimitador",";")
# dbutils.widgets.text("path","abfss://ingestas@stbigdatadev02.dfs.core.windows.net/data/trafico/trafico_mediado_fijo/mediacion/meco_call/stage/")
# dbutils.widgets.text("nombre_archivo","DWH_FU_A_20241211_113410_3150.CDR")
# dbutils.widgets.text("add_variable","{file_date: 20241211,file_number: 3149,file_orig: FU,file_time: 113405}")
# dbutils.widgets.text("encoding_in","")

# COMMAND ----------

print("[INFO] Parametros=====")
encabezado = dbutils.widgets.get("encabezado")
largo_campos = dbutils.widgets.get("largo_campos")
delimitador = dbutils.widgets.get("delimitador")
path = dbutils.widgets.get("path")
nombre_archivo = dbutils.widgets.get("nombre_archivo")
nuevos_campos = dbutils.widgets.get("add_variable")
encoding_in = dbutils.widgets.get("encoding_in")
encoding = dbutils.widgets.get("encoding")


# COMMAND ----------

# MAGIC %md ###Leer archivos sin delimitadores

# COMMAND ----------

# archivo_ori = spark.read.option("encoding", "cp1252").option("header",encabezado).text(f"{path}/{nombre_archivo}")
archivo_ori = spark.read.option("encoding", encoding_in).option("header",encabezado).csv(f"{path}/{nombre_archivo}")

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

structured_df = archivo_ori.select(*field_expressions)


# COMMAND ----------

# DBTITLE 1,Agrega campos
add_variable = {}

try:
  add_variable = json.loads(nuevos_campos)
except:
  add_variable = {}

if len(add_variable.keys()) != 0:
    print("agregando columnas")
    for columna in add_variable.keys():
        structured_df = structured_df.withColumn(columna, lit(add_variable[columna]))
else:
    print("no agrega columnas")

# COMMAND ----------

structured_df.repartition(1)\
    .write\
    .option("header",encabezado)\
    .option("delimiter",delimitador)\
    .option("encoding", encoding)\
    .mode("overwrite")\
    .csv(f"{path}/{nombre_archivo}")

# COMMAND ----------

arch = [archivo for archivo in dbutils.fs.ls(f"{path}/{nombre_archivo}") if archivo.path.endswith(".csv")][0]

dbutils.fs.mv(arch.path, f"{path}/{nombre_archivo.split('.')[0]}.csv")

dbutils.fs.rm(f"{path}/{nombre_archivo}",True)
