# Databricks notebook source
from pyspark.sql.functions import lit, unix_timestamp, from_unixtime

# COMMAND ----------

ruta_stage = dbutils.widgets.get("ruta_stage")
ruta_destino = dbutils.widgets.get("ruta_destino")
nombre_archivo = dbutils.widgets.get("nombre_archivo")
delimitador = dbutils.widgets.get("delimitador")
encabezado = dbutils.widgets.get("encabezado").lower()
encoding = dbutils.widgets.get("encoding")
quote = dbutils.widgets.get("quote")
original_file_date = dbutils.widgets.get("original_file_date")
formato_entrada = dbutils.widgets.get("formato_entrada")
formato_salida = dbutils.widgets.get("formato_salida")

if formato_salida == "":
    formato_salida = "yyyy-MM-dd"

df = spark.read.option("delimiter", delimitador).option("header", encabezado).option("quote", quote).option("encoding", encoding).csv(f"{ruta_stage}/{nombre_archivo}")
display(df)

df = df.withColumn("partition_date", from_unixtime(unix_timestamp(lit(original_file_date), formato_entrada)).cast("date"))
dbutils.fs.rm(f"{ruta_stage}/{nombre_archivo}", recurse=True)
display(df)

df.coalesce(1).write.format("csv").option("header", encabezado).option("delimiter", delimitador).mode("overwrite").save(f"{ruta_stage}/{nombre_archivo}")

