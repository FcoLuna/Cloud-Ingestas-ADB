# Databricks notebook source
# MAGIC %md
# MAGIC ### Parsing para Ingesta con campos extras
# MAGIC El archivo que viene con esta ingesta tiene dos campos adicionales nulos haciendo que se quiebre la inserción de los datos en el notebook de validaciones prelanding. Para esto se hace una nueva lectura, previa a validaciones prelanding.

# COMMAND ----------

# cargar librerias
from pyspark.sql.functions import *
from pyspark.sql.types import *
import pyspark.sql.functions as F
import json

# COMMAND ----------

# obtener parámetros
ruta_stage      = dbutils.widgets.get("ruta_stage")
nombre_archivo  = dbutils.widgets.get("nombre_archivo")
delimitador     = dbutils.widgets.get("delimitador")
encabezado      = dbutils.widgets.get("encabezado").lower()
quote           = dbutils.widgets.get("quote")
encoding        = dbutils.widgets.get("encoding")


# COMMAND ----------

#ruta_stage      = "abfss://ingestas@stbigdatadev02.dfs.core.windows.net/data/producto_asignado/parque_movil/control_de_gestion/#bajas_voluntarias_dia/processing/"
#nombre_archivo  = "detalle_bajas_movil_20250106.txt"
#delimitador     = ";"
#encabezado      = "false"
#encoding        = "UTF-8"
#quote           = "\""

# COMMAND ----------

# cargar dataframe. Se 
df = (spark.read
      .option("delimiter", delimitador)
      .option("header", encabezado)
      .option("quote",quote)
      .option("encoding", encoding)
      .csv(f"{ruta_stage}/{nombre_archivo}")
      )

# COMMAND ----------

df_2 = df\
.withColumn('_c0', to_date(lit(F.substring('_c0', 1, 10)),"yyyy-MM-dd"))\
.withColumn('_c1', trim(col('_c1')).cast(LongType()))\
.withColumn('_c2', trim(col('_c2')).cast(LongType()))\
.withColumn('_c3', trim(col('_c3')).cast(StringType()))\
.withColumn('_c4', trim(col('_c4')).cast(StringType()))\
.withColumn('_c5', trim(col('_c5')).cast(StringType()))\
.withColumn('_c6', trim(col('_c6')).cast(StringType()))\
.withColumn('_c7', trim(col('_c7')).cast(StringType()))\
.withColumn('_c8', trim(col('_c8')).cast(StringType()))\
.withColumn('_c9', trim(col('_c9')).cast(StringType()))\
.withColumn('_c10', trim(col('_c10').cast(StringType())))

# COMMAND ----------

# guardar en ruta temporal con otro delimitador
ruta_stage      = ruta_stage.replace('temp','')
(df_2
 .coalesce(1)
 .write.mode("overwrite")
 .format("csv")
 .options(delimiter=delimitador)
 .save(f"{ruta_stage}", header=encabezado.capitalize())
 )

# COMMAND ----------

# mover archivo a ruta stage renombrado 
# nombre_archivo  = nombre_archivo.replace('.gz','')
file = [f.path for f in dbutils.fs.ls(f"{ruta_stage}") if f.name.startswith("part-00000")][0]
dbutils.fs.mv(
  file,
  f"{ruta_stage}/{nombre_archivo}"
  )

# COMMAND ----------

# quitar archivo temporal después de renombrado
dbutils.fs.rm(f"{ruta_stage}/temp/{nombre_archivo}")
