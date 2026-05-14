# Databricks notebook source
# MAGIC %md
# MAGIC ### Parsing para Ingesta DECO_ID_ACCESO
# MAGIC El archivo que viene con esta ingesta tiene un retorno de carro en su contenido haciendo que se quiebre la lectura de los datos. Para esto se hace una nueva lectura, previa a validaciones prelanding.

# COMMAND ----------

# cargar librerias
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# obtener parámetros
ruta_stage      = dbutils.widgets.get("ruta_stage")
nombre_archivo  = dbutils.widgets.get("nombre_archivo")
delimitador     = "|"
encabezado      = dbutils.widgets.get("encabezado").lower()
encoding        = dbutils.widgets.get("encoding")
quote           = dbutils.widgets.get("quote")

# COMMAND ----------

# cargar dataframe. Se 
df = (spark.read
      .option("delimiter", delimitador)
      .option("header", encabezado)
      .option("quote",quote)
      .option("encoding", encoding)
      .csv(f"{ruta_stage}/{nombre_archivo}",
           escape='"', 
           multiLine=True, 
           ignoreLeadingWhiteSpace=True, 
           ignoreTrailingWhiteSpace=True
           )
      )

# COMMAND ----------

df.count()

# COMMAND ----------

# reemplazar caracteres
from pyspark.sql.functions import regexp_replace

for i in range(len(df.columns)):
  column = df.columns[i]
  df = df.withColumn(column, regexp_replace(column, "\r", ""))
  print(column)

# COMMAND ----------

df.count()

# COMMAND ----------

# guardar en ruta temporal con otro delimitador
ruta_stage      = ruta_stage.replace('temp','')
(df
 .coalesce(1)
 .write.mode("overwrite")
 .format("csv")
 .options(delimiter="|")
 .save(f"{ruta_stage}", header=True)
 )

# COMMAND ----------

# mover archivo a ruta stage renombrado 
nombre_archivo  = nombre_archivo.replace('.gz','')
file = [f.path for f in dbutils.fs.ls(f"{ruta_stage}") if f.name.startswith("part-00000")][0]
dbutils.fs.mv(
  file,
  f"{ruta_stage}/{nombre_archivo}"
  )

# COMMAND ----------

# quitar archivo temporal después de renombrado
dbutils.fs.rm(f"{ruta_stage}/temp/{nombre_archivo}.gz")

# COMMAND ----------

# dfn = (spark.read
#       .option("delimiter", ";")
#       .option("header", encabezado)
#       .option("quote",quote)
#       .option("encoding", encoding)
#       .csv(f"{ruta_stage}/{nombre_archivo}")
#       )

# COMMAND ----------

# dfn.count()

# COMMAND ----------

# display(
#   dfn
# )

# COMMAND ----------


