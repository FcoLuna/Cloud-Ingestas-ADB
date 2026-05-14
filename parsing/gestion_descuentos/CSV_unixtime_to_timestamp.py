# Databricks notebook source
# DBTITLE 1,Import
from pyspark.sql.functions import to_timestamp, col, date_format

# COMMAND ----------

dbutils.widgets.text("path_adls","abfss://ingestas@stbigdatadev02.dfs.core.windows.net")
dbutils.widgets.text("archivo_input","/data/interacciones/campanas/gestion_descuentos/movimientos_puntos/stage_tmp/movimientos_puntos.csv")
dbutils.widgets.text("path_output","/data/interacciones/campanas/gestion_descuentos/movimientos_puntos/stage/")
dbutils.widgets.text("epoch_columns","fechaMovimiento,fechaVigencia")
dbutils.widgets.text("header","True")
dbutils.widgets.text("delimeter","~")
dbutils.widgets.text("quote","\"")
dbutils.widgets.text("escape","\\")
              

# COMMAND ----------

# DBTITLE 1,Variables ADF
       
path_adls = dbutils.widgets.get("path_adls")
archivo_input = dbutils.widgets.get("archivo_input")
path_output = dbutils.widgets.get("path_output")
epoch_columns  = dbutils.widgets.get("epoch_columns")
header  = dbutils.widgets.get("header")
delimeter  = dbutils.widgets.get("delimeter")
quote  = dbutils.widgets.get("quote")
escape  = dbutils.widgets.get("escape")

# COMMAND ----------

df = spark.read.format("csv")\
.option("header", header)\
.option("multiline", "true")\
.option("delimiter", delimeter)\
.option("quote", quote)\
.option("escape", escape)\
.load(f"{path_adls}{archivo_input}")

original_columns = df.columns

v_epoch_columns = epoch_columns.split(",")

for col_name in v_epoch_columns:
  df = df.withColumn(f"{col_name}_con_formato",date_format((col(col_name).cast("double") / 1000).cast("timestamp"),"yyyy-MM-dd HH:mm:ss.SSS"))
  df = df.drop(col(col_name))
  df = df.withColumnRenamed(f"{col_name}_con_formato", col_name)

#Borrar stage
#dbutils.fs.rm(f"{path_adls}{path_output}", True)

df.select(*original_columns)\
.repartition(1)\
.write\
.format("csv")\
.option("header", header)\
.option("multiline", "true")\
.option("delimiter", delimeter)\
.option("quote", quote)\
.option("escape", escape)\
.mode("overwrite")\
.save(f"{path_adls}{path_output}")


