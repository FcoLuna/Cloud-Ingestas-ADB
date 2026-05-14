# Databricks notebook source
# MAGIC %md
# MAGIC ### Parsing para Ingesta PARAMETRICA_CATALOGO_PLANES
# MAGIC Esta ingesta requiere de un campo calculado adicional caracteristica_roaming obtenido a patior de dias_roaming, junto con la validacion de un valor decimal del campo dcto_porc. Para esto se hace una nueva lectura, previa a validaciones prelanding.

# COMMAND ----------

# cargar librerias
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# obtener parámetros
ruta_stage      = dbutils.widgets.get("ruta_stage")
nombre_archivo  = dbutils.widgets.get("nombre_archivo")
delimitador     = dbutils.widgets.get("delimitador")
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

# reemplazar caracteres y crea nueva columna
df = df.withColumn("DCTO_PORC", regexp_replace("DCTO_PORC", ",", ".")) \
       .withColumn("CARACTERISTICA_ROAMING", col("DIAS_ROAMING")) \
       .withColumn("DIAS_ROAMING",when(
        substring(col("DIAS_ROAMING"), 1, 1) == "W",
        split(col("DIAS_ROAMING"), " ")[2]  # Tercer elemento si empieza con "W"
            ).otherwise(
        split(col("DIAS_ROAMING"), " ")[0]  # Primer elemento si no empieza con "W"
            ).cast("int")) \
       .select("ID_BO", "ID_PO", "ID_BO_DCTO", "BO_DESC", "PO_DESC", "BO_DCTO_DESC", "CF_VOZ", "DMP_VOZ", "CF_DATOS", "DMP_DATOS", "CF_LLENO", "CF_DCTO", "DCTO_PORC", "DCTO_MONTO", "VIGENCIA_DCTO", "USO_DCTO", "MINUTOS", "VALOR_MINUTO", "CAPACIDAD", "TALLA", "UMBRAL", "CORTE", "SMS", "BOLSA_MONETARIA", "DATOS_TEMATICOS", "DIAS_ROAMING", "PLAN_GROUP", "VCP", "PLAN_RANK", "TIPO_PLAN", "SEGMENTO", "MULTILINEA", "EQUIPO", "FUNC_COMPARTE_GB", "ELEGIBILIDAD_PLAN", "USO_PLAN", "TIPO_PARRILLA", "PARRILLA_VIGENTE", "VALIDACION", "AREA_RESPONSABLE", "FECHA_CIERRE", "AJUSTES_IPC", "ASIG_INTERNA", "FAMILIA_FUN", "AFINIDAD", "MOVISTAR_ONE", "COMPARTE_GB", "MOVISTAR_PLAY", "PLAN_2X1", "USO_NBA", "MOVISTAR_CTODO", "MCT_UMBRAL", "NETFLIX", "ONE_NUMBER", "ARMA_TPLAN", "MANDATO", "MAYORISTA", "PLAN_MIGRA", "FECHA_ACTUALIZACION", "CARACTERISTICA_ROAMING")



# COMMAND ----------

# Reemplazar celdas vacías en todas las columnas por null
df_transformed = df.select([when(col(c) == "", None).otherwise(col(c)).alias(c) for c in df.columns])

# COMMAND ----------

df_transformed.show()
df_transformed.count()

# COMMAND ----------

# guardar en ruta temporal con otro delimitador
ruta_stage      = ruta_stage.replace('temp','')
(df_transformed
 .coalesce(1)
 .write.mode("overwrite")
 .format("csv")
 .options(delimiter= delimitador)
 .save(f"{ruta_stage}", header=True)
 )

# COMMAND ----------

# mover archivo a ruta stage renombrado 
file = [f.path for f in dbutils.fs.ls(f"{ruta_stage}") if f.name.startswith("part-00000")][0]
dbutils.fs.mv(
  file,
  f"{ruta_stage}/{nombre_archivo}"
  )

# COMMAND ----------

# quitar archivo temporal después de renombrado
dbutils.fs.rm(f"{ruta_stage}/temp/{nombre_archivo}")
