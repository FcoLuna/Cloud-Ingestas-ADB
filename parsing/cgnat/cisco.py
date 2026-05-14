# Databricks notebook source
# MAGIC %md
# MAGIC ### Parsing para Ingesta CISCO
# MAGIC El archivo viene con registros acumulados en una misma linea, la que contienen bases de un evento como timestamp, router y multiples registros para ese mismo evento. Se hace uso de método "explode" para generar tantos registros como elementos tenga el registro previamente dividido por la clausula "User"

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

# Debug
# ruta_stage      = "abfss://ingestas@stbigdatadev02.dfs.core.windows.net/data/gestion_recursos/inventario_red/cgnat/cisco/stage/temp"
# nombre_archivo  = "cgnat_cisco_20241227.log"
# delimitador     = "|"
# encabezado      = "false"
# encoding        = "UTF-8"
# quote           = "\""

# COMMAND ----------

files = [file.path for file in dbutils.fs.ls(ruta_stage) if file.name.startswith("10_179_") and file.name.endswith(".log")]

# COMMAND ----------

files

# COMMAND ----------

df_maestro = None

# Leer cada archivo, agregar la columna con su nombre y concatenarlo al maestro
for file_path in files:
    file_name = file_path.split("/")[-1]  # Obtener el nombre del archivo desde la ruta completa
    df = (spark.read
        .option("delimiter", delimitador)
        .option("header", encabezado)
        .option("quote",quote)
        .option("encoding", encoding).csv(file_path))  # Leer el archivo CSV
    df = df.withColumn("filename_spark", lit(file_name))  # Agregar columna con el nombre del archivo
    
    if df_maestro is None:
        df_maestro = df
    else:
        df_maestro = df_maestro.union(df)

# COMMAND ----------

display(
    df_maestro
)

# COMMAND ----------

# registros antes de transformación
df_maestro.count()

# COMMAND ----------

# separar columna mediante el caracter "["
split_df = df_maestro.withColumn("values", split(col("_c0"), "\["))

# COMMAND ----------

# Separar el timestamp (primer elemento) y los datos (resto de elementos)
xdf = (
    split_df.withColumn("base", split(col("values").getItem(0), " "))  
            .withColumn("dato", explode(slice(col("values"),2, size(col("values")))))  # Explode los valores (a partir del segundo)
            .select("base","dato","filename_spark")  # Seleccionar columnas finales
            .withColumn("dato", split(col("dato"), " "))
)

# COMMAND ----------

display(xdf)

# COMMAND ----------

# Crear nuevas columnas basadas en los índices del split
dfo = (
    xdf
        .withColumn("timestamp_router", concat(
            xdf["base"].getItem(0), 
            lit(" "), 
            xdf["base"].getItem(1),
            lit(" "), 
            xdf["base"].getItem(2))
                    )
        .withColumn("ip_router_origen", xdf["base"].getItem(3))
        .withColumn("flag", xdf["base"].getItem(5))
        .withColumn("anio", xdf["base"].getItem(6))
        .withColumn("mes", xdf["base"].getItem(7))
        .withColumn("dia", xdf["base"].getItem(8))
        .withColumn("hora", xdf["base"].getItem(9))
        .withColumn("flag2", xdf["base"].getItem(11))
        .withColumn("flag3", xdf["base"].getItem(12))
        .withColumn("tipo_nat", xdf["base"].getItem(13))
        .withColumn("flag4", xdf["base"].getItem(14))
        .withColumn("tipo_transaccion", xdf["dato"].getItem(0))
        .withColumn("flag5", when(xdf["dato"].getItem(1) == '-', lit(None).cast(StringType())).otherwise(xdf["dato"].getItem(1)))
        .withColumn("ip_privada_cliente", xdf["dato"].getItem(2))
        .withColumn("host_origen", xdf["dato"].getItem(3))
        .withColumn("ip_publica_cliente", when(xdf["dato"].getItem(5) == '-', lit(None).cast(StringType())).otherwise(xdf["dato"].getItem(5)))
        .withColumn("flag6", xdf["base"].getItem(14))
        .withColumn("puerto_inicio", when(xdf["dato"].getItem(7) == '-', lit(None).cast(StringType())).otherwise(xdf["dato"].getItem(7)))
        .withColumn("puerto_fin", when(xdf["dato"].getItem(8) == '-', lit(None).cast(StringType())).otherwise(xdf["dato"].getItem(8)))
        .withColumn("partition_date", to_date(concat(col("anio"), lit("-"), col("mes"), lit("-"), lpad(col("dia"),2,"0")), "yyyy-MMM-dd").cast("date"))
        .drop("base", "dato")
        .select("timestamp_router", "ip_router_origen", "flag", "anio", "mes", "dia", "hora", "flag2", "flag3", "tipo_nat", "flag4", "tipo_transaccion", "flag5", "ip_privada_cliente", "host_origen", "ip_publica_cliente", "flag6", "puerto_inicio", "puerto_fin", "partition_date", "filename_spark")
)

# COMMAND ----------

display(
    dfo
)

# COMMAND ----------

# registros después de transformación
dfo.count()

# COMMAND ----------

# guardar en ruta temporal con otro delimitador
ruta_stage      = ruta_stage.replace('temp','')
(dfo
 .coalesce(1)
 .write.mode("overwrite")
 .format("csv")
 .options(delimiter=delimitador)
 .save(f"{ruta_stage}", header=encabezado.capitalize())
 )

# COMMAND ----------

# mover archivo a ruta stage renombrado 
ruta_stage = ruta_stage.replace("/temp","")
# nombre_archivo = nombre_archivo.replace(".log",".csv") # Posible reemplazo de nombre
file = [f.path for f in dbutils.fs.ls(f"{ruta_stage}") if f.name.startswith("part-00000")][0]
dbutils.fs.mv(
  file,
  f"{ruta_stage}/{nombre_archivo}"
  )

# COMMAND ----------

dbutils.fs.rm(f"{ruta_stage}/temp", recurse=True)
