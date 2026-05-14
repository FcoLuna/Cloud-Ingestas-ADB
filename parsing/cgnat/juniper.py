# Databricks notebook source
# MAGIC %md
# MAGIC ### Parsing para Ingesta JUNIPER
# MAGIC El archivo viene con registros acumulados en una misma linea, la que contienen un evento como timestamp, router y registros para ese mismo evento. Se hace uso de método "split" para obtener todos los elementos que contiene el registro, usando como separador el caracter espacio

# COMMAND ----------

# cargar librerias
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# # obtener parámetros
ruta_stage      = dbutils.widgets.get("ruta_stage")
nombre_archivo  = dbutils.widgets.get("nombre_archivo")
delimitador     = dbutils.widgets.get("delimitador")
encabezado      = dbutils.widgets.get("encabezado").lower()
encoding        = dbutils.widgets.get("encoding")
quote           = dbutils.widgets.get("quote")

# COMMAND ----------

files = [file.path for file in dbutils.fs.ls(ruta_stage) if file.name.startswith("10_44_") and file.name.endswith(".log")]

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

# separar columna
dfs = df_maestro.withColumn("split_column", split(df_maestro["_c0"], " "))

# COMMAND ----------

display(
    dfs
)

# COMMAND ----------

# Crear nuevas columnas basadas en los índices del split
dfo = (
    dfs
        .withColumn("timestamp_router", concat(
            dfs["split_column"].getItem(0), 
            lit(" "), 
            dfs["split_column"].getItem(1),
            lit(" "), 
            dfs["split_column"].getItem(2))
                    )
        .withColumn("timestamp_servidor", concat(
            dfs["split_column"].getItem(3), 
            lit(" "), 
            dfs["split_column"].getItem(4),
            dfs["split_column"].getItem(5))
                    )
        .withColumn("timestamp_servidor", substr(col("timestamp_servidor"), lit(0), length(col("timestamp_servidor")) - 1))
        .withColumn("router_cgnat", dfs["split_column"].getItem(6))
        .withColumn("router_cgnat", substr(col("router_cgnat"), lit(0), length(col("router_cgnat")) - 1))
        .withColumn("tipo_transaccion", dfs["split_column"].getItem(7))
        .withColumn("tipo_transaccion", substr(col("tipo_transaccion"), lit(0), length(col("tipo_transaccion")) - 1))
        .withColumn("ip_privada_cliente", dfs["split_column"].getItem(8))
        .withColumn("ip_publica_cliente", split(dfs["split_column"].getItem(10), ":")[0])
        .withColumn("rango_puerto_cliente", split(dfs["split_column"].getItem(10), ":")[1])
        .drop("_c0","split_column")
        .select("timestamp_router", "timestamp_servidor", "router_cgnat", "tipo_transaccion", "ip_privada_cliente", "ip_publica_cliente", "rango_puerto_cliente","filename_spark")
)

# COMMAND ----------

display(
    dfo
)

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
file = [f.path for f in dbutils.fs.ls(f"{ruta_stage}") if f.name.startswith("part-00000")][0]
dbutils.fs.mv(
  file,
  f"{ruta_stage}/{nombre_archivo}"
  )

# COMMAND ----------

# quitar archivo temporal después de renombrado
dbutils.fs.rm(f"{ruta_stage}/temp", recurse=True)
