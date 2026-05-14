# Databricks notebook source
# MAGIC %md
# MAGIC ### Parsing para Ingesta RECUPERACION_EQUIPOS
# MAGIC El archivo que viene con esta ingesta tiene dos campos adicionales nulos haciendo que se quiebre la inserción de los datos en el notebook de validaciones prelanding. Para esto se hace una nueva lectura, previa a validaciones prelanding.

# COMMAND ----------

# cargar librerias
from pyspark.sql.functions import *
from pyspark.sql.types import *
import json

# COMMAND ----------

# obtener parámetros
ruta_stage      = dbutils.widgets.get("ruta_stage")
nombre_archivo  = dbutils.widgets.get("nombre_archivo")
delimitador     = dbutils.widgets.get("delimitador")
encabezado      = dbutils.widgets.get("encabezado").lower()
encoding        = dbutils.widgets.get("encoding")
quote           = dbutils.widgets.get("quote")

# COMMAND ----------

# ruta_stage      = "abfss://ingestas@stbigdatadev02.dfs.core.windows.net/data/gestion_recursos/servicio_terreno/empresas_recupero/recuperacion_equipos/stage"
# nombre_archivo  = "now_recuperacion_20241129.csv"
# delimitador     = ";"
# encabezado      = "true"
# encoding        = "UTF-8"
# quote           = "\""

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

lista_columnas = df.columns
lista_columnas

# COMMAND ----------

lista_columnas = [columna.replace("(yyyy-mm-dd)","").lower() for columna in lista_columnas]

# COMMAND ----------

lista_columnas

# COMMAND ----------

df = df.select(
    *[
        col(old_col).alias(new_col) for (old_col, new_col) in zip(df.columns, lista_columnas)
    ]
)

# COMMAND ----------

def file_exists(path):
    try:
        dbutils.fs.ls(path)
        return True
    except Exception as e:
        if 'java.io.FileNotFoundException' in str(e):
            return False
        else:
            raise
json_schema_path = ruta_stage.replace("/stage/temp","") + "/" + ruta_stage.replace("/stage/temp","").split("/")[-1] + ".json"
check_file = file_exists(json_schema_path)

if check_file: 
        file_content = spark.read.text(json_schema_path).collect()
        json_string = ''.join(row.value for row in file_content)
        data = json.loads(json_string)

        columnas_seleccionadas = []

        for i in data['fields']:
            columna = i['name']
            columnas_seleccionadas.append(columna)

        try:
            columnas_seleccionadas.remove('filename_spark')
            columnas_seleccionadas.remove('ts')
            df = df.select(columnas_seleccionadas)
        except:
            pass

# COMMAND ----------

display(
    df
)

# COMMAND ----------

# guardar en ruta temporal con otro delimitador
ruta_stage      = ruta_stage.replace('temp','')
(df
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
