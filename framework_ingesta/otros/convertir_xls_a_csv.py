# Databricks notebook source
import json
import ast

from pyspark.sql.functions import *
from pyspark.sql.types import *
from datetime import datetime

# Valores extraídos de Parametros.conf > path
adls_container  = dbutils.widgets.get("adls_container")
dir_adls_rel    = dbutils.widgets.get("dir_adls_rel")
nombre_xls      = dbutils.widgets.get("nombre_xls")

filtro = {}
try:
  filtro = json.loads(dbutils.widgets.get("filtro"))
  filtro = ast.literal_eval(filtro)
except:
  filtro = {}

# Testing
# adls_container  = "abfss://ingestas@stbigdatadev02.dfs.core.windows.net"
# dir_adls_rel    = "/data/interacciones/canales/opinat/isn_baf/stage/"
# nombre_xls      = "IsnBaf_20241224.xls"
# filtro          = "{\"baf\":\"Sí\"}"
# filtro          = ast.literal_eval(filtro)

path_file       = adls_container + dir_adls_rel + nombre_xls
nombre_csv      = nombre_xls.replace(".xls", ".csv")
ruta_stage      = adls_container + dir_adls_rel

# funcion para verificar si existe archivo en ADLS
def file_exists(path):
    try:
        dbutils.fs.ls(path)
        return True
    except Exception as e:
        if 'java.io.FileNotFoundException' in str(e):
            return False
        else:
            raise
# Ruta de esquema
json_schema_path    = ruta_stage.replace("/stage/","") + "/" + ruta_stage.replace("/stage/","").split("/")[-1] + ".json"

# Si existe archivo, cargar
check_file = file_exists(json_schema_path)
if check_file: 
    file_content = spark.read.text(json_schema_path).collect()
    json_string  = ''.join(row.value for row in file_content)
    data = json.loads(json_string)
else: 
    print("no hay archivo json")

# Convertir a StructType
data["fields"] = data["fields"][:-2]
schema = StructType.fromJson(data)

# Leer el archivo con pyspark y librería excel
display(path_file)
df = (
    spark.read.schema(schema).format("com.crealytics.spark.excel")
    .option("header", "true")
    .load(path_file)
)

#quitar saltos de linea
df_limpio = df.select([
    regexp_replace(col(c), r'[\r\n]+', ' ').alias(c) if dict(df.dtypes)[c] == 'string' else col(c)
    for c in df.columns
])

# aplicar filtro genérico pasado por parámetros. Dejo registros si contiene esta regla -> "(columna, valor)
if len(filtro.keys()) != 0:
    print(filtro)
    for (clave, valor) in filtro.items():
        df_limpio = df_limpio.where(col(clave)==valor)

# #crea archivo csv en ruta temporal para despues renombrarlo
df_limpio.coalesce(1).write.format("csv").options(delimiter=";").mode("overwrite").save(f"{adls_container}{dir_adls_rel}/temp", header="false")

# renombrar archivo csv
file = [f.path for f in dbutils.fs.ls(f"{adls_container}{dir_adls_rel}/temp") if f.name.startswith("part-00000")][0]
dbutils.fs.mv(
  file,
  f"{adls_container}{dir_adls_rel}{nombre_csv}"
  )

# COMMAND ----------

df = spark.read.option("delimiter",";").option("header","false").option("encoding","UTF-8").csv(adls_container + dir_adls_rel + nombre_csv)

# COMMAND ----------

display(
  df
)

# COMMAND ----------

df.count()
