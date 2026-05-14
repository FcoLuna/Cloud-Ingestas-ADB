# Databricks notebook source
import requests
import json
from datetime import datetime, timezone

from pyspark.sql import SparkSession

ruta_stage      = dbutils.widgets.get("ruta_stage")
fecha           = dbutils.widgets.get("fecha")
delimitador     = dbutils.widgets.get("delimitador")
nombre_archivo  = dbutils.widgets.get("nombre_archivo")

# # For Debug
# ruta_stage      = "abfss://ingestas@stbigdatadev02.dfs.core.windows.net/data/gestion_recursos/operacion_red/smart_wifi/my_synthesis_chile/stage/temp"
# fecha           = "2025-01-01"
# delimitador     = ","
# nombre_archivo  = "MY_SYNTHESIS_CHILE_20250101.csv"

result_exit     = {}

print(ruta_stage)
print(nombre_archivo)

#Fecha en formato datetime
fecha_dt = datetime.strptime(fecha, "%Y-%m-%d")

# ts_from: Inicio del día (00:00:00) y lo convertimos a timestamp entero
ts_from = int(datetime(fecha_dt.year, fecha_dt.month, fecha_dt.day,  0,  0,  0, tzinfo=timezone.utc).timestamp())

# ts_to: Fin del día (23:59:59) y lo convertimos a timestamp entero
ts_to   = int(datetime(fecha_dt.year, fecha_dt.month, fecha_dt.day, 23, 59, 59, tzinfo=timezone.utc).timestamp())

url = "https://apiv2-eu.devo.com/search/query"

payload = json.dumps({
  "query": "from my.synthesis.chile select *",
  "from": ts_from,
  "to": ts_to,
  "mode": {
    "type": "json"
  },
  "dateFormat": "default"
})
headers = {
  'Content-Type': 'application/json',
  'Authorization': 'Bearer e9280a84c08ab7ed0d73aea4b6136759'
}

response = requests.request("POST", url, headers=headers, data=payload)
# los datos que nos interesan están en la clave object
response_json = response.json()['object']


# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

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
json_schema_path    = ruta_stage.replace("/stage/temp","") + "/" + ruta_stage.replace("/stage/temp","").split("/")[-1] + ".json"
print(json_schema_path)

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

# convertimos las claves del diccionario a minusculas
response_json_lower = [{k.lower(): v for k, v in dic.items()} for dic in response_json]

# df2 = spark.read.schema(schema).json(response_data)
df = spark.createDataFrame(response_json_lower, schema=schema)

# forzar a que la hora esté en utc0 tal como viene cuando se trae como csv
df = df.withColumn("eventdate", date_format(
        expr("to_utc_timestamp(timestamp_seconds(eventdate/1000),'America/Santiago')"),
        "yyyy-MM-dd HH:mm:ss.SSS"
    ))

#quitar caracteres especiales en caso de que los tuviera
df = df.select([
    regexp_replace(col(c), r'[\x0F\x08\x02\x06\x0E\x18\x19\x7F]', ' ').alias(c) if dict(df.dtypes)[c] == 'string' else col(c)
    for c in df.columns
])

# quitar saltos de linea en caso de que los tuviera
df = df.select([
    regexp_replace(col(c), r'[\r\n]+', ' ').alias(c) if dict(df.dtypes)[c] == 'string' else col(c)
    for c in df.columns
])

# #crea archivo csv en ruta temporal para despues renombrarlo
df.coalesce(1).write.format("csv").options(delimiter=delimitador).mode("overwrite").save(f"{ruta_stage}", header="true")

# renombrar archivo csv
ruta_stage = ruta_stage.replace("/temp","")
file = [f.path for f in dbutils.fs.ls(f"{ruta_stage}/temp") if f.name.startswith("part-00000")][0]
dbutils.fs.mv(
  file,
  f"{ruta_stage}/{nombre_archivo}"
  )


# COMMAND ----------

dfs = spark.read.option("delimiter",delimitador).option("header","true").option("encoding","UTF-8").csv(f"{ruta_stage}/{nombre_archivo}")

# COMMAND ----------

display(
  dfs
)

# COMMAND ----------

count = dfs.count()
count

result_exit['cantidad de registros'] = count

# COMMAND ----------

dbutils.notebook.exit(result_exit)
