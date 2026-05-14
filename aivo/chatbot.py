# Databricks notebook source
import requests
import json
import os
import re
import pandas as pd
from io import StringIO
from pyspark.sql import DataFrame
from delta.tables import DeltaTable
from pyspark.sql.functions import current_date, current_timestamp, regexp_replace, col, to_date, date_format, lit
from pyspark.sql.types import StringType, TimestampType

# COMMAND ----------

spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")

# COMMAND ----------

#PARAMETROS
dia = dbutils.widgets.get("dia")
mes = dbutils.widgets.get("mes")
year = dbutils.widgets.get("year")
bigdata_close_date = year + '-' + mes + '-' + dia
email = dbutils.widgets.get("email")
dir_adls = dbutils.widgets.get("dir_adls")
filename = dbutils.widgets.get("filename")
merge_string = dbutils.widgets.get("merge_string")
tablaUC = dbutils.widgets.get("tablaUC")
password='$Srvv263'

mes = mes.zfill(2)
dia = dia.zfill(2)


# dia='01'
# mes='11'
# year='2024'
# email='pablo.gonzalezm@telefonica.com'
# password='$Srvv263'
# dir_adls=f"abfss://ingestas@stbigdatadev02.dfs.core.windows.net/data/interacciones/canales/aivo/chatbot/"
# filename='chatbot_interacciones_canales_aivo_20241101_16.csv'
# merge_string='a.id = b.id and a.fecha = b.fecha and a.pregunta = b.pregunta and a.respuesta = b.respuesta'
# tablaUC='bi_ingestas.raw_interacciones.chatbot'


#RUTAS
path_stage   = dir_adls + '/stage/'
path_raw     = dir_adls + '/raw/'
path_landing = dir_adls + '/landing/'


# COMMAND ----------

#Funciones
def mergeData(destination_path, merge_string, df, tablaUC) -> bool:
    try:
        print("Modo Merge")
        
        # Crea la DeltaTable a partir de la ruta de Delta Lake
        deltaTable = DeltaTable.forPath(spark, destination_path)
        
        # Realiza la operación MERGE INTO
        (deltaTable.alias("a")
                   .merge(df.alias("b"), merge_string)
                   .whenMatchedUpdateAll()
                   .whenNotMatchedInsertAll()
                   .execute())
        
        return True

    except Exception as e:
        print(f"Error en el Merge: {e}")
        
        # Si la partición es True, escribe los datos con partición
        if spark.catalog.tableExists(tablaUC):
          print("Tabla existe hubo un error en el merge")           
        else:
          print("Tabla no existe se crea")    
          df.write.format("delta") \
                  .mode("overwrite") \
                  .partitionBy("year", "month")\
                  .option("overwriteSchema", "true") \
                  .option("path", destination_path) \
                  .saveAsTable(tablaUC)
        
        return False
      

# Función para limpiar los nombres de las columnas
def clean_column_names(df: DataFrame) -> DataFrame:
    cleaned_columns = []
    for col_name in df.columns:
        # Reemplaza caracteres no válidos por un guion bajo o los elimina
        cleaned_col = re.sub(r'[ ,;{}()\n\t=]', '_', col_name)
        cleaned_columns.append(cleaned_col)
    
    # Cambia los nombres de las columnas en el DataFrame
    return df.toDF(*cleaned_columns)

# COMMAND ----------

# LOGIN
login_url = "https://api.aivo.co/api/v1/user/login-simple"
login_payload = {
    "email": f"{email}",
    "password": f"{password}"
}
headers = {
    "Content-Type": "application/json"
}

response = requests.post(login_url, headers=headers, data=json.dumps(login_payload))
response_data = response.json()
var_bearer = response_data['Authorization']
print(var_bearer)


# LOGIN TOKEN
login_url = "https://api.aivo.co/api/v1/user/login-simple"
login_payload = {
    "email": f"{email}",
    "password": f"{password}",
}
headers = {
    "Content-Type": "application/json"
}

response = requests.post(login_url, headers=headers, data=json.dumps(login_payload))
response_data = response.json()
var_bearer = response_data['Authorization']
print(var_bearer)

# COMMAND ----------

# AGREGA LISTADO DE FECHAS Y TOKEN
params = []
for hour in range(24):
    param = f"from={year}-{mes}-{dia}%20{hour:02d}:00:00&to={year}-{mes}-{dia}%20{hour:02d}:59:59"
    params.append(param)
# Tokens adicionales
tokens = [
    "TnpreU1qQmpNbVl3TjJVM016WXlOVGMzT1RNd1pHRTROVE15TVRFelpHTT0=",
    "WkdObVltRmhaalptTlRZMVlUazJOREF4WlRBNU56azVPVFJsT1dNNFlUST0=",
    "TldJNU5ETTRNV0poWkRKa01EZzJNREV5WWpZMFpEZzBORGxoTVRKa1kyUT0=",
    "Wm1JNVltVXlOakJqWXpCaVpqVXpOekExTmpWbVl6SmpaalJpTkRZNFlUUT0=",
    "T1dNelpEWmhZMlpqWm1KbU5tRmtNelpoWVdVNE5XWXlObUV6TURjNU5qWT0=",
    "TnpOa1l6TXhZalUzWmpjd09USTVNbUpqT1RNek0yWXpPVGcwTURRd04yTT0=",
    "T1RRMU9EZ3lOV05rTlRabU56UmpOR1JpTlRWbVptRXlPRGhsWVRBd1pUWT0=",
    "T0RoaVlqQXdNekJsWVRrd05HUmtabVUyWm1aaE5UTm1OV0l5WkRGaU16az0=",
    "TkRNM1pXSXpNakZtTkdFelpUSTVZV0ZsTW1RMU5qYzFaV0ZqTnpNek1URT0=",
    "TlRCbU5EUTVNalUzWWpoaE9XSXhaRFkzTVdOak16QXdZbVZrTURWbFpHTT0=",
    "WXprM1ltSXlaamhtWlRBME5qbGlOamhrWXpVeU5qUXhOekkyTXpJek9Eaz0="
]

# COMMAND ----------

# DataFrame para almacenar los datos
df = pd.DataFrame()

# Función para realizar la solicitud y agregar los datos al DataFrame
def fetch_and_add_to_dataframe(param):
    global df
    url = f"https://api.aivo.co/api/v1/stats/conversation/complete-list/export?{param}"
    
    for token in tokens:
        headers = {
            "Authorization": f"{var_bearer}",
            "Content-Type": "application/json",
            "X-Token": token
        }
        print(url)
        response = requests.get(url, headers=headers)
        if response.status_code == 200:
            data = response.text
            temp_df = pd.read_csv(StringIO(data))
            df = pd.concat([df, temp_df], ignore_index=True)
        else:
            print(f"Error {response.status_code} al obtener datos con url {url} token {token}")

# Realizar las solicitudes y agregar los datos al DataFrame
for param in params:
    fetch_and_add_to_dataframe(param)

# Convertir todas las columnas a tipo string para evitar problemas de tipo de datos
df = df.astype(str)

# Guardar el DataFrame Spark en un archivo CSV en ADLS
df = spark.createDataFrame(df)

# cuenta el total de registros
total_registros = df.count()
print(f"total registros con duplicados {total_registros}")

# Llama a la función de limpieza
df_cleaned = clean_column_names(df).dropDuplicates()

# cuenta el total de registros
total_registros = df_cleaned.count()
print(f"total registros sin duplicados {total_registros}")


df_cleaned.coalesce(1) \
    .write \
    .mode("overwrite") \
    .csv(path_stage, header=True)

display(df_cleaned)

# COMMAND ----------

# Agregar campos de partición y bigdata_close_date y bigdata_ctrl_id
df_final = df_cleaned.withColumn("bigdata_close_date",lit(bigdata_close_date)) \
                     .withColumn("current_timestamp", current_timestamp().alias("current_timestamp")) \
                     .withColumn("bigdata_ctrl_id", regexp_replace(col("current_timestamp").cast("string"), "[^A-Z0-9_]", "")) \
                     .withColumn("partition_field", to_date(col("Fecha"), "yyyy-MM-dd")) \
                     .withColumn("year", date_format(col("partition_field"), "yyyy")) \
                     .withColumn("month", date_format(col("partition_field"), "MM")) \
                     .withColumn("day", date_format(col("partition_field"), "dd")) \
                     .withColumn("parametros_de_usuario", regexp_replace(col("Parámetros_de_usuario"), "[\n\r]", " ")) \
                     .drop("partition_field") \
                     .select(
                         col("Id").alias("id").cast(StringType()),
                         col("Fecha").alias("fecha").cast(TimestampType()),
                         col("Canal").alias("canal").cast(StringType()),
                         col("Condición").alias("condicion").cast(StringType()),
                         col("Media").alias("media").cast(StringType()),
                         col("Nombre_de_usuario").alias("nombre_de_usuario").cast(StringType()),
                         col("Email_de_usuario").alias("email_de_usuario").cast(StringType()),
                         col("Hash_de_usuario").alias("hash_de_usuario").cast(StringType()),
                         col("parametros_de_usuario").alias("parametros_de_usuario").cast(StringType()),
                         col("Nombre_de_la_intención").alias("nombre_de_la_intencion").cast(StringType()),
                         col("Pregunta").alias("pregunta").cast(StringType()),
                         col("Respuesta").alias("respuesta").cast(StringType()),
                         col("Feedback").alias("feedback").cast(StringType()),
                         col("Resolución").alias("resolucion").cast(StringType()),
                         col("Tag").alias("tag").cast(StringType()),
                         col("Host").alias("host").cast(StringType()),
                         col("País").alias("pais").cast(StringType()),
                         col("Ciudad").alias("ciudad").cast(StringType()),
                         col("Dispositivo").alias("dispositivo").cast(StringType()),
                         col("Path").alias("path").cast(StringType()),
                         col("Tipo_de_encuesta").alias("tipo_de_encuesta").cast(StringType()),
                         col("Valor").alias("valor").cast(StringType()),
                         col("Encuesta").alias("encuesta").cast(StringType()),
                         col("Pregunta_encuesta").alias("pregunta_encuesta").cast(StringType()),
                         col("Respuesta_encuesta").alias("respuesta_encuesta").cast(StringType()),
                         col("bigdata_close_date").alias("bigdata_close_date").cast(StringType()),
                         col("bigdata_ctrl_id").alias("bigdata_ctrl_id").cast(StringType()),
                         col("year").alias("year").cast(StringType()),
                         col("month").alias("month").cast(StringType())
                     ).filter("year is not NULL").dropDuplicates()

# COMMAND ----------

if spark.catalog.tableExists(tablaUC):
    print("La Tabla existe se eliminan registros por año mes y día")
    
    # Crear una instancia de DeltaTable
    delta_table = DeltaTable.forName(spark, tablaUC)
    
    # Eliminar los registros correspondientes a la fecha de ejecucion
    delta_table.delete(f"bigdata_close_date = '{year}-{mes}-{dia}'")
    
    # Escribir los nuevos datos en la tabla
    df_final.write.format("delta") \
        .mode("append") \
        .partitionBy("year", "month") \
        .option("overwriteSchema", "true") \
        .option("path", path_raw) \
        .saveAsTable(tablaUC)
else:
    print("La tabla no existe. Se crea la tabla")
    
    # Escribir los nuevos datos en la tabla
    df_final.write.format("delta") \
        .mode("overwrite") \
        .partitionBy("year", "month") \
        .option("overwriteSchema", "true") \
        .option("path", path_raw) \
        .saveAsTable(tablaUC)
