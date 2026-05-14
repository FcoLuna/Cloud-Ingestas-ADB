# Databricks notebook source
# MAGIC %md
# MAGIC Leer el archivo CSV

# COMMAND ----------

# MAGIC %md
# MAGIC ### Limpieza Archivo Detalle Números Portados

# COMMAND ----------

# cargar librerias
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# # obtener parámetros
#ruta_stage      = dbutils.widgets.text("ruta_stage", "abfss://ingestas@stbigdatadev02.dfs.core.windows.net/data/interacciones/ordenes/oap/detalle_numeros_portados/stage")
#ruta_landing    = dbutils.widgets.text("ruta_landing", "abfss://ingestas@stbigdatadev02.dfs.core.windows.net/data/interacciones/ordenes/oap/detalle_numeros_portados/landing")
#nombre_archivo  = dbutils.widgets.text("nombre_archivo", "detalle-numeros-portados_2025-01-19.csv")
#delimitador     = dbutils.widgets.text("delimitador", ";")
#encabezado      = dbutils.widgets.text("encabezado", "true")
#encoding        = dbutils.widgets.text("encoding", "UTF-8")
#quote           = dbutils.widgets.text("quote", "\"")

ruta_stage      = dbutils.widgets.get("ruta_stage")
ruta_landing    = dbutils.widgets.get("ruta_landing")
nombre_archivo  = dbutils.widgets.get("nombre_archivo")
delimitador     = dbutils.widgets.get("delimitador")
encabezado      = dbutils.widgets.get("encabezado").lower()
encoding        = dbutils.widgets.get("encoding")
quote           = dbutils.widgets.get("quote")

# COMMAND ----------

from pyspark.sql import SparkSession

# Crear una sesión de Spark
spark = SparkSession.builder.appName("LimpiarReporte").getOrCreate()

# Ruta al archivo CSV original
#input_file_path = "abfss://ingestas@stbigdatadev02.dfs.core.windows.net/data/interacciones/ordenes/oap/detalle_numeros_portados/stage/detalle-numeros-portados_2025-01-17.csv"

input_file_path = f"{ruta_stage}/{nombre_archivo}"

# Leer el archivo CSV en un DataFrame de PySpark
df = spark.read.text(input_file_path)

# Mostrar las primeras filas del DataFrame para entender su estructura
#df.show(20, truncate=False)
#df.printSchema()
df.show(20, truncate=False)

# COMMAND ----------

#df.count()

# COMMAND ----------

# MAGIC %md
# MAGIC Eliminar líneas vacías

# COMMAND ----------

# Filtrar las filas que no tienen valores nulos en todas las columnas
df_cleaned = df.filter(df.value.isNotNull())  # Filtramos si la primera columna no es nula

# Verificar que se eliminaron las líneas vacías
#df_cleaned.show(20, truncate=False)

# COMMAND ----------

#df_cleaned.count()

# COMMAND ----------

# MAGIC %md
# MAGIC Eliminar los títulos intercalados

# COMMAND ----------

# Supongamos que los títulos contienen la palabra 'SOLP' en alguna columna

df_cleaned_no_titles = df_cleaned.filter(df_cleaned.value.contains("SOLP"))

# Mostrar las primeras filas después de eliminar los títulos
df_cleaned_no_titles.show(20, truncate=False)

# COMMAND ----------

#df_cleaned_no_titles.count()

# COMMAND ----------

# MAGIC %md
# MAGIC Eliminar Comillas y  Simbolos especiales

# COMMAND ----------

from pyspark.sql import functions as F

df_cleaned_no_titles2 = df_cleaned_no_titles.withColumn("value", F.regexp_replace("value", "\"", ""))

# COMMAND ----------

df_cleaned_no_titles2 = df_cleaned_no_titles.withColumn("value", F.regexp_replace("value", "|", ""))

# COMMAND ----------

# MAGIC %md
# MAGIC Guardar csv Limpio sin Header

# COMMAND ----------

# Ruta donde se guardará el archivo CSV limpio
#output_file_path = "abfss://ingestas@stbigdatadev02.dfs.core.windows.net/data/interacciones/ordenes/oap/detalle_numeros_portados/stage/archivo_limpio"

output_file_path = f"{ruta_stage}/archivo_limpio"

# Guardar el DataFrame limpio en formato CSV
#df_cleaned_no_titles2.coalesce(1).write.mode("overwrite").option("sep", ";").option("header", "true").csv(output_file_path)
df_cleaned_no_titles2.coalesce(1).write.mode("overwrite").option("sep", delimitador).option("header", encabezado).csv(output_file_path)

# Verificar que se guardó correctamente
print(f"Archivo limpio guardado en: {output_file_path}")

# COMMAND ----------

# MAGIC %md
# MAGIC Guardar Archivo csv en Landing

# COMMAND ----------

# mover archivo a ruta landing
# Verificar si el archivo existe en el directorio de origen
try:
    # Usar dbutils.ls para listar los archivos en el directorio de origen
    archivos_origen = dbutils.fs.ls(f"{ruta_stage}/{nombre_archivo}")

    # Filtrar el archivo específico
    archivo_existe = any(archivo.name == f"{nombre_archivo}" for archivo in archivos_origen)

    if archivo_existe:
        # Si el archivo existe, moverlo al directorio de destino
        dbutils.fs.mv(f"{ruta_stage}/{nombre_archivo}", f"{ruta_landing}/{nombre_archivo}")
        print(f"Archivo {ruta_stage}/{nombre_archivo} movido a {ruta_landing}/{nombre_archivo}.")
    else:
        print(f"El archivo {ruta_stage}/{nombre_archivo} no existe en el directorio de origen.")
except Exception as e:
    print(f"Error al verificar o mover el archivo: {e}")
