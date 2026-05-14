# Databricks notebook source
# ruta_stage = dbutils.widgets.get("ruta_stage")
# ruta_destino = dbutils.widgets.get("ruta_destino")
# nombre_archivo = dbutils.widgets.get("nombre_archivo")
# validate = dbutils.widgets.get("validate").lower().capitalize() == "True"
# validate_schema = dbutils.widgets.get("validate_schema")
# replace_text = dbutils.widgets.get("replace_text").lower().capitalize() == "True"
# replace_schema = dbutils.widgets.get("replace_schema")

# delimitador = dbutils.widgets.get("delimitador")
# encabezado = dbutils.widgets.get("encabezado").lower()
# encabezado_py = encabezado.capitalize() == "True"
# quote = dbutils.widgets.get("quote")



ruta_stage = "abfss://contenedor-test@stbigdatadev02.dfs.core.windows.net/data/gss_feedback_callout/stage"
ruta_destino = "abfss://contenedor-test@stbigdatadev02.dfs.core.windows.net/data/gss_feedback_callout/landing"
nombre_archivo = "Feedback_GSS_20231212.csv"
validate = False
validate_schema = "[['StringType', (0,500)], ['StringType', (0,500)], ['StringType', (0,500)], ['IntegerType'], ['DateType', 'yyyy-MM-dd'], ['StringType', (0,500)]]"
min_string = 0
max_string = 500
#format_date = "yyyy-MM-dd"
delimitador = ";"
dir_hdfs = "abfss://contenedor-test@stbigdatadev02.dfs.core.windows.net/data/gss_feedback_callout"
encabezado = "true"
encabezado_py = encabezado.capitalize() == "True"
quote = "\""
replace_text = True
replace_schema = "[('(\\;)',';')]"

result_exit = {}

# COMMAND ----------

import re
import ast
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# MAGIC %md
# MAGIC #### Lectura de CSV

# COMMAND ----------

df = spark.read.option("delimiter", delimitador).option("header", encabezado).option("quote",quote).option("encoding", "ISO-8859-1").csv(f"{ruta_stage}/{nombre_archivo}")
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Validar esquema CSV

# COMMAND ----------

if validate:
    df = df.withColumn("index_tmp", monotonically_increasing_id())
    df_inicial = df
    lista_schema = ast.literal_eval(validate_schema)

    # VALIDAR NUMERO DE COLUMNAS
    if len(df_inicial.columns) != len(lista_schema) +1:
        print("ERROR CANTIDAD DE COLUMNAS")
        raise ValueError (f"No coincide el esquema que contiene {len(lista_schema)} elementos, para un CSV con {len(df_inicial.columns)-1} columnas.")
    else:
        # PRUEBA CON LISTA DE SCHEMA POR CADA COLUMNA
        index_bad = spark.createDataFrame(data = [], schema=StructType([StructField('index_tmp', IntegerType(), True)]))
        for i in range(len(df_inicial.columns)):
            column = df_inicial.columns[i]
            if column == "index_tmp":
                continue

            if lista_schema[i][0] == "DateType":
                format_date = lista_schema[i][1]
                df_temp = df.withColumn(column, to_date(col(column), format_date))
                df = df_temp.filter(col(column).isNotNull())
                index_bad = index_bad.union(df_temp.filter(col(column).isNull()).select("index_tmp"))
            
            elif lista_schema[i][0] == "IntegerType":
                df_temp = df.withColumn(column, df[column].cast(IntegerType()))
                df = df_temp.filter(col(column).isNotNull())
                index_bad = index_bad.union(df_temp.filter(col(column).isNull()).select("index_tmp"))

            elif lista_schema[i][0] == "LongType":
                df_temp = df.withColumn(column, df[column].cast(LongType()))
                df = df_temp.filter(col(column).isNotNull())
                index_bad = index_bad.union(df_temp.filter(col(column).isNull()).select("index_tmp"))

            elif lista_schema[i][0] == "FloatType":
                df_temp = df.withColumn(column, df[column].cast(FloatType()))
                df = df_temp.filter(col(column).isNotNull())
                index_bad = index_bad.union(df_temp.filter(col(column).isNull()).select("index_tmp"))

            elif lista_schema[i][0] == "StringType":
                min_len = int(lista_schema[i][1][0])
                max_len = int(lista_schema[i][1][1])
                index_bad = index_bad.union(df.where((length(col(column)) < min_len) | (length(col(column)) > max_len)).select("index_tmp"))
                df = df.where((length(col(column)) >= min_len) | (length(col(column)) <= max_len)).withColumn(column, df[column].cast(StringType()))

        # SE CREA DATAFRAME CON FILAS MAL PARSEADAS
        df_bad = df_inicial.join(index_bad.distinct(), ['index_tmp'], how='inner').drop("index_tmp")
        if df_bad.count() > 0:
            df_bad.write.format('csv').options(delimiter=delimitador).mode('overwrite').save(f"{ruta_stage}_error/{nombre_archivo}", header="true")
            result_exit['validation_csv'] = 'error'
            print("ERROR VALIDACION EN ALGUNAS FILAS")
        else:
            result_exit['validation_csv'] = 'ok'
            print("VALIDACION OK")

        df = df.drop("index_tmp")
else:
    result_exit['validation_csv'] = 'ok'

# COMMAND ----------

# MAGIC %md
# MAGIC #### Reemplazar valores

# COMMAND ----------

if replace_text:
    lista_replace = ast.literal_eval(replace_schema)
    print(lista_replace)
    for elem_replace in lista_replace:
        for i in range(len(df.columns)):
            column = df.columns[i]
            df = df.withColumn(column, regexp_replace(column, elem_replace[0], elem_replace[1]))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Escribir a ruta de destino (landing) OLD

# COMMAND ----------

# # df_pandas = df.toPandas()
# # df_pandas.to_csv(f"{ruta_destino}/{nombre_archivo}", sep=delimitador, header=encabezado_py)
# print('uniendo particiones..')
# df.coalesce(1).write.format('csv').options(delimiter=delimitador).mode('overwrite').save(f"{ruta_destino}/temp", header=encabezado)

# #print("borrando...")
# #dbutils.fs.rm(f"{ruta_stage}/{nombre_archivo}", recurse=True) 
# # #DESCOMENTAR!!!!DESCOMENTAR!!!!DESCOMENTAR!!!!DESCOMENTAR!!!!DESCOMENTAR!!!!DESCOMENTAR!!!!DESCOMENTAR!!!!DESCOMENTAR!!!!DESCOMENTAR!!!!

# lista = dbutils.fs.ls(f"{ruta_destino}/temp")
# print('lista ', lista)
# filename = [i.name for i in lista if ".csv" in i.name[-4:]][0]
# print('filename ', filename)
# print("moviendo...")
# dbutils.fs.mv(f"{ruta_destino}/temp/{filename}", f"{ruta_destino}/{nombre_archivo}")

# print('borrando temp')
# dbutils.fs.rm(f"{ruta_destino}/temp", recurse=True) 
# print('saliendo...')
# dbutils.notebook.exit(result_exit)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Escribir a ruta de destino (landing)

# COMMAND ----------

# df_pandas = df.toPandas()
# df_pandas.to_csv(f"{ruta_destino}/{nombre_archivo}", sep=delimitador, header=encabezado_py)
print('uniendo particiones..')
df.coalesce(1).write.format('csv').options(delimiter=delimitador).mode('overwrite').save(f"{ruta_destino}/temp", header=encabezado)

#print("borrando...")
#dbutils.fs.rm(f"{ruta_stage}/{nombre_archivo}", recurse=True) 
# #DESCOMENTAR!!!!DESCOMENTAR!!!!DESCOMENTAR!!!!DESCOMENTAR!!!!DESCOMENTAR!!!!DESCOMENTAR!!!!DESCOMENTAR!!!!DESCOMENTAR!!!!DESCOMENTAR!!!!

lista = dbutils.fs.ls(f"{ruta_destino}/temp")
print('lista ', lista)
filename = [i.name for i in lista if ".csv" in i.name[-4:]][0]
print('filename ', filename)
print("moviendo...")
dbutils.fs.mv(f"{ruta_destino}/temp/{filename}", f"{ruta_destino}/{nombre_archivo}")

print('borrando temp')
dbutils.fs.rm(f"{ruta_destino}/temp", recurse=True) 
print('saliendo...')
dbutils.notebook.exit(result_exit)
