# Databricks notebook source
# MAGIC %run ../funciones/Manejo_ADLS_v2

# COMMAND ----------

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
# encoding = dbutils.widgets.get("encoding")
# is_compuesta = dbutils.widgets.get("is_compuesta").lower().capitalize() == "True"
# format_date_filename_in = dbutils.widgets.get("format_date_filename_in")
# format_date_filename_out = dbutils.widgets.get("format_date_filename_out")
# format_ts = dbutils.widgets.get("format_ts")

# result_exit = {}

# EJEMPLO VALIDATE_SCHEMA
# [['StringType',(0,500),True],['DateType','yyyy-MM-dd',True],['IntegerType',True],['StringType',(0,500),True],['StringType',(0,500),True],['StringType',(0,500),True],['StringType',(0,500),True],['StringType',(0,500),True],['StringType',(0,500),True],['StringType',(0,500),True],['StringType',(0,500),True],['StringType',(0,500),True]]

# EJEMPLO REPLACE_SCHEMA EN FORMATO REGEX
# [('(\\,)',','),('&(#129|#137|#141|#145|#147|#154|gt);',' ')]

#filename_spark
#Feedback_GSSS_20230101 -> Feedback_GSSS_202301 -> Feedback_GSSS_YYYYMM
#Feedback_GSS_${filename:substringAfterLast('_'):toDate('yyyyMMdd'):format('yyyy')}${filename:substringAfterLast('_'):toDate('yyyyMMdd'):format('MM')}.csv

#ts
#${now():toNumber():format('yyyyMMddHHmmss')}

# from datetime import datetime

# def get_parte_fecha(fecha, parte_fecha, formato_fecha):
#     date = datetime.strptime(fecha, formato_fecha)
#     if parte_fecha == "Y":
#         return str(date.year)
#     elif parte_fecha == "M":
#         return f"{date.month:02d}"
#     elif parte_fecha == "D":
#         return f"{date.day:02d}"
#     else:
#         raise ValueError("Parte de fecha no válida")

# COMMAND ----------

ruta_stage = "abfss://contenedor-test@stbigdatadev02.dfs.core.windows.net/data/mov_sap_tienda/stage"
ruta_destino ="abfss://contenedor-test@stbigdatadev02.dfs.core.windows.net/data/mov_sap_tienda/landing"
nombre_archivo = "0404_OWS_Movimientos_1_20231231.txt"
nombre_proceso = "0404_OWS_Movimientos_1"
partition_landing = "m" #y, m, d
# nombre_archivo_landing = "Feedback_GSS_yyyyMM"
file_date = "20231231"
format_date_filename_in = "yyyyMMdd"
validate = False
validate_schema = """[['DateType',"dd/MM/yyyy",True],['StringType',(0,500),True],['LongType',True],['StringType',(0,500),True],['StringType',(0,500),True],['StringType',(0,500),True],['StringType',(0,500),True],['IntegerType',True],['StringType',(0,500),True],['StringType',(0,500),True],['StringType',(0,500),True],['StringType',(0,500),True],['StringType',(0,500),True],['StringType',(0,500),True],['StringType',(0,500),True],['StringType',(0,500),True],['StringType',(0,500),True]]"""
replace_text = False
replace_schema = "[('(\\;)',';')]"
delimitador = ";"
encabezado = True
encabezado_py = True
quote ="\""
encoding = "UTF-8"
is_compuesta = True

date_filename_landing = ""

# format_date_filename_out = "yyyyMM"
format_ts = "yyyyMMddHHmmss"

result_exit = {}

# COMMAND ----------

import re
import ast
from pyspark.sql.functions import *
from pyspark.sql.types import *
from datetime import datetime

# COMMAND ----------

# MAGIC %md
# MAGIC #### Lectura de CSV

# COMMAND ----------

df = spark.read.option("delimiter", delimitador).option("header", encabezado).option("quote",quote).option("encoding", encoding).csv(f"{ruta_stage}/{nombre_archivo}")
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Validar esquema CSV

# COMMAND ----------

if validate:
    df = df.withColumn("index_tmp", monotonically_increasing_id())
    df_inicial = df
    lista_schema = ast.literal_eval(validate_schema)
    print(lista_schema)

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
            
            df = df_inicial
            if lista_schema[i][-1]:
                df = df.filter(col(column).isNotNull()) if lista_schema[i][0] == "StringType" else df.filter((col(column) != "") & (col(column).isNotNull()))

            if lista_schema[i][0] == "DateType":
                format_date = lista_schema[i][1]
                df_temp = df.withColumn(column, to_date(col(column), format_date))
                index_bad = index_bad.union(df_temp.filter(col(column).isNull()).select("index_tmp"))
            
            elif lista_schema[i][0] == "IntegerType":
                df_temp = df.withColumn(column, df[column].cast(IntegerType()))
                index_bad = index_bad.union(df_temp.filter(col(column).isNull()).select("index_tmp"))

            elif lista_schema[i][0] == "LongType":
                df_temp = df.withColumn(column, df[column].cast(LongType()))
                index_bad = index_bad.union(df_temp.filter(col(column).isNull()).select("index_tmp"))

            elif lista_schema[i][0] == "FloatType":
                df_temp = df.withColumn(column, df[column].cast(FloatType()))
                index_bad = index_bad.union(df_temp.filter(col(column).isNull()).select("index_tmp"))

            elif lista_schema[i][0] == "StringType":
                min_len = int(lista_schema[i][1][0])
                max_len = int(lista_schema[i][1][1])
                df_temp = df.withColumn(column, df[column].cast(StringType()))
                index_bad = index_bad.union(df_temp.where((length(col(column)) < min_len) | (length(col(column)) > max_len)).select("index_tmp"))

        # SE CREA DATAFRAME CON FILAS MAL PARSEADAS
        df_bad = df_inicial.join(index_bad.distinct(), ['index_tmp'], how='inner').drop("index_tmp")
        df_good = df_inicial.join(index_bad.distinct(), ['index_tmp'], how='leftanti').drop("index_tmp")
        if df_bad.count() > 0:
            df_bad.coalesce(1).write.format('csv').options(delimiter=delimitador).mode('overwrite').save(f"{ruta_stage}_error/{nombre_archivo}", header=encabezado)
            result_exit['validation_csv'] = 'error'
            print("ERROR VALIDACION EN ALGUNAS FILAS")
        else:
            result_exit['validation_csv'] = 'ok'
            print("VALIDACION OK")

        df = df_good
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
# MAGIC ### Agregar columnas de Ingesta Compuesta

# COMMAND ----------

nombre_archivo_landing=""
formato_archivo_landing = ""
if is_compuesta:
    replace_format = [("yyyy","%Y"), ("YYYY","%Y"), ("MMMMM","%B"), ("MMM","%b"), ("MM","%m"), ("dd","%d"), ("hh","%I"), ("HH","%H"), ("mm","%M"), ("ss","%S")]
    for i in replace_format:
        format_date_filename_in = format_date_filename_in.replace(i[0], i[1])
        # format_date_filename_out = format_date_filename_out.replace(i[0], i[1])
        format_ts = format_ts.replace(i[0], i[1])

    # file_date = nombre_archivo.split(".csv")[0].split("_")[-1]
    # temp_date = datetime.strftime(datetime.strptime(file_date, format_date_filename_in), format_date_filename_out)
    # filename_spark = '_'.join(nombre_archivo.split("_")[:-1]) + f"_{temp_date}.csv"
    ts = datetime.strftime(datetime.now(), format_ts) 

    df = df.withColumn("filename_spark", lit(nombre_archivo))
    df = df.withColumn("ts", lit(ts))


    #Modificar directorio de destino landing con particiones
    year = datetime.strptime(file_date, format_date_filename_in).year
    month = datetime.strptime(file_date, format_date_filename_in).month
    day = datetime.strptime(file_date, format_date_filename_in).day

    # partition_landing = "d"

    #Definir nombre del archivo de particion en el landing adempas de la ruta de destino de landing, se construye en base al nivel de particion 
    #entregado como parametro. El nombre del archivo es una combinacion entre el nombre del proceso, la fecha de particion y el formato del archivo fuente.
    if partition_landing == "m":
        date_filename_landing = datetime.strftime(datetime.strptime(file_date, format_date_filename_in), "%Y%m")
        ruta_destino = ruta_destino+"/"+str(year)+"/"+str(month)
    elif partition_landing == "y":
        date_filename_landing = datetime.strftime(datetime.strptime(file_date, format_date_filename_in), "%Y")
        ruta_destino = ruta_destino+"/"+str(year)
    elif partition_landing == "d":
        date_filename_landing = datetime.strftime(datetime.strptime(file_date, format_date_filename_in), "%Y%m%d")
        ruta_destino = ruta_destino+"/"+str(year)+"/"+str(month)+"/"+str(day)
    
    formato_archivo_landing = nombre_archivo.split(".")[-1]
    nombre_archivo_landing = f"{nombre_proceso}_{date_filename_landing}.{formato_archivo_landing}"
    print(nombre_archivo_landing)
    print(ruta_destino)
    result_exit['dir_adls_landing']=ruta_destino


# COMMAND ----------

#Funcion para consultar si existe el archivo en la ruta especificada por el parametro path
def fileExists(path, file_name):
    file_list = dbutils.fs.ls(path)
    for file in file_list:
        if file.name == file_name:
            return True
            break
    return False

# COMMAND ----------

# MAGIC %md
# MAGIC ## Escribir a ruta de destino (landing)

# COMMAND ----------

print("[INFO] moviendo df a temp")
# df.coalesce(1).write.format(formato_archivo_landing).options(delimiter=delimitador).mode("overwrite").save(f"{ruta_destino}/temp", header=encabezado)
df.coalesce(1).write.format("csv").options(delimiter=delimitador).mode("overwrite").save(f"{ruta_destino}/temp", header=encabezado)
# dbutils.fs.rm(f"{ruta_stage}/{nombre_archivo}", recurse=True) 
if is_compuesta:
    print("[INFO] es compuesta")
    #Comprobar que exista el archivo con existFile
    if fileExists(ruta_destino, nombre_archivo_landing):
        #Cargar el archivo
        print("[INFO] Cargando df de particion existente")
        dforiginal = spark.read.option("delimiter", delimitador).option("header", encabezado).option("quote",quote).option("encoding", encoding).csv(f"{ruta_destino}/{nombre_archivo_landing}")
        #unir archivo existente con el procesado
        print("[INFO] Uniendo df de particion existente con df procesado")
        df = dforiginal.union(df)
        dbutils.fs.rm(f"{ruta_destino}/temp", recurse=True) 
        df.coalesce(1).write.format("csv").options(delimiter=delimitador).mode("overwrite").save(f"{ruta_destino}/temp", header=encabezado)
    else:
        print("archivo de particion no existe")

print("[INFO] Cargando archivo a landing")
#Crear archivo en directorio de landing
lista = dbutils.fs.ls(f"{ruta_destino}/temp")
filename = [i.name for i in lista if ".csv" in i.name[-4:]][0]
dbutils.fs.mv(f"{ruta_destino}/temp/{filename}", f"{ruta_destino}/{nombre_archivo_landing}")
dbutils.fs.rm(f"{ruta_destino}/temp", recurse=True) 

# dbutils.notebook.exit(result_exit)

# COMMAND ----------

# df.printSchema()


# dforiginal = spark.read.option("delimiter", delimitador).option("header", encabezado).option("quote",quote).option("encoding", encoding).csv("abfss://contenedor-test@stbigdatadev02.dfs.core.windows.net/data/gss_feedback_callout/landing/Feedback_GSS_20231216.csv")

# dforiginal.printSchema()

# dffinal = dforiginal.union(df)

# display(dffinal)

# dffinal.coalesce(1) \
#   .write \
#   .format("csv") \
#   .option("header", "true") \
#   .mode("overwrite") \
#   .save("abfss://contenedor-test@stbigdatadev02.dfs.core.windows.net/data/gss_feedback_callout/landing/2023/12/tmp")

# lista = dbutils.fs.ls("abfss://contenedor-test@stbigdatadev02.dfs.core.windows.net/data/gss_feedback_callout/landing/2023/12/tmp")
# filename = [i.name for i in lista if ".csv" in i.name[-4:]][0]
# dbutils.fs.mv(f"abfss://contenedor-test@stbigdatadev02.dfs.core.windows.net/data/gss_feedback_callout/landing/2023/12/tmp/{filename}", "abfss://contenedor-test@stbigdatadev02.dfs.core.windows.net/data/gss_feedback_callout/landing/2023/12/Feedback_GSS_20231217.csv")
# dbutils.fs.rm("abfss://contenedor-test@stbigdatadev02.dfs.core.windows.net/data/gss_feedback_callout/landing/2023/12/tmp", recurse=True) 
