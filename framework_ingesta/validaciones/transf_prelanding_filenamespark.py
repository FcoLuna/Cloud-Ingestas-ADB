# Databricks notebook source
import re
import ast
from pyspark.sql.functions import *
from pyspark.sql.types import *
from datetime import datetime

# COMMAND ----------

# MAGIC %md
# MAGIC ### Obtención de parámetros desde Data Factory

# COMMAND ----------

# # # Variables Pruebas
# ruta_stage      = "abfss://bigdatadev@stbigdatadev02.dfs.core.windows.net/data/gestion_recursos/logistica/sap/transitos/stage"
# ruta_destino    = "abfss://bigdatadev@stbigdatadev02.dfs.core.windows.net/data/gestion_recursos/logistica/sap/transitos/landing"
# nombre_archivo  = "CHILE_transfers_20240703.txt.gz"
# validate        = True
# validate_schema = "[['StringType',(0,500)], ['StringType',(0,500)], ['StringType',(0,500)], ['StringType',(0,500)],['StringType',(0,500)],['StringType',(0,500)],['StringType',(0,500)],['StringType',(0,500)],['StringType',(0,500)],['StringType',(0,500)],['StringType',(0,500)], ['StringType',(0,500)], ['StringType',(0,500)], ['StringType',(0,500)],['StringType',(0,500)],['StringType',(0,500)],['StringType',(0,500)],['StringType',(0,500)],['StringType',(0,500)],['StringType',(0,500)],['StringType',(0,500)], ['StringType',(0,500)], ['StringType',(0,500)], ['StringType',(0,500)],['StringType',(0,500)],['StringType',(0,500)],['StringType',(0,500)]]"

# replace_text    = True
# replace_schema  = "[ ('(\\,)',','), ('&(#129|#137|#141|#145|#147|#154|gt);',' ') ]"
# delimitador     = ";"
# encabezado      = True
# encoding        = "UTF-8"
# quote           = "'"
# file_size       = "99181"
# is_compuesta    = True
# # is_dmps         = True

# try:
#   is_dmps = dbutils.widgets.get("is_dmps").lower().capitalize() == "True"
# except:
#   is_dmps = False


# if is_compuesta:
#     # Parámetros ingesta compuesta
#     nombre_proceso          = "CHILE_transfers" 
#     partition_landing       = "m"
#     file_date               = "20240703"
#     format_date_filename_in = "yyyyMMdd"
#     format_ts               = "yyyyMMddHHmmss"

# if is_dmps:
#     original_file_date = dbutils.widgets.get("original_file_date")
#     validate_schema_avro = dbutils.widgets.get("validate_schema_avro").lower().capitalize() == "True"
#     schema_avro = dbutils.widgets.get("schema_avro")

# nombre_archivo_landing = nombre_archivo
# result_exit = {}

# print("nombre_archivo_landing: ", nombre_archivo_landing)
   

# COMMAND ----------

# # Parámetros generales
ruta_stage = dbutils.widgets.get("ruta_stage")
ruta_destino = dbutils.widgets.get("ruta_destino")
nombre_archivo = dbutils.widgets.get("nombre_archivo")
validate = dbutils.widgets.get("validate").lower().capitalize() == "True"
validate_schema = dbutils.widgets.get("validate_schema")
replace_text = dbutils.widgets.get("replace_text").lower().capitalize() == "True"
replace_schema = dbutils.widgets.get("replace_schema")
delimitador = dbutils.widgets.get("delimitador")
encabezado = dbutils.widgets.get("encabezado").lower()
encoding = dbutils.widgets.get("encoding")
quote = dbutils.widgets.get("quote")
file_size = dbutils.widgets.get("file_size")
is_compuesta = dbutils.widgets.get("is_compuesta").lower().capitalize() == "True"
##nuevo
# is_dmps = dbutils.widgets.get("is_dmps").lower().capitalize() == "True"

try:
  is_dmps = dbutils.widgets.get("is_dmps").lower().capitalize() == "True"
except:
  is_dmps = False


add_original_filename = False
try:
  add_original_filename = dbutils.widgets.get("add_original_filename").lower().capitalize() == "True"
except:
  add_original_filename = False

if is_compuesta:
    # Parámetros ingesta compuesta
    filenamespark = dbutils.widgets.get("filenamespark")
    nombre_proceso = dbutils.widgets.get("nombre_proceso")
    partition_landing = dbutils.widgets.get("partition_landing")
    file_date = dbutils.widgets.get("file_date")
    format_date_filename_in = dbutils.widgets.get("format_date_filename_in")
    format_ts = dbutils.widgets.get("format_ts")

if is_dmps:
    original_file_date = dbutils.widgets.get("original_file_date")
    validate_schema_avro = dbutils.widgets.get("validate_schema_avro").lower().capitalize() == "True"
    schema_avro = dbutils.widgets.get("schema_avro")

nombre_archivo_landing = nombre_archivo
result_exit = {}

# EJEMPLO VALIDATE_SCHEMA
# [['StringType',(0,500),True],['DateType','yyyy-MM-dd',True],['IntegerType',True],['StringType',(0,500),True],['StringType',(0,500),True],['StringType',(0,500),True],['StringType',(0,500),True],['StringType',(0,500),True],['StringType',(0,500),True],['StringType',(0,500),True],['StringType',(0,500),True],['StringType',(0,500),True]]

# EJEMPLO REPLACE_SCHEMA EN FORMATO REGE
# [('(\\,)',','),('&(#129|#137|#141|#145|#147|#154|gt);',' ')]

# COMMAND ----------

# hay que revisar esto porque cuando esta ejecutando una ingesta simple da error el format_date_filename_in
# print(f"[INFO] nombre_archivo_landing: {nombre_archivo_landing}")
# print(f"[INFO] ruta_stage: {ruta_stage}")
# print(f"[INFO] nombre_archivo: {nombre_archivo}")
# print(f"[INFO] format_date_filename_in: {format_date_filename_in}")
# print(f"[INFO] file_date: {file_date}")
# print(f"[INFO] partition_landing: {partition_landing}")
# print(f"[INFO] nombre_proceso: {nombre_proceso}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Validación Tamaño del Archivo

# COMMAND ----------

if file_size == "0":
    print("ERROR ARCHIVO NO VÁLIDO")
    raise ValueError ("Archivo con tamaño no valido. Tamaño 0")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Lectura de CSV

# COMMAND ----------

df = spark.read.option("delimiter", delimitador).option("header", encabezado).option("quote",quote).option("encoding", encoding).csv(f"{ruta_stage}/{nombre_archivo}")
display(df)

# COMMAND ----------

#PARTE NUEVA PARA RENOMBRAR COLUMNAS 
new_column_names = [c.replace(" ", "_").replace(",", "").replace(";", "").replace("{", "").replace("}", "").replace("(", "").replace(")", "").replace("\n", "").replace("\t", "").replace("=", "").replace("/", "_").replace(".", "_") for c in df.columns]

df = df.toDF(*new_column_names)
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Agregar columnas de Ingesta Compuesta

# COMMAND ----------

if is_dmps:
    if validate_schema_avro:
        lista_schema_avro = ast.literal_eval(schema_avro)
        if len(df.columns) != len(lista_schema_avro):
            # Iterar sobre las columnas faltantes del esquema Avro
            for columna in lista_schema_avro[len(df.columns):]:
                # Agregar la columna nueva al DataFrame con valor None
                df = df.withColumn(str(columna[0]), lit(None))
    display(df)

if validate:
    df = df.withColumn("index_tmp", monotonically_increasing_id())
    df_inicial = df
    lista_schema = ast.literal_eval(validate_schema)
    print(lista_schema)

    # VALIDAR NUMERO DE COLUMNAS
    if len(df_inicial.columns) != len(lista_schema) +1:
        print(df_inicial.columns)
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

            elif lista_schema[i][0] == "TimeStampType":
                format_timestamp = lista_schema[i][1]
                df_temp = df.withColumn(column, to_timestamp(col(column), format_timestamp))
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
            df_bad.coalesce(1).write.format('csv').options(delimiter=delimitador).mode('overwrite').save(f"{ruta_stage}_error/temp", header=encabezado)
            lista_error = dbutils.fs.ls(f"{ruta_stage}_error/temp")
            filename_error = [i.name for i in lista_error if ".csv" in i.name[-4:]][0]
            dbutils.fs.mv(f"{ruta_stage}_error/temp/{filename_error}", f"{ruta_stage}_error/error_{nombre_archivo}")
            dbutils.fs.rm(f"{ruta_stage}_error/temp", recurse=True) 

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
# MAGIC ### Reemplazar valores

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

formato_archivo_landing = ""
date_filename_landing = ""

if is_dmps:
    print("id dmps")
    # colunmna para logica procesos dmps
    df = df.withColumn("partition_date", lit(original_file_date))

if is_compuesta:    
    print("is compuesta")
    replace_format = [("yyyy","%Y"), ("YYYY","%Y"), ("MMMMM","%B"), ("MMM","%b"), ("MM","%m"), ("dd","%d"), ("hh","%I"), ("HH","%H"), ("mm","%M"), ("ss","%S")]
    for i in replace_format:
        format_date_filename_in = format_date_filename_in.replace(i[0], i[1])
        format_ts = format_ts.replace(i[0], i[1])


    ts = datetime.strftime(datetime.now(), format_ts) 

    if(add_original_filename):
        df = df.withColumn("original_filename", lit(nombre_archivo))
    df = df.withColumn("filename_spark", lit(filenamespark))
    df = df.withColumn("ts", lit(ts))


    print(f"[INFO] file_date={file_date}")
    print(f"[INFO] format_date_filename_in={format_date_filename_in}")

    ################################################################################# 
    #Modificar directorio de destino landing con particiones
    year = datetime.strptime(file_date, format_date_filename_in).year
    print(f"[INFO] year={year}")
    month = datetime.strptime(file_date, format_date_filename_in)
    month  = month.strftime("%m")
    print(f"[INFO] month={month}")
    day = datetime.strptime(file_date, format_date_filename_in)
    day  = day.strftime("%d")
    print(f"[INFO] day={day}")

    ##################################################################################


    # partition_landing = "d"

    #Definir nombre del archivo de particion en el landing ademas de la ruta de destino de landing, se construye en base al nivel de particion 
    #entregado como parametro. El nombre del archivo es una combinacion entre el nombre del proceso, la fecha de particion y el formato del archivo fuente.
    print(f"[INFO] partition_landing={partition_landing}")
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

    ########################################PARTE NUEVA#############################################
    print(f"[INFO] formato_archivo_landing={formato_archivo_landing}")
    if formato_archivo_landing == "gz":
        formato_archivo_landing = "txt"
        nombre_archivo_landing = f"{nombre_proceso}_{date_filename_landing}.{formato_archivo_landing}"
    else:
        nombre_archivo_landing = f"{nombre_proceso}_{date_filename_landing}.{formato_archivo_landing}"

    print(f"[INFO] nombre_archivo_landing={nombre_archivo_landing}")
    print(f"[INFO] ruta_destino={ruta_destino}")
    result_exit['dir_adls_landing'] = ruta_destino
    result_exit['filename'] = nombre_archivo_landing


# COMMAND ----------

#Funcion para consultar si existe el archivo en la ruta especificada por el parametro path
def fileExists(path, file_name):
    file_list = dbutils.fs.ls(path)
    print(file_list)
    for file in file_list:
        if file.name == file_name:
            return True
            break
    return False

# COMMAND ----------

# MAGIC %md
# MAGIC ### Escribir a ruta de destino (landing)

# COMMAND ----------

print("[INFO] moviendo df a temp")
print("primer df que mueve")
display(df)
df.coalesce(1).write.format("csv").options(delimiter=delimitador).mode("overwrite").save(f"{ruta_destino}/temp", header=encabezado)

if is_compuesta:
    print("[INFO] es compuesta")
    
    #Comprobar que exista el archivo con existFile
    if fileExists(ruta_destino, nombre_archivo_landing):
        #Cargar el archivo
        print("[INFO] Cargando df de particion existente")
        dforiginal = spark.read.option("delimiter", delimitador).option("header", encabezado).option("quote",quote).option("encoding", encoding).csv(f"{ruta_destino}/{nombre_archivo_landing}")
        
        #Unir archivo existente con el procesado
        print("[INFO] Uniendo df de particion existente con df procesado")
        df = dforiginal.union(df)
        dbutils.fs.rm(f"{ruta_destino}/temp", recurse=True) 
        df.coalesce(1).write.format("csv").options(delimiter=delimitador).save(f"{ruta_destino}/temp", header=encabezado)
    else:
        print("archivo de particion no existe")

display(df)
print("[INFO] Cargando archivo a landing")

#Crear archivo en directorio de landing
lista = dbutils.fs.ls(f"{ruta_destino}/temp")
filename = [i.name for i in lista if ".csv" in i.name[-4:]][0]
dbutils.fs.mv(f"{ruta_destino}/temp/{filename}", f"{ruta_destino}/{nombre_archivo_landing}")
dbutils.fs.rm(f"{ruta_destino}/temp", recurse=True) 

#Elimina Stage
dbutils.fs.rm(f"{ruta_stage}/{nombre_archivo}", recurse=True) 


# COMMAND ----------

dbutils.notebook.exit(result_exit)
