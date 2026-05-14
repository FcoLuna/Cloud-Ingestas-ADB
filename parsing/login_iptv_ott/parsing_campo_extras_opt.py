# Databricks notebook source
# MAGIC %md
# MAGIC ### Parsing para Ingesta con campos extras
# MAGIC El archivo que viene con esta ingesta tiene dos campos adicionales nulos haciendo que se quiebre la inserción de los datos en el notebook de validaciones prelanding. Para esto se hace una nueva lectura, previa a validaciones prelanding.

# COMMAND ----------

# cargar librerias
from pyspark.sql.functions import *
from pyspark.sql.types import *
import json
import re
from collections import defaultdict
from datetime import datetime
from pyspark.sql.functions import split, regexp_replace, concat, lit

# COMMAND ----------

# obtener parámetros

ruta_stage      = dbutils.widgets.get("ruta_stage")
nombre_archivo  = dbutils.widgets.get("nombre_archivo")
delimitador     = dbutils.widgets.get("delimitador")
encabezado      = dbutils.widgets.get("encabezado").lower()
encoding        = dbutils.widgets.get("encoding")
quote           = dbutils.widgets.get("quote")
origen          = dbutils.widgets.get("origen")
ruta_landing    =dbutils.widgets.get("ruta_landing")


# COMMAND ----------

'''ruta_stage      = "abfss://ingestas@stbigdatadev02.dfs.core.windows.net/data/interacciones/canales/gvp/login/stage/temp_ott"
nombre_archivo  = "gvp_login_ott"
delimitador     = ";"
encabezado      = "true"
encoding        = "UTF-8"
quote           = "\""
origen          = "ott"
ruta_landing    ="abfss://ingestas@stbigdatadev02.dfs.core.windows.net/data/interacciones/canales/gvp/login/landing"
'''



# COMMAND ----------

current_datetime = datetime.now()
current_date = current_datetime.strftime("%Y%m%d%H%M%S")
print(current_date)

# COMMAND ----------

#setemos directorio donde se encuentran los archivos
directorio = dbutils.fs.ls(ruta_stage)
print(directorio)


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

json_schema_path = ruta_stage.replace("/stage/temp_"+origen,"") + "/" + ruta_stage.replace("/stage/temp_"+origen,"").split("/")[-1] + ".json"
check_file = file_exists(json_schema_path)

# COMMAND ----------

#preprocesamiento, transformacion y carga de archivos

agrupados_por_nombre = {}
patron_fecha = re.compile(r'\d{4}-\d{2}-\d{2}')
archivos_por_fecha = defaultdict(list)
dataframes = []
matrizSalida = []
dicMatriz = {}
columnas_seleccionadas = []
checkSchema = False
#dfcombinado = []

# agrupar archivos por fecha contenida en el nombre del archivo
for archivo in directorio:
    fecha_encontrada = patron_fecha.search(archivo.name)
    if fecha_encontrada:
        fecha = fecha_encontrada.group()
        archivos_por_fecha[fecha].append(archivo)

#carga dataframe con los archivos agrupados por fecha
#seta el schema definido en el json para eliminar campos adicionales si es que trae
#Escribe archivos agrupados segun cantidad de fechas distintas que vengan
#retorna como salida las rutas y nombres de archivos distintos creados al notebook validaciones_transformaciones prelanding
schema = StructType([
    StructField("datetime", StringType(), True),
    StructField("devicetypeused", StringType(), True),
    StructField("deviceid", StringType(), True),
    StructField("statusmessage", StringType(), True),
    StructField("statuscode", StringType(), True),
    StructField("usertype", StringType(), True),
    StructField("uniqueusercode", StringType(), True),
    StructField("method", StringType(), True),
    StructField("sessionid", StringType(), True),
    StructField("userid", StringType(), True),
    StructField("profileid", StringType(), True)
])

schemaLanding = StructType([
    StructField("datetime", StringType(), True),
    StructField("devicetypeused", StringType(), True),
    StructField("deviceid", StringType(), True),
    StructField("statusmessage", StringType(), True),
    StructField("statuscode", StringType(), True),
    StructField("usertype", StringType(), True),
    StructField("uniqueusercode", StringType(), True),
    StructField("method", StringType(), True),
    StructField("sessionid", StringType(), True),
    StructField("userid", StringType(), True),
    StructField("profileid", StringType(), True),
    StructField("filename_spark", StringType(), True),
    StructField("ts", StringType(), True)
])

print(f"largo arreglo {len(archivos_por_fecha)}")

for fecha, archivos in archivos_por_fecha.items():
    print(f"Archivos para la fecha {fecha}: {archivos}")
    #Obtenemos el TS 
    current_datetime = datetime.now().strftime("%Y%m%d%H%M%S")
    # Procesar cada archivo y agregarlo a la lista de DataFrames
    #filename_spark = split(archivos[0].name,".")[0]
    
    dataframes = [
        spark.read.csv(archivo.path, header=encabezado, encoding=encoding, sep=delimitador, quote=quote, schema=schema)
        .withColumn("filename_spark",
                    concat(lit("Chile"), lit(origen), lit("_LogLogin_"), split(split(lit(archivo.name),".csv")[0],"_")[3], lit("_") , regexp_replace(split(lit(archivo.name), "_")[2],"-",""), lit(".csv"))
        )
        .withColumn("ts", lit(current_datetime)
        )
        for archivo in archivos
    ] 
    print(f"datos del dia :{fecha} cant archivos del dia :{len(archivos)}")

    df_combinado = dataframes[0]
    for df in dataframes[1:]:
        df_combinado = df_combinado.union(df)
    
    print(f"largo df_combinado :{df_combinado.count()}")

     
    fechaDir = fecha.replace("-","")
    ruta_stage2      = ruta_stage.replace('temp_'+origen,origen)
    ruta_stage2      = ruta_stage2 +"/"+fechaDir
    fechalanding = fecha.replace("-","/")
    print(f"fechadir: {fechalanding}")
    print(f"ruta_Landing: {ruta_landing}/{fechalanding}/{nombre_archivo}_{fechaDir}.csv")
     
    landing_path = f"{ruta_landing}/{fechalanding}/{nombre_archivo}_{fechaDir}.csv"
    try:
        dbutils.fs.ls(landing_path)
        dfLan = spark.read.csv(landing_path, header=encabezado, encoding=encoding, sep=delimitador, quote=quote, schema=schemaLanding)
    except:
        dfLan = spark.createDataFrame([], schemaLanding)
     
    dfLan.printSchema()
    print(f"largo dfLan :{dfLan.count()}")
    print(f"largo df_combinado :{df_combinado.count()}")
    #une data de landing con el stage
    df_combinado = df_combinado.union(dfLan) 
    
    (df_combinado.coalesce(1)
        .write.mode("overwrite")
        .format("csv")
        .options(delimiter=delimitador)
        .save(f"{ruta_stage2}", header=encabezado.capitalize())
     )
    # mover archivo a ruta stage renombrado 
    file = [f.path for f in dbutils.fs.ls(f"{ruta_stage2}") if f.name.startswith("part-00000")][0]
    nombre_archivo2 = nombre_archivo +"_"+ fechaDir + ".csv" 
    #landing_path = f"{ruta_landing}/{fechalanding}/{nombre_archivo2}_{fechaDir}.csv"
    dbutils.fs.mv(file,f"{ruta_stage2}/{nombre_archivo2}")


    #mover Archivo a Landing
    landing_path_out = f"{ruta_landing}/{fechalanding}/"
    print(f"landing_path_out: {landing_path_out}")
    dbutils.fs.mv(f"{ruta_stage2}/{nombre_archivo2}",f"{landing_path}")

    #agregaArchivo a matriz de salida
    dicMatriz = {"nombreFile": nombre_archivo2, "pathFile": landing_path_out}
    matrizSalida.append(dicMatriz)

   



# COMMAND ----------

# quitar archivo temporal después de renombrado
#ruta_stage      = ruta_stage.replace(origen,'temp_'+origen)
print(ruta_stage)
dbutils.fs.rm(f"{ruta_stage}/", recurse=True)

# COMMAND ----------

import json

print(matrizSalida)
# Convertir el arreglo a formato JSON
arreglo_json = json.dumps(matrizSalida)

# Retornar el arreglo como salida del notebook
dbutils.notebook.exit(arreglo_json)

