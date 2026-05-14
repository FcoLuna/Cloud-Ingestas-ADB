# Databricks notebook source
# MAGIC %md
# MAGIC Ingesta Números Portados

# COMMAND ----------

# cargar librerias
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import SparkSession
from pyspark.sql.utils import AnalysisException
from pyspark.sql.functions import current_timestamp

# COMMAND ----------

# MAGIC %md
# MAGIC Leyendo Parámetros

# COMMAND ----------

# # obtener parámetros
#dir_adls = dbutils.widgets.text("dir_adls", "abfss://ingestas@stbigdatadev02.dfs.core.windows.net/data/interacciones/ordenes/oap/detalle_numeros_portados")
#ruta_stage      = dbutils.widgets.text("ruta_stage", "abfss://ingestas@stbigdatadev02.dfs.core.windows.net/data/interacciones/ordenes/oap/detalle_numeros_portados/stage")
#nombre_archivo  = dbutils.widgets.text("nombre_archivo", "detalle-numeros-portados_2025-01-19.csv")
#delimitador     = dbutils.widgets.text("delimitador", ";")
#encabezado      = dbutils.widgets.text("encabezado", "true")
#encoding        = dbutils.widgets.text("encoding", "UTF-8")
#quote           = dbutils.widgets.text("quote", "\"")
#catalogo        = dbutils.widgets.text("catalogo", "bi_ingestas")
#table_name      = dbutils.widgets.text("table_name", "raw_interacciones.detalle_numeros_portados")

###
#starttime_nifi = dbutils.widgets.text("starttime_nifi", "20240523 160152")
#endtime_nifi = dbutils.widgets.text("endtime_nifi", "20240523 160708")
#original_file_size = dbutils.widgets.text("original_file_size", "50567324")
#original_file_date = dbutils.widgets.text("original_file_date", "2024-05-17T13:03:41Z")
#pipelineRunId = dbutils.widgets.text("pipelineRunId", "d953f885-7914-460a-ac01-389000494824")
#catalog_control = dbutils.widgets.text("catalog_control", "bidesarrollo.control.control_ingestas")
###
 
dir_adls = dbutils.widgets.get("dir_adls") 
ruta_stage      = dbutils.widgets.get("ruta_stage")
nombre_archivo  = dbutils.widgets.get("nombre_archivo")
delimitador     = dbutils.widgets.get("delimitador")
encabezado      = dbutils.widgets.get("encabezado").lower()
encoding        = dbutils.widgets.get("encoding")
quote           = dbutils.widgets.get("quote")
catalogo        = dbutils.widgets.get("catalogo")
table_name      = dbutils.widgets.get("table_name")
starttime_nifi = dbutils.widgets.get("starttime_nifi")
endtime_nifi = dbutils.widgets.get("endtime_nifi")
original_file_size = dbutils.widgets.get("original_file_size")
original_file_date = dbutils.widgets.get("original_file_date")
pipelineRunId = dbutils.widgets.get("pipelineRunId")
catalog_control = dbutils.widgets.get("catalog_control")

full_table_name = f"{catalogo}.{table_name}"
full_table_name_append = f"{catalogo}.{table_name}_append"


###


# COMMAND ----------

# MAGIC %md
# MAGIC Lectura de Registros Limpios csv Números Portados

# COMMAND ----------

#df = spark.read.option("delimiter", ";").option("header", True).option("quote", "\"").option("encoding", "UTF-8").csv("abfss://ingestas@stbigdatadev02.dfs.core.windows.net/data/interacciones/ordenes/oap/detalle_numeros_portados/stage/archivo_limpio")
status = ""
desc_status = ""
try:
    status = "[OK]"
    desc_status = "[OK]"
    df = spark.read.option("delimiter", delimitador).option("header", encabezado).option("quote", quote).option("encoding", encoding).csv(f"{ruta_stage}/archivo_limpio")
    df.show(5, truncate=False)
except Exception as e:
    # Capturar cualquier excepción y mostrar un mensaje de error
    status = "[ERROR]"
    desc_status = f"[ERROR] : {e}"
    print(status)
    print(desc_status)    
    print(f"Se produjo un error: {e}")

# COMMAND ----------

from pyspark.sql import functions as F

try:
    df_tmp1 = df.select(
        df['_c0'], 
        F.split(df['_c0'], ',').alias('split_column')
    )
    display(df_tmp1)
except Exception as e:
    # Capturar cualquier excepción y mostrar un mensaje de error
    status = "[ERROR]"
    desc_status = f"[ERROR] : {e}"
    print(status)
    print(desc_status)    
    print(f"Se produjo un error: {e}")

# COMMAND ----------

try:
    df_transformed = df_tmp1.select(
     *[F.col("split_column")[i].alias(f"col_{i+1}") for i in range(len(df_tmp1.head()[0]))]
    )
    # Mostrar el DataFrame transformado
    df_transformed.show(truncate=False)
    df_transformed.printSchema()
except Exception as e:
    # Capturar cualquier excepción y mostrar un mensaje de error
    status = "[ERROR]"
    desc_status = f"[ERROR] : {e}"
    print(status)
    print(desc_status)    
    print(f"Se produjo un error: {e}")

# COMMAND ----------

try:
    df_tmp2 = df_transformed.select(
    "col_1", "col_2", "col_3", "col_4", "col_5", "col_6", "col_7", "col_8",
    "col_9", "col_10", "col_11", "col_12", "col_13", "col_14", "col_15"
    )
    display(df_tmp2)
except Exception as e:
    # Capturar cualquier excepción y mostrar un mensaje de error
    status = "[ERROR]"
    desc_status = f"[ERROR] : {e}"
    print(status)
    print(desc_status)    
    print(f"Se produjo un error: {e}")

# COMMAND ----------

##Creando el valor de Bigdata ctrl_id con la fecha actual
from datetime import datetime

try:
    current_time = datetime.now()  # O usando time.time() para segundos desde la época
    print(current_time)
    formatted_time = current_time.strftime("%Y%m%d%H%M%S")
    counter = "001"
    bigdata_ctrl_id = "{}{}".format(formatted_time,counter)
    print(bigdata_ctrl_id)
except Exception as e:
    # Capturar cualquier excepción y mostrar un mensaje de error
    status = "[ERROR]"
    desc_status = f"[ERROR] : {e}"
    print(status)
    print(desc_status)    
    print(f"Se produjo un error: {e}")

# COMMAND ----------

try:
    df_tmp2 = df_tmp2 \
    .withColumnRenamed("col_1", "id_operador") \
    .withColumnRenamed("col_2", "hora_envio_solicitud") \
    .withColumnRenamed("col_3", "nt") \
    .withColumnRenamed("col_4", "tipo_de_servicio_donante") \
    .withColumnRenamed("col_5", "tipo_de_servicio_receptor") \
    .withColumnRenamed("col_6", "region") \
    .withColumnRenamed("col_7", "comuna") \
    .withColumnRenamed("col_8", "localidad") \
    .withColumnRenamed("col_9", "canal_de_atencion") \
    .withColumnRenamed("col_10", "modalidad") \
    .withColumnRenamed("col_11", "rut_del_cliente") \
    .withColumnRenamed("col_12", "id_portabilidad") \
    .withColumnRenamed("col_13", "receptor") \
    .withColumnRenamed("col_14", "donante") \
    .withColumnRenamed("col_15", "estado") 
    # .withColumn("bigdata_close_date", current_date()) \
    # .withColumn("bigdata_ctrl_id", lit(bigdata_ctrl_id))
    display(df_tmp2)
except Exception as e:
    # Capturar cualquier excepción y mostrar un mensaje de error
    status = "[ERROR]"
    desc_status = f"[ERROR] : {e}"
    print(status)
    print(desc_status)    
    print(f"Se produjo un error: {e}")


# COMMAND ----------

df_tmp2.printSchema()

# COMMAND ----------

from pyspark.sql.functions import col, concat, substring, lit

try:
        df_tmp2 = df_tmp2 \
        .withColumn(
        "hora_envio_solicitud", 
        concat(
        substring(col("hora_envio_solicitud"), 7, 4),
        lit("-"),
        substring(col("hora_envio_solicitud"), 0, 2),
        lit("-"),
        substring(col("hora_envio_solicitud"), 4, 2),
        lit(" "),
        substring(col("hora_envio_solicitud"), 12, 2),
        lit(":"),
        substring(col("hora_envio_solicitud"), 15, 2),
        lit(":"),
        substring(col("hora_envio_solicitud"), 18, 2)
        )
        ) 
        display(df_tmp2)

except Exception as e:
    # Capturar cualquier excepción y mostrar un mensaje de error
    status = "[ERROR]"
    desc_status = f"[ERROR] : {e}"
    print(status)
    print(desc_status)    
    print(f"Se produjo un error: {e}")

# COMMAND ----------

df_tmp3 = df_tmp2
try:
    df_tmp3 = df_tmp3 \
        .withColumn("bigdata_close_date", current_timestamp()) \
        .withColumn("bigdata_ctrl_id", lit(bigdata_ctrl_id))

    df_tmp3 = df_tmp3 \
        .withColumn("bigdata_ctrl_id", col("bigdata_ctrl_id").cast("bigint"))

except Exception as e:
    # Capturar cualquier excepción y mostrar un mensaje de error
    status = "[ERROR]"
    desc_status = f"[ERROR] : {e}"
    print(status)
    print(desc_status)    
    print(f"Se produjo un error: {e}")

# COMMAND ----------

#df_tmp3.show(5, False)

# COMMAND ----------

# MAGIC %md
# MAGIC Registro en Tabla en Catalogo

# COMMAND ----------

# Suponiendo que tienes un DataFrame llamado df y una conexión activa de Spark
spark = SparkSession.builder.appName("DataValidationAndInsert").getOrCreate()

# COMMAND ----------

# MAGIC %sql
# MAGIC --DESCRIBE bi_ingestas.raw_interacciones.detalle_numeros_portados_append;

# COMMAND ----------

#df_tmp3.printSchema()

# COMMAND ----------

# Validar si el DataFrame tiene registros
try:
    if df_tmp3.count() > 0:
    # Verificar si la tabla existe en el catálogo
    # Intentamos describir la tabla para ver si existe

        # Si no ocurre excepción, la tabla existe, entonces se puede hacer el insert
        print(f"Se procederá con el append a la tabla {full_table_name_append} .")
        df_tmp3.write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable(full_table_name_append)
  
    else:
        print("El DataFrame está vacío. No se insertarán registros.")

except Exception as e:
    # Capturar cualquier excepción y mostrar un mensaje de error
    status = "[ERROR]"
    desc_status = f"[ERROR] : {e}"
    print(status)
    print(desc_status)    
    print(f"Se produjo un error: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC Borrando Duplicados Detalles Solicitudes Portabilidad

# COMMAND ----------

#df_duplicados = spark.sql("SELECT * FROM bi_ingestas.raw_interacciones.detalle_solicitud_portabilidad")
try:
    df_duplicados = spark.sql(f"""SELECT id_operador, hora_envio_solicitud, nt, tipo_de_servicio_donante, tipo_de_servicio_receptor, region, comuna, localidad, canal_de_atencion, modalidad, rut_del_cliente, id_portabilidad, receptor, donante, estado FROM {full_table_name_append}""")
except Exception as e:
    # Capturar cualquier excepción y mostrar un mensaje de error
    status = "[ERROR]"
    desc_status = f"[ERROR] : {e}"
    print(status)
    print(desc_status)    
    print(f"Se produjo un error: {e}")

# COMMAND ----------

# Realizar un groupBy en todos los campos de la tabla
from pyspark.sql import functions as F

try:
    # Realizar un groupBy en todos los campos de la tabla
    df_sin_duplicados_group = df_duplicados.groupBy(
        "id_operador", 
        "hora_envio_solicitud", 
        "nt", 
        "tipo_de_servicio_donante", 
        "tipo_de_servicio_receptor", 
        "region", 
        "comuna", 
        "localidad", 
        "canal_de_atencion",        
        "modalidad", 
        "rut_del_cliente", 
        "id_portabilidad", 
        "receptor", 
        "donante", 
        "estado"
    ).agg(
        *[F.first(col).alias(f"first_{col}") for col in df_duplicados.columns]
    )

    df_sin_duplicados = df_sin_duplicados_group.select(
        "id_operador", "hora_envio_solicitud", "nt", "tipo_de_servicio_donante", 
        "tipo_de_servicio_receptor", "region", "comuna", "localidad", "canal_de_atencion", "modalidad", "rut_del_cliente", "id_portabilidad", "receptor", "donante", "estado"
    )

    # Sobrescribir la tabla con los resultados sin duplicados
    df_sin_duplicados.write.format("delta").mode("overwrite").option("mergeSchema", "true").saveAsTable(full_table_name)

except Exception as e:
    # Capturar cualquier excepción y mostrar un mensaje de error
    status = "[ERROR]"
    desc_status = f"[ERROR] : {e}"
    print(status)
    print(desc_status)    
    print(f"Se produjo un error: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC Actualización Archivo Last_ingest_time.txt

# COMMAND ----------

try:
    dbutils.fs.rm(f"{dir_adls}/last_ingest_time.txt", True)
except Exception as e:
    # Capturar cualquier excepción y mostrar un mensaje de error
    status = "[ERROR]"
    desc_status = f"[ERROR] : {e}"
    print(status)
    print(desc_status)    
    print(f"Se produjo un error: {e}")


# COMMAND ----------

try:
    current_timestamp = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + 'Z'
    valor_fecha = current_timestamp 
    contents = "timestamp;\n"+valor_fecha+";"
    dbutils.fs.put(f"{dir_adls}/last_ingest_time.txt", contents, overwrite=True)
except Exception as e:
    # Capturar cualquier excepción y mostrar un mensaje de error
    status = "[ERROR]"
    desc_status = f"[ERROR] : {e}"
    print(status)
    print(desc_status)    
    print(f"Se produjo un error: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC Catalogo de Control

# COMMAND ----------

## Registro Catálogo de Control

big_data_ctrl_id = str(bigdata_ctrl_id) 
bigdata_ctrl_id = str(bigdata_ctrl_id) 
process_name = table_name
data_source_type = "file"
data_source_name = f"{nombre_archivo}"
endtime_spark = endtime_nifi    
starttime_spark = starttime_nifi
process_type = "normal"
final_file_size = original_file_size
original_row_count = df_tmp2.count()
final_row_count = original_row_count
dif_row_count = original_row_count - final_row_count

original_file_size = str(original_file_size)
final_file_size = str(final_file_size)
original_row_count = str(original_row_count)
final_row_count = str(final_row_count)
dif_row_count = str(dif_row_count)

final_number_of_files = str(original_row_count)
end_file_name = table_name
insert_data_ctrl_date = current_time.strftime("%Y-%m-%d")
hdfs_path = f"{dir_adls}/raw"

#Formato de Variable con Fechas
original_file_datef = datetime.strptime(original_file_date, "%Y-%m-%dT%H:%M:%SZ")
original_file_date2 = original_file_datef.strftime("%Y-%m-%d %H:%M:%S")

original_file_datef = datetime.strptime(original_file_date, "%Y-%m-%dT%H:%M:%SZ")
original_file_date2 = original_file_datef.strftime("%Y-%m-%d %H:%M:%S")

# Utilizamos substring para extraer las partes de la fecha y hora
anio = starttime_nifi[:4]
mes = starttime_nifi[4:6]
dia = starttime_nifi[6:8]
hora = starttime_nifi[9:11]
minuto = starttime_nifi[11:13]
segundo = starttime_nifi[13:15]
# Formateamos la fecha en el nuevo formato
starttime_nifi2 = f"{anio}-{mes}-{dia} {hora}:{minuto}:{segundo}"

# Utilizamos substring para extraer las partes de la fecha y hora
anio = endtime_nifi[:4]
mes = endtime_nifi[4:6]
dia = endtime_nifi[6:8]
hora = endtime_nifi[9:11]
minuto = endtime_nifi[11:13]
segundo = endtime_nifi[13:15]
endtime_nifi2 = f"{anio}-{mes}-{dia} {hora}:{minuto}:{segundo}"

print("original_file_date2 = " + str(original_file_date2))
print("starttime_nifi2 = " + str(starttime_nifi2))
print("endtime_nifi2 = " + str(endtime_nifi2))

# Convert strings to datetime objects
starttime_nifi_dt = datetime.strptime(starttime_nifi2, "%Y-%m-%d %H:%M:%S")
endtime_nifi_dt = datetime.strptime(endtime_nifi2, "%Y-%m-%d %H:%M:%S")

# Calculate the difference
totaltime_nifi_diff = endtime_nifi_dt - starttime_nifi_dt
print("totaltime_nifi_diff = " + str(totaltime_nifi_diff))
totaltime_nifi_diff_seconds = totaltime_nifi_diff.total_seconds()
print("totaltime_nifi_diff_seconds = " + str(totaltime_nifi_diff_seconds))

totaltime_nifi = str(totaltime_nifi_diff_seconds)

totaltime_spark = float(totaltime_nifi_diff_seconds)
totaltime_process = float(totaltime_nifi_diff_seconds)

print("totaltime_spark = " + str(totaltime_spark))
print("totaltime_process = " + str(totaltime_process))

print("bigdata_ctrl_id " + str(bigdata_ctrl_id))
print("process_name " + str(process_name))
print("data_source_type " + str(data_source_type))
print("data_source_name " + str(data_source_name))
print("original_file_date " + str(original_file_date))
print("starttime_nifi " + str(starttime_nifi))
print("endtime_nifi " + str(endtime_nifi))
print("totaltime_nifi " + str(totaltime_nifi))
print("starttime_spark " + str(starttime_spark))
print("endtime_spark " + str(endtime_spark))
print("totaltime_spark " + str(totaltime_spark))
print("totaltime_process " + str(totaltime_process))
print("insert_data_ctrl_date " + str(insert_data_ctrl_date))
print("process_type " + str(process_type))
print("original_file_size " + str(original_file_size))
print("final_file_size " + str(final_file_size))
print("original_row_count " + str(original_row_count))
print("final_row_count " + str(final_row_count))
print("dif_row_count " + str(dif_row_count))
print("final_number_of_files " + str(final_number_of_files))
print("end_file_name " + str(end_file_name))
print("hdfs_path " + str(hdfs_path))
print("pipelineRunId " + str(pipelineRunId))
print("status " + str(status))
print("desc_status " + str(desc_status))
print("catalog_control" + str(catalog_control))

print(status, desc_status, big_data_ctrl_id, process_name, data_source_type, data_source_name, original_file_date2, starttime_nifi2, endtime_nifi2, totaltime_nifi, starttime_spark, process_type, original_file_size, final_file_size, original_row_count, final_row_count, dif_row_count, final_number_of_files, end_file_name, insert_data_ctrl_date, hdfs_path, pipelineRunId,  bigdata_ctrl_id)

data_control = [(big_data_ctrl_id, process_name, data_source_type, data_source_name, original_file_date2,
starttime_nifi2, endtime_nifi2, totaltime_nifi, starttime_spark, endtime_spark ,
totaltime_spark, totaltime_process, insert_data_ctrl_date, process_type, 
original_file_size, final_file_size, original_row_count, final_row_count, 
dif_row_count, final_number_of_files, end_file_name, hdfs_path,
pipelineRunId, status, desc_status,  bigdata_ctrl_id )]

columns = ["big_data_ctrl_id", "process_name", "data_source_type", "data_source_name", "original_file_date",
"starttime_nifi", "endtime_nifi", "totaltime_nifi", "starttime_spark", "endtime_spark", 
"totaltime_spark", "totaltime_process", "insert_data_ctrl_date", "process_type", 
"original_file_size", "final_file_size", "original_row_count", "final_row_count", 
"dif_row_count", "final_number_of_files", "end_file_name", "hdfs_path", 
"pipelineRunId", "status", "desc_status", "bigdata_ctrl_id"]

df_data_control = spark.createDataFrame(data_control, columns)

df_data_control = df_data_control \
.withColumn("original_file_date", from_unixtime(unix_timestamp(col("original_file_date"), "yyyy-MM-dd HH:mm:ss"), "yyyy-MM-dd HH:mm:ss")) \
.withColumn("starttime_nifi", from_unixtime(unix_timestamp(col("starttime_nifi"), "yyyy-MM-dd HH:mm:ss"), "yyyy-MM-dd HH:mm:ss")) \
.withColumn("endtime_nifi", from_unixtime(unix_timestamp(col("endtime_nifi"), "yyyy-MM-dd HH:mm:ss"), "yyyy-MM-dd HH:mm:ss")) \
.withColumn("totaltime_spark", col("totaltime_spark").cast("float")) \
.withColumn("totaltime_process", col("totaltime_process").cast("float")) \
.withColumn("big_data_ctrl_id", col("big_data_ctrl_id").cast("string")) \
.withColumn("bigdata_ctrl_id", col("bigdata_ctrl_id").cast("string"))

df_data_control.write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable(catalog_control)

