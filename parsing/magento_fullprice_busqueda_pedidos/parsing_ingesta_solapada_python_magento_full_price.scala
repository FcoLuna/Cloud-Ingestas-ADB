// Databricks notebook source
// MAGIC %md
// MAGIC **PARSING INGESTA SOLAPADA**
// MAGIC
// MAGIC Parsing para Ingesta que particiona desde un campo fecha
// MAGIC El archivo que viene con esta ingesta tiene un campo fecha el cual se utiliza para particionar la data. El Notebook agrupa los datos por fechas, creando n archivos segun n fechas distintas, decarta los campos que viene con fechas nula para eviar que se caiga el proceso.

// COMMAND ----------

// DBTITLE 1,carga librerias y crea sparksession
// MAGIC %python
// MAGIC #from pyspark.sql import SparkSession
// MAGIC from pyspark.sql.functions import col, struct, collect_list
// MAGIC from pyspark.sql.types import *
// MAGIC import json
// MAGIC from pyspark.sql import Row
// MAGIC from pyspark.sql import DataFrame
// MAGIC from pyspark.sql.utils import AnalysisException
// MAGIC from collections import defaultdict
// MAGIC from pyspark.dbutils import DBUtils
// MAGIC from pyspark.sql.types import StructType, StructField, StringType
// MAGIC import os
// MAGIC
// MAGIC
// MAGIC
// MAGIC # Crear una sesión de Spark
// MAGIC #spark = SparkSession.builder.appName("INGESTA_SOLAPADA").getOrCreate()
// MAGIC '''spark = SparkSession.builder \
// MAGIC     .config("spark.connect.grpc.maxInboundMessageSize", "256m") \
// MAGIC     .getOrCreate()'''
// MAGIC '''spark = SparkSession.builder \
// MAGIC     .config("spark.rpc.message.maxSize", "512m") \
// MAGIC     .getOrCreate()'''
// MAGIC #spark = SparkSession.builder \
// MAGIC #    .appName("Sesion Spark") \
// MAGIC #    .getOrCreate()
// MAGIC

// COMMAND ----------

// MAGIC %python
// MAGIC # Configuración de rutas
// MAGIC
// MAGIC ruta_adls = dbutils.widgets.get("ruta_adls")
// MAGIC out_dir = "abfss://ingestas@stbigdatadev02.dfs.core.windows.net/data/gestion_recursos/operacion_red/fatem/reporte_inicio_cliente_falla_masiva_fo/landing"
// MAGIC campo_particion = dbutils.widgets.get("partition_date_column")
// MAGIC particion_tabla = dbutils.widgets.get("partition_landing")
// MAGIC formato_archivo = dbutils.widgets.get("file_extencion")
// MAGIC encabezado = dbutils.widgets.get("header")
// MAGIC delimitador = dbutils.widgets.get("delimiter")
// MAGIC nombre_archivo = dbutils.widgets.get("file_name")
// MAGIC ruta_archivo = f"{ruta_adls}/stage/tmp/{nombre_archivo}"
// MAGIC schema = StructType([])
// MAGIC date_format_file = dbutils.widgets.get("date_format_file")
// MAGIC tipo_ingesta = dbutils.widgets.get("tipo_ingesta")
// MAGIC
// MAGIC # Configuración de rutas
// MAGIC '''ruta_adls = "abfss://ingestas@stbigdatadev02.dfs.core.windows.net/data/interacciones/canales/magento/fullprice_busqueda_pedidos"
// MAGIC out_dir = "abfss://ingestas@stbigdatadev02.dfs.core.windows.net/data/gestion_recursos/operacion_red/fatem/reporte_inicio_cliente_falla_masiva_fo/landing"
// MAGIC campo_particion = "periodo"
// MAGIC particion_tabla = "m"
// MAGIC formato_archivo = "csv"
// MAGIC encabezado = "true"
// MAGIC delimitador = ","
// MAGIC nombre_archivo = "csv_fullprice_busqueda_pedidos_04-06-2025.csv"
// MAGIC ruta_archivo = f"{ruta_adls}/stage/tmp/{nombre_archivo}"
// MAGIC schema = StructType([])
// MAGIC date_format_file = "dd-MM-yyyy"
// MAGIC tipo_ingesta = "i"'''
// MAGIC
// MAGIC        
// MAGIC # Función para leer un archivo CSV y"

// COMMAND ----------

// DBTITLE 1,define funciones
// MAGIC %python
// MAGIC #Valida existencia de la ruta en Databricks
// MAGIC def file_exists(path: str) -> bool:
// MAGIC     try:
// MAGIC         dbutils.fs.ls(path)  # Lista archivos en la ruta
// MAGIC         return True
// MAGIC     except Exception as e:
// MAGIC         if "java.io.FileNotFoundException" in str(e):
// MAGIC             return False
// MAGIC         else:
// MAGIC             raise e  # Lanza la excepción si es otro error
// MAGIC
// MAGIC #Define NUm Particiones si archivo es mayor a 128MB
// MAGIC '''def num_partitions_calc(tmp: str) -> int:
// MAGIC     spark = SparkSession.getActiveSession()
// MAGIC     if spark is None:
// MAGIC         raise RuntimeError("No active SparkSession")
// MAGIC     
// MAGIC     hadoop_conf = spark._jsc.hadoopConfiguration()
// MAGIC     path = spark._jvm.org.apache.hadoop.fs.Path(tmp)
// MAGIC     fs = path.getFileSystem(hadoop_conf)
// MAGIC     data_size = fs.getContentSummary(path).getLength()
// MAGIC     
// MAGIC     if data_size < 10737418240:  # 10 GB
// MAGIC         num_partitions = math.ceil(data_size / 1073741824)  # 1 GB
// MAGIC         num_partitions = 1 if num_partitions == 0 else num_partitions
// MAGIC     else:
// MAGIC         num_partitions = math.ceil(data_size / 10737418240)  # 10 GB
// MAGIC         num_partitions = 1 if num_partitions == 0 else num_partitions
// MAGIC     
// MAGIC     return int(num_partitions)'''
// MAGIC
// MAGIC

// COMMAND ----------

// MAGIC %python
// MAGIC
// MAGIC # Función para verificar si un archivo existe (simplificada)
// MAGIC def file_exists(path):
// MAGIC     try:
// MAGIC         # En un entorno real, usaría el filesystem de Azure (ABFSS)
// MAGIC         # Esto es solo una simulación
// MAGIC         dbutils.fs.ls(path)
// MAGIC         return True
// MAGIC     except:
// MAGIC         return False
// MAGIC
// MAGIC # Obtener ruta del schema JSON
// MAGIC #json_schema_path = ruta_stage.replace("/stage/tmp", "") + "/" + ruta_stage.replace("/stage/tmp", "").split("/")[-1] + ".json"
// MAGIC json_schema_path = ruta_adls + "/" + ruta_adls.split("/")[-1] + ".json"
// MAGIC print(f"ruta: {json_schema_path}")
// MAGIC
// MAGIC # Verificar si el archivo existe
// MAGIC check_file = file_exists(json_schema_path)
// MAGIC if check_file:
// MAGIC     # Leer el archivo JSON como texto
// MAGIC     file_content = spark.read.text(json_schema_path).collect()
// MAGIC     file_content = "".join([row.value for row in file_content])
// MAGIC     
// MAGIC     # Convertir la cadena JSON a diccionario
// MAGIC     data = json.loads(file_content)
// MAGIC     
// MAGIC     # Extraer las columnas de la estructura JSON
// MAGIC     columnas_seleccionadas = []
// MAGIC     schema_fields = []
// MAGIC     
// MAGIC     if "fields" in data:
// MAGIC         fields = data["fields"]
// MAGIC         for field in fields:
// MAGIC             if "name" in field:
// MAGIC                 column_name = field["name"]
// MAGIC                 columnas_seleccionadas.append(column_name)
// MAGIC                 schema_fields.append(StructField(column_name, StringType(), nullable=True))
// MAGIC     
// MAGIC     print(f"Número inicial de columnas: {len(columnas_seleccionadas)}")
// MAGIC     
// MAGIC     # Eliminar columnas específicas si la ingesta es incremental
// MAGIC     if tipo_ingesta == "i" or tipo_ingesta == "incremental":
// MAGIC         print("Eliminanando columnas especificas")
// MAGIC         columns_to_remove = {"filename_spark", "ts"}
// MAGIC     columnas_seleccionadas = [col for col in columnas_seleccionadas if col not in columns_to_remove]
// MAGIC     schema_fields = [field for field in schema_fields if field.name not in columns_to_remove]
// MAGIC     
// MAGIC     print(f"Número de columnas después de eliminar: {len(columnas_seleccionadas)}")
// MAGIC     
// MAGIC     # Mostrar columnas restantes
// MAGIC     for col in columnas_seleccionadas:
// MAGIC         print(col)
// MAGIC     
// MAGIC     # Crear schema final
// MAGIC     schema = StructType(schema_fields)

// COMMAND ----------

// MAGIC %python
// MAGIC from pyspark.sql.functions import col
// MAGIC
// MAGIC # Verificar existencia del archivo
// MAGIC checkFile = file_exists(ruta_archivo)
// MAGIC print(checkFile)
// MAGIC
// MAGIC # Leer el DataFrame (comentado el if en el original)
// MAGIC df = spark.read.format(formato_archivo) \
// MAGIC     .option("inferSchema", "false") \
// MAGIC     .option("multiline", "true") \
// MAGIC     .option("delimiter", delimitador) \
// MAGIC     .option("header", encabezado) \
// MAGIC     .schema(schema) \
// MAGIC     .load(ruta_archivo)
// MAGIC
// MAGIC # Mostrar el schema
// MAGIC df.printSchema()
// MAGIC print(df.count())
// MAGIC
// MAGIC # Corregir campos nulos en campo de partición
// MAGIC print(campo_particion)
// MAGIC dfCorregido = df.filter(col(campo_particion).isNotNull())
// MAGIC print(dfCorregido.count())

// COMMAND ----------

// MAGIC %python
// MAGIC from pyspark.sql.functions import col, date_format, to_date, to_timestamp, when, when
// MAGIC
// MAGIC # Set Spark configuration
// MAGIC #spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")
// MAGIC
// MAGIC # Show distinct values of partition column
// MAGIC dfCorregido.select(campo_particion).distinct().show(20, False)
// MAGIC
// MAGIC # Create auxiliary DataFrame
// MAGIC dfAux = dfCorregido
// MAGIC
// MAGIC # Format date column
// MAGIC #from pyspark.sql.functions import col, date_format, to_date, to_timestamp, when
// MAGIC
// MAGIC dfFormateado = dfCorregido.withColumn(
// MAGIC     campo_particion,
// MAGIC     date_format(
// MAGIC         when(
// MAGIC             col(campo_particion).cast("timestamp").isNotNull(),  # Verifica si puede convertirse a timestamp
// MAGIC             to_date(col(campo_particion).cast("timestamp"))
// MAGIC         ).otherwise(
// MAGIC             to_date(col(campo_particion))  # Si es `date`, lo convierte directamente
// MAGIC         ),
// MAGIC         "dd/MM/yyyy"
// MAGIC     )
// MAGIC )
// MAGIC '''dfFormateado = dfCorregido.withColumn(
// MAGIC     campo_particion,
// MAGIC     date_format(
// MAGIC         to_date(col(campo_particion)),
// MAGIC         "dd/MM/yyyy"  # This should be defined as your date format string, e.g. "yyyy-MM-dd"
// MAGIC     )
// MAGIC )'''
// MAGIC '''dfFormateado = dfCorregido.withColumn(
// MAGIC     campo_particion,
// MAGIC         to_date(col(campo_particion))
// MAGIC         #date_format_file  # This should be defined as your date format string, e.g. "yyyy-MM-dd"
// MAGIC )'''
// MAGIC
// MAGIC print(f'{dfFormateado.count()}')
// MAGIC
// MAGIC print("data frame original")
// MAGIC dfCorregido.select(campo_particion).distinct().show(20, False)
// MAGIC # Print formatted dates
// MAGIC print("Salida Fecha Formateada")
// MAGIC dfFormateado.select(campo_particion).distinct().show(20, False)
// MAGIC dfFormateado.select(campo_particion).show(20,False)
// MAGIC print("data frame aux ")
// MAGIC dfAux.select(campo_particion).distinct().show(20, False)
// MAGIC       

// COMMAND ----------

// MAGIC %python
// MAGIC from pyspark.sql import functions as F
// MAGIC from pyspark.sql.types import *
// MAGIC import math
// MAGIC
// MAGIC # Configuración mejorada para manejar grandes volúmenes de datos
// MAGIC ##spark.conf.set("spark.rpc.message.maxSize", "1024")  # Aumentar tamaño máximo de mensajes (en MB)
// MAGIC #spark.conf.set("spark.driver.maxResultSize", "4g")   # Aumentar memoria para resultados del driver
// MAGIC
// MAGIC def num_partitions_calc(tmp_path: str) -> int:
// MAGIC     """Calcula el número de particiones basado en el tamaño de los datos"""
// MAGIC     try:
// MAGIC         file_info = dbutils.fs.ls(tmp_path)
// MAGIC         data_size = sum(f.size for f in file_info)
// MAGIC         print(f"Tamaño total de datos: {data_size/1024/1024:.2f} MB")
// MAGIC         
// MAGIC         # Cálculo más conservador de particiones
// MAGIC         if data_size < 134217728:  # 128 MB
// MAGIC             return 1
// MAGIC         elif data_size < 1073741824:  # 1 GB
// MAGIC             return math.ceil(data_size / (100 * 1024**2))  # 1 partición por 100MB
// MAGIC         else:
// MAGIC             return min(200, math.ceil(data_size / (500 * 1024**2)))  # Máximo 200 particiones
// MAGIC     except Exception as e:
// MAGIC         print(f"Error calculando particiones: {str(e)}")
// MAGIC         return 1
// MAGIC
// MAGIC # 1. Calcular número de particiones de forma segura
// MAGIC numPartitions = num_partitions_calc(ruta_archivo)
// MAGIC print(f"Número de particiones calculadas: {numPartitions}")
// MAGIC
// MAGIC # 2. Agrupar datos por fecha evitando collect_list (que causa el problema)
// MAGIC '''df_grouped = (dfAux
// MAGIC     .withColumn("fecha_formateada", F.date_format(F.col(campo_particion), date_format_file))
// MAGIC     .repartition(numPartitions, "fecha_formateada"))'''
// MAGIC df_grouped = (dfAux
// MAGIC     .withColumn("fecha_formateada", F.col(campo_particion))
// MAGIC     .repartition(numPartitions, "fecha_formateada"))
// MAGIC
// MAGIC # Contar fechas distintas de forma eficiente
// MAGIC distinct_dates = df_grouped.select("fecha_formateada").distinct().count()
// MAGIC df_grouped.select("fecha_formateada").distinct().show(10,False)
// MAGIC print(f"Número de fechas distintas: {distinct_dates}")
// MAGIC
// MAGIC # 3. Configuración de rutas
// MAGIC output_base_path = f"{ruta_adls.rstrip('/')}/stage"
// MAGIC
// MAGIC # 4. Procesamiento optimizado sin recolectar todos los datos al driver
// MAGIC def process_and_write_date(date_df):
// MAGIC     count = date_df.count()
// MAGIC     print(f"process_and_write_date fecha: {count}")
// MAGIC     ##date_df.printSchema()
// MAGIC     # Validar que el DataFrame no esté vacío antes de acceder a .first()
// MAGIC     if count == 0:
// MAGIC         print("process_and_write_date: DataFrame vacío, no se puede procesar.")
// MAGIC         return None
// MAGIC
// MAGIC     # Acceder correctamente al valor dentro del objeto Row
// MAGIC     #fecha = date_df.first().campo_particion  # Alternativamente: fecha = date_df.first().asDict()["fecha_formateada"]
// MAGIC     fecha = date_df.first().asDict()["fecha_formateada"]
// MAGIC     date_df = date_df.drop("fecha_formateada")
// MAGIC     print(f"process_and_write_date  pasa a escribir dataFrame: {fecha}")
// MAGIC     try:
// MAGIC         # Escribir directamente sin collect
// MAGIC         (date_df
// MAGIC          .write
// MAGIC          .mode("overwrite")
// MAGIC          .option("header", str(encabezado).lower())
// MAGIC          .option("delimiter", delimitador)
// MAGIC          .csv(f"{output_base_path}/tmp/{fecha}"))
// MAGIC         
// MAGIC         return fecha
// MAGIC     except Exception as e:
// MAGIC         print(f"process_and_write_date: Error procesando fecha {fecha}: {str(e)}")
// MAGIC         return None
// MAGIC
// MAGIC
// MAGIC # 5. Procesar cada fecha por separado
// MAGIC #Verifica que la columna exista y que el DataFrame no esté vacío
// MAGIC '''        if "fecha_formateada" in df_grouped.columns:
// MAGIC              date_df = df_grouped.filter(F.col("fecha_formateada") == fecha).drop("fecha_formateada")
// MAGIC              process_and_write_date(date_df)
// MAGIC         else:
// MAGIC              print("Error: La columna 'fecha_formateada' no existe en el DataFrame.")
// MAGIC              # También puedes imprimir las columnas disponibles para debuggear
// MAGIC print("Columnas disponibles:", df_grouped.columns)'''
// MAGIC matrizSalida = []
// MAGIC for fecha in [row["fecha_formateada"] for row in df_grouped.select("fecha_formateada").distinct().collect()]:
// MAGIC     try:
// MAGIC         print(f"Comienza for : {fecha}")
// MAGIC         
// MAGIC         # Filtrar y escribir datos para cada fecha
// MAGIC         #date_df = df_grouped.filter(F.col("fecha_formateada") == fecha).drop("fecha_formateada")
// MAGIC         date_df = df_grouped.filter(F.col("fecha_formateada") == fecha)#.drop("fecha_formateada")
// MAGIC         print(f"Llamada funcion process_and_write_date : {fecha}, largo df: {date_df.count()}")
// MAGIC         process_and_write_date(date_df)
// MAGIC         
// MAGIC         
// MAGIC         # Mover archivo a ubicación final
// MAGIC         print(f"Mover archivo ubicacion final : {fecha}")
// MAGIC         part_files = [f for f in dbutils.fs.ls(f"{output_base_path}/tmp/{fecha}") 
// MAGIC                      if f.name.startswith("part-") and f.name.endswith(".csv")]
// MAGIC         
// MAGIC         if part_files:
// MAGIC             file_to_move = part_files[0].path
// MAGIC             #largo = len(nombre_archivo.split('_'))
// MAGIC             nombre_final = f"{nombre_archivo.replace(nombre_archivo.split('_')[-1], fecha.replace('-', ''))}.csv"
// MAGIC             destino_final = f"{output_base_path}/{nombre_final}"
// MAGIC             
// MAGIC             dbutils.fs.mv(file_to_move, destino_final)
// MAGIC             matrizSalida.append({
// MAGIC                 "nombreFile": nombre_final,
// MAGIC                 "pathFile": output_base_path
// MAGIC             })
// MAGIC             
// MAGIC             # Limpiar directorio temporal
// MAGIC             dbutils.fs.rm(f"{output_base_path}/tmp/{fecha}", recurse=True)
// MAGIC             
// MAGIC     except Exception as e:
// MAGIC         print(f"Ciclo for : Error procesando fecha {fecha}: {str(e)}")
// MAGIC
// MAGIC # Resultados finales
// MAGIC print("\nProceso completado")
// MAGIC print(f"Total archivos generados: {len(matrizSalida)}")
// MAGIC if matrizSalida:
// MAGIC     display(matrizSalida)

// COMMAND ----------

// MAGIC %python
// MAGIC # quitar archivo temporal después de renombrado
// MAGIC
// MAGIC print(f"{ruta_adls}/stage/tmp")
// MAGIC dbutils.fs.rm(f"{ruta_adls}/stage/tmp/", recurse=True)

// COMMAND ----------

// MAGIC %python
// MAGIC import json
// MAGIC
// MAGIC print(matrizSalida)
// MAGIC # Convertir el arreglo a formato JSON
// MAGIC arreglo_json = json.dumps(matrizSalida)
// MAGIC
// MAGIC # Retornar el arreglo como salida del notebook
// MAGIC dbutils.notebook.exit(arreglo_json)
