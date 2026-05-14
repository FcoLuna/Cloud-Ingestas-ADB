// Databricks notebook source
// MAGIC %python
// MAGIC from google.cloud import bigquery
// MAGIC from google.oauth2 import service_account
// MAGIC import json
// MAGIC from pyspark.sql import SparkSession
// MAGIC
// MAGIC # Obtener las credenciales desde un secreto
// MAGIC secret_value = dbutils.secrets.get(scope="secrets-ingestas", key="sec-json-mimovistar-prod")
// MAGIC
// MAGIC # Cargar las credenciales
// MAGIC credentials_dict = json.loads(secret_value)
// MAGIC credentials = service_account.Credentials.from_service_account_info(credentials_dict)
// MAGIC
// MAGIC # Crear el cliente de BigQuery utilizando las credenciales
// MAGIC client = bigquery.Client(credentials=credentials, project=credentials.project_id)
// MAGIC
// MAGIC # Obtener el nombre de la tabla desde ADF
// MAGIC table_name = dbutils.widgets.get("tableName")  # El nombre de la tabla proporcionado por ADF
// MAGIC
// MAGIC # Construir la consulta (usando directamente el parámetro table_name)
// MAGIC query = f"""
// MAGIC     SELECT 
// MAGIC       DISTINCT
// MAGIC       SAFE_CAST(SPLIT(REPLACE(SAFE_CONVERT_BYTES_TO_STRING(FROM_BASE64(base.user_id)),'"',''), '*')[OFFSET(1)] AS INT64) AS NUMCEL,
// MAGIC       replace(split(SAFE_CONVERT_BYTES_TO_STRING(FROM_BASE64(base.user_id)),'*')[OFFSET(0)],'"','') AS RUTCLI,
// MAGIC       base.platform AS DES_APLI,
// MAGIC       base.event_date AS FECTRAN,
// MAGIC       cuenta.cuenta AS CUENTA,
// MAGIC       base.app_info.version AS DESCAT,
// MAGIC       'LOGIN EN BIGQUERY' AS DES_LOG,
// MAGIC       base.device.category AS COMORI
// MAGIC     FROM (
// MAGIC       SELECT SAFE_CONVERT_BYTES_TO_STRING(FROM_BASE64(user_id)) AS DECODE_USER_ID, *
// MAGIC       FROM `mimovistar-prod.analytics_182807761.{table_name}`
// MAGIC       WHERE LEFT(user_id, 2) <> 'U2'
// MAGIC     ) AS base
// MAGIC     LEFT JOIN (
// MAGIC       SELECT DECODE_USER_ID, COUNT(DECODE_USER_ID) AS CUENTA
// MAGIC       FROM (
// MAGIC         SELECT SAFE_CONVERT_BYTES_TO_STRING(FROM_BASE64(user_id)) AS DECODE_USER_ID
// MAGIC         FROM `mimovistar-prod.analytics_182807761.{table_name}`
// MAGIC         WHERE LEFT(user_id, 2) <> 'U2'
// MAGIC       ) AS subquery
// MAGIC       GROUP BY DECODE_USER_ID
// MAGIC     ) AS cuenta
// MAGIC     ON cuenta.DECODE_USER_ID = base.DECODE_USER_ID
// MAGIC """
// MAGIC
// MAGIC # Ejecutar la consulta en BigQuery
// MAGIC query_job = client.query(query)
// MAGIC
// MAGIC # Convertir el resultado en un DataFrame de Pandas
// MAGIC result_df = query_job.to_dataframe()
// MAGIC
// MAGIC # Convertir el DataFrame de Pandas a un DataFrame de PySpark
// MAGIC spark = SparkSession.builder.appName("BigQueryToAzure").getOrCreate()
// MAGIC spark_df = spark.createDataFrame(result_df)
// MAGIC
// MAGIC # Crear el widget
// MAGIC dbutils.widgets.text("output_save", "", "Output Save")
// MAGIC output_save = dbutils.widgets.get("output_save")
// MAGIC
// MAGIC # Guardar el DataFrame en ADL
// MAGIC spark_df.repartition(1).write \
// MAGIC     .option("header", "true") \
// MAGIC     .option("delimiter", ",") \
// MAGIC     .mode("append") \
// MAGIC     .csv(output_save)

// COMMAND ----------

//Actualizar el nombre del archivo

import java.text.SimpleDateFormat
import java.util.Date

// Obtener la fecha actual
val dateFormat = new SimpleDateFormat("yyyyMMdd")
val dateStr = dateFormat.format(new Date())

// Crear el nombre del archivo con la fecha
val tableName = dbutils.widgets.get("tableName")

// Agregar '.csv' al final del nombre de la tabla
val tableNameWithExtension = s"${tableName}.csv"

var output_rename = dbutils.widgets.get("output_rename")

// Listar los archivos en el directorio y filtrar el archivo que comienza con "part-"
val files = dbutils.fs.ls(output_rename)
val oldFilePathOption = files.find(file => file.name.startsWith("part-"))

// Renombrar archivo
oldFilePathOption match {
  case Some(oldFile) =>
    val oldFilePath = oldFile.path
    val newFilePath = output_rename + tableNameWithExtension
    dbutils.fs.mv(oldFilePath, newFilePath)
    println(s"El archivo ha sido renombrado a: $tableNameWithExtension")

  case None =>
    println("No se encontró ningún archivo que cumpla con el criterio.")
}
println(tableNameWithExtension)
