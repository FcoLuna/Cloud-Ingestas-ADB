// Databricks notebook source
// MAGIC %python
// MAGIC import json
// MAGIC from google.oauth2.service_account import Credentials
// MAGIC from googleapiclient.discovery import build
// MAGIC import pandas as pd
// MAGIC from datetime import datetime, timedelta
// MAGIC
// MAGIC # Obtener el secreto desde Databricks
// MAGIC secret_value = dbutils.secrets.get(scope="secrets-ingestas", key="sec-json-lectura-sheets-392714")
// MAGIC
// MAGIC # Cargar el JSON del secreto
// MAGIC credentials_info = json.loads(secret_value)
// MAGIC
// MAGIC # Crear las credenciales desde el contenido del JSON
// MAGIC credentials = Credentials.from_service_account_info(
// MAGIC     credentials_info,
// MAGIC     scopes=["https://www.googleapis.com/auth/spreadsheets.readonly"]
// MAGIC )
// MAGIC
// MAGIC # Conectarse a la API de Google Sheets
// MAGIC service = build("sheets", "v4", credentials=credentials)
// MAGIC sheet = service.spreadsheets()
// MAGIC
// MAGIC # Leer datos de Google Sheets
// MAGIC spreadsheet_id = "1KQWulsUWCvD3sjIcb3hwzHz2NgGQQSQ7pV6RJnPuslo"  # Reemplaza con tu ID
// MAGIC range_name = "Facebook!A1:L"  # Cambia el rango según sea necesario
// MAGIC
// MAGIC result = sheet.values().get(spreadsheetId=spreadsheet_id, range=range_name).execute()
// MAGIC values = result.get("values", [])
// MAGIC
// MAGIC # Convertir a DF
// MAGIC if not values:
// MAGIC     print("No data found.")
// MAGIC else:
// MAGIC     df = pd.DataFrame(values[1:], columns=values[0])  # Usa la primera fila como encabezado
// MAGIC     
// MAGIC     # Asegúrate de que la columna Date esté en formato datetime
// MAGIC     df['Date'] = pd.to_datetime(df['Date'], format='%Y-%m-%d', errors='coerce').dt.tz_localize('America/Santiago', nonexistent='shift_forward')
// MAGIC
// MAGIC #Obtener la fecha de ejecución en formato YYYYMMDD
// MAGIC execution_date = datetime.now().strftime("%Y%m%d")
// MAGIC spark_df = spark.createDataFrame(df)
// MAGIC
// MAGIC # Crear el widget
// MAGIC dbutils.widgets.text("output_save", "", "Output Save")
// MAGIC output_save = dbutils.widgets.get("output_save")
// MAGIC
// MAGIC # Guardar en Azure Data Lake
// MAGIC spark_df.repartition(1).write \
// MAGIC     .option("header", "true") \
// MAGIC     .option("delimiter", ";") \
// MAGIC     .mode("append").csv(output_save)
// MAGIC

// COMMAND ----------

//Actualizar el nombre del archivo

import java.time.{ZonedDateTime, ZoneId}
import java.time.format.DateTimeFormatter

// Obtener la fecha actual
val formatter = DateTimeFormatter.ofPattern("yyyyMMdd")
val dateStr = ZonedDateTime.now(ZoneId.of("America/Santiago")).format(formatter)

// Crear el nombre del archivo con la fecha
val filename = s"facebook_ads_$dateStr.csv"
var output_rename = dbutils.widgets.get("output_rename")

// Listar los archivos en el directorio y filtrar el archivo que comienza con "part-"
val files = dbutils.fs.ls(output_rename)
val oldFilePathOption = files.find(file => file.name.startsWith("part-"))

// Renombrar archivo
oldFilePathOption match {
  case Some(oldFile) =>
    val oldFilePath = oldFile.path
    val newFilePath = output_rename + filename
    // Renombrar el archivo
    dbutils.fs.mv(oldFilePath, newFilePath)
    println(s"El archivo ha sido renombrado a: $filename")

  case None =>
    println("No se encontró ningún archivo que cumpla con el criterio.")
}
println(filename)
