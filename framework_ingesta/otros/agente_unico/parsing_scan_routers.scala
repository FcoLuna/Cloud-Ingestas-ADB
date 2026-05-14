// Databricks notebook source
import org.apache.spark.sql.{DataFrame, Row, Column}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import spark.implicits._

// COMMAND ----------

dbutils.widgets.text("delimitador","|")
dbutils.widgets.text("encabezado","true")
dbutils.widgets.text("path_stage","abfss://ingestas@stbigdatadev02.dfs.core.windows.net/data/gestion_recursos/operacion_red/agente_unico/agun_scan_routers/stage")
dbutils.widgets.text("dateFormatFile","yyyyMMdd")
dbutils.widgets.text("timestampFormat","yyyy-MM-dd HH|mm|ss")
dbutils.widgets.text("filename","output_2024-12-27.txt")

// COMMAND ----------

val delimitador = dbutils.widgets.get("delimitador")
val encabezado = dbutils.widgets.get("encabezado")
val path_stage = dbutils.widgets.get("path_stage")
val dateFormatFile = dbutils.widgets.get("dateFormatFile")
val timestampFormat = dbutils.widgets.get("timestampFormat")
val filename = dbutils.widgets.get("filename")

// COMMAND ----------

var og = spark.read
  .option("header", encabezado)
  .option("delimiter", delimitador)
  .csv(path_stage)

// COMMAND ----------

display(og)

// COMMAND ----------

val format = "yyyy-MM-dd-HH-mm"
val dateFormat = "yyyy-MM-dd HH:mm:ss"
var df_scan_report = spark.read
  .option("inferSchema", "false")
  .option("header", encabezado)
  .option("delimiter", delimitador)
  .csv(path_stage)
  //.withColumn("neighbor_de", regexp_replace(col("neighbor_de"), "[^:a-zA-Z0-9_]", ""))
  //.withColumn("neighbor_de", when(regexp_extract(col("neighbor_de"), " ", 1) === "", null).otherwise(col("neighbor_de")))
  .withColumn("ssid_de", regexp_replace(col("ssid_de"), "[^-A-Za-z0-9_. ]", ""))
  .withColumn("timest_fh", to_timestamp(col("timest_fh"), format))
  .withColumn("timest_fh", date_format(col("timest_fh"), dateFormat))
  .filter(col("timest_fh").startsWith("20"))
  .filter(col("timest_fh").endsWith("00"))
  .dropDuplicates()



// Convert the string column to a timestamp column
//df_scan_report = df_scan_report.withColumn("timest_fh", to_timestamp(col("timest_fh"), format))

// COMMAND ----------

display(df_scan_report)

// COMMAND ----------

if(df_scan_report.count() > 0) {
  df_scan_report.repartition(1).write
    .mode(SaveMode.Overwrite)
    .option("header", encabezado)
    .option("delimiter", delimitador)
    .csv(path_stage)

  val lista_archivos = dbutils.fs.ls(path_stage)
  for (elemento <- lista_archivos) {
    if (elemento.path.endsWith(".csv")) {
      dbutils.fs.mv(elemento.path, path_stage + "/" + filename)
    } else {
      dbutils.fs.rm(elemento.path)
    }
  }
}
