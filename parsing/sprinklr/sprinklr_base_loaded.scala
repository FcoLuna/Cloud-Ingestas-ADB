// Databricks notebook source
// Importa las bibliotecas necesarias para Spark y funciones
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DateType
import java.time.LocalDate
import java.time.format.DateTimeFormatter
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.types.{StructType, StructField, StringType}
import org.apache.spark.sql.Column
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.functions.{col, length, regexp_replace, split, when}

// Configuración de la sesión Spark
val spark = SparkSession.builder.appName("Parsing_Sprinklr_Base_Loaded").getOrCreate()

var parsing_in = dbutils.widgets.get("parsing_in")
var parsing_out = dbutils.widgets.get("parsing_out")
var filename = dbutils.widgets.get("nombre_archivo")

//val parsing_in = "abfss://ingestas@stbigdatadev02.dfs.core.windows.net/data/interacciones/campanas/sprinklr/sprinklr_base_loaded/parsing_in/"
//val parsing_out = "abfss://ingestas@stbigdatadev02.dfs.core.windows.net/data/interacciones/campanas/sprinklr/sprinklr_base_loaded/stage/"
//val filename = "Sprinklr_Base_Loaded_20241106.csv"

// Función para limpiar columnas String
def cleanStringColumn(colName: String): Column = {
  val trimmedColumn = trim(col(colName))
  when(trimmedColumn === "" || trimmedColumn === "null" || col(colName).isNull, null)
    .otherwise(trimmedColumn)
    .alias(colName)
}

try {

  val Timestamp_pattern = "^\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}$"
  val DateFormat_pattern = "^[0-9]{8}$"

  // Lectura del archivo con delimitador |
  val df1 = spark.read.option("header", "true").option("inferSchema", "false").option("delimiter", "|").csv(parsing_in + filename)
  val df2 = df1.select(df1.columns.map(c => when(trim(col(c)) === "" || col(c) === "null", null).otherwise(col(c)).alias(c)): _*).
    withColumn("NBA_FECHA_PROCESAMIENTO", date_format(to_date(col("NBA_FECHA_PROCESAMIENTO"), "ddMMMyyyy:HH:mm:ss"), "yyyy-MM-dd")).
    filter(col("call_fec_gest_ini").isNotNull).
    filter(col("call_fec_gest_ini").rlike(DateFormat_pattern)).
    filter(col("datainserted_datetime").rlike(Timestamp_pattern)).
    filter(col("DIR_NRO").isNull || length(col("DIR_NRO")) <= 10).
    filter(col("OFER_P1_PRECIO").isNull || length(col("OFER_P1_PRECIO")) <= 10).dropDuplicates()

  val df_cleaned_nuevo = df2.select(
    df1.columns.map { colName =>
      df1.schema(colName).dataType match {
        case _: org.apache.spark.sql.types.StringType => cleanStringColumn(colName)
        case _ => col(colName)
      }
    }: _*
  )

  val df_filled = df_cleaned_nuevo.na.fill("null_placeholder")

  val dfNewWithoutDuplicates_reverse = df_filled.columns.foldLeft(df_filled) { (tempDf, colName) =>
    val columnType = tempDf.schema(colName).dataType
    if (columnType.isInstanceOf[org.apache.spark.sql.types.StringType]) {
      tempDf.withColumn(colName, when(col(colName) === "null_placeholder", lit(null)).otherwise(col(colName)))
    } else {
      tempDf
    }
  }

  val folder_name = filename.stripSuffix(".csv")

  // Guarda el archivo en la carpeta de salida
  dfNewWithoutDuplicates_reverse.repartition(1).write.option("header", "true").option("delimiter", "|").mode("append").csv(parsing_out)

// Listar los archivos en el directorio y Filtrar el archivo que comienza con "part-"
val files = dbutils.fs.ls(parsing_out)
val oldFilePathOption = files.find(file => file.name.startsWith("part-"))

// Si encontramos el archivo, renombrarlo
oldFilePathOption match {
  case Some(oldFile) =>
    val oldFilePath = oldFile.path
    val newFilePath = parsing_out + filename
    // Renombrar el archivo
    dbutils.fs.mv(oldFilePath, newFilePath)
    println(s"El archivo ha sido renombrado a: $filename")

  case None =>
    println("No se encontró ningún archivo que cumpla con el criterio.")
}

} catch {
  case e: Exception =>
    println("[ERROR] " + e)
    throw e
}

println("[INFO] PROCESO TERMINADO")


// COMMAND ----------

val files = dbutils.fs.ls(parsing_in)
files.foreach(file => dbutils.fs.rm(file.path, true))
