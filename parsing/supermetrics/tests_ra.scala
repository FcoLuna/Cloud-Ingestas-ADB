// Databricks notebook source
//BORRAR ARCHIVOS DE UNA CARPETA

val parsing_in = "abfss://ingestas@stbigdatadev02.dfs.core.windows.net/data/interacciones/campanas/sprinklr/sprinklr_base_loaded/parsing_in"
val files = dbutils.fs.ls(parsing_in)
files.foreach(file => dbutils.fs.rm(file.path, true))

// COMMAND ----------

//LEER UN ARCHIVO JSON, HQL, TXT, ETC.

val txtPath = "abfss://ingestas@stbigdatadev02.dfs.core.windows.net/data/interacciones/canales/logueos_app_mi_movistar/last_ingest_time.txt"

val txtPath2 = "abfss://ingestas@stbigdatadev02.dfs.core.windows.net/data/interacciones/canales/logueos_app_mi_movistar/logueos_app_mi_movistar.hql"

val hqlContent = spark.read.textFile(txtPath2).collect().mkString("\n")
println(hqlContent)


// COMMAND ----------



// COMMAND ----------

val path1 = "abfss://ingestas@stbigdatadev02.dfs.core.windows.net/data/interacciones/canales/supermetrics/google_ads/stage/"

//val path2 = "abfss://ingestas@stbigdatadev02.dfs.core.windows.net/data/interacciones/campanas/sprinklr/sprinklr_estado_agentes/landing/2024/11/LOGUEOS_APP_MI_MOVISTAR_202411.csv"

val df1 = spark.read.option("header", "true").option("delimiter", "|").csv(path1)

val buscar = df1.filter($"call_reg_id" === "2046346471.5964967931").select("tiempo_primer_timbre_contacto","fecha_hora_ini_discado").show(5, false)
//df1.show(50, false)
//val distinctj = df1.select("fectran").distinct().show(5, false)
//val busca = df1.filter($"fectran" === "20241118").count()

//df1.count()

// COMMAND ----------

//CREA UNA CARPETA
val directoryPath_base_loaded = "abfss://ingestas@stbigdatadev02.dfs.core.windows.net/data/interacciones/canales/logueos_app_mi_movistar/parsing_in"

dbutils.fs.mkdirs(directoryPath_base_loaded)



// COMMAND ----------

//LEER ARCHIVOS DELTA

val filePath = "abfss://ingestas@stbigdatadev02.dfs.core.windows.net/data/interacciones/campanas/sprinklr/sprinklr_feedback/raw/year=2024/month=11"
val df = spark.read.format("delta").load(filePath)

val buscar = df.filter($"call_reg_id" === "2046346471.5964967931").select("tiempo_primer_timbre_contacto","fecha_hora_ini_discado").show(5, false)

//val buscar = df.filter($"call_reg_id" === "2046413013.611218929").show(10, false)
//df.show(15, false)
//df.count()

// COMMAND ----------

// SECRETOS CREADOS

dbutils.secrets.list("secrets-ingestas")

// COMMAND ----------

//ELIMINAR CARPETA

val folderPath22 = "abfss://ingestas@stbigdatadev02.dfs.core.windows.net/data/interacciones/canales/supermetrics/google_ads/stage/google_ads_20241129.csv"
dbutils.fs.rm(folderPath22, recurse = true)



// COMMAND ----------

//COPIAR ARCHIVO DESDE DBFS A ABFSS

// Ruta de origen en DBFS
val dbfsSourcePath = "/FileStore/tables/test_supermetrics/Recambios_int_mes.txt"

// Ruta de destino en Azure Data Lake
val datalakeDestPath = "abfss://ingestas@stbigdatadev02.dfs.core.windows.net/data/gestion_recursos/operacion_red/calicux/actividad_usuario_iptv/test/"

dbutils.fs.cp(dbfsSourcePath, datalakeDestPath)

// COMMAND ----------

/Workspace/Users/rodrigo.asenjo@telefonica.com/detail-20241120

// COMMAND ----------

// MAGIC %python
// MAGIC !python --version
// MAGIC %pip install --index-url=https://mirror.gcr.io/pypi/simple google-api-python-client google-auth google-auth-httplib2
// MAGIC

// COMMAND ----------

// MAGIC %python
// MAGIC !python --version
// MAGIC

// COMMAND ----------

// Importa las bibliotecas necesarias para Spark y funciones
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DateType
import java.time.LocalDate
import java.time.format.DateTimeFormatter
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.types.{StructType, StructField, StringType}
import org.apache.spark.sql.Column

// Configuración de la sesión Spark
val spark = SparkSession.builder.appName("Parsing Sprinklr Feedback").getOrCreate()
//spark.sparkContext.setLogLevel("INFO")

//var parsing_in = dbutils.widgets.get("parsing_in")
//var parsing_out = dbutils.widgets.get("parsing_out")
//var filename = dbutils.widgets.get("nombre_archivo")

// Define las rutas de entrada y salida en Azure Data Lake
val parsing_in = "abfss://ingestas@stbigdatadev02.dfs.core.windows.net/data/interacciones/campanas/sprinklr/sprinklr_feedback/parsing_in/"

val parsing_out = "abfss://ingestas@stbigdatadev02.dfs.core.windows.net/data/interacciones/campanas/sprinklr/sprinklr_feedback/parsing_out/"

// Nombre del archivo de entrada
val filename = "Sprinklr_Feedback_20241116.csv"

// Función para limpiar columnas String
def cleanStringColumn(colName: String): Column = {
  val trimmedColumn = trim(col(colName))
  when(trimmedColumn === "" || trimmedColumn === "null" || col(colName).isNull, null)
    .otherwise(trimmedColumn)
    .alias(colName)
}

try {
  // Formateo de la fecha
  val datePart = filename.split("_")(2).substring(0, 8)
  val formatter = DateTimeFormatter.ofPattern("yyyyMMdd")
  val currentDate = LocalDate.parse(datePart, formatter)

  val datePattern = "^\\d{4}-\\d{2}-\\d{2}$"
  val filterNumbers = "^-?\\d+(\\.\\d+)?$"

  // Lectura del archivo con delimitador |
  val df1 = spark.read.option("header", "true").option("inferSchema", "false").option("delimiter", "|").csv(parsing_in+filename)
  val df2 = df1.select(df1.columns.map(c => when(trim(col(c)) === "" || col(c) === "null", null).otherwise(col(c)).alias(c)): _*)
    .withColumn("NBA_FECHA_PROCESAMIENTO", date_format(to_date(col("NBA_FECHA_PROCESAMIENTO"), "ddMMMyyyy:HH:mm:ss"), "yyyy-MM-dd"))
    .filter(col("FECHA").isNotNull)
    .filter(col("FECHA").rlike(datePattern))
    .filter(col("FECHA").cast(DateType).isNotNull)
    .filter(col("ACC_SCORE").isNull || col("ACC_SCORE").rlike(filterNumbers))
    .dropDuplicates()

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

dfNewWithoutDuplicates_reverse.show(10, false)

  // Guarda el archivo en la carpeta de salida
  //dfNewWithoutDuplicates_reverse.repartition(1).write.option("header", "true").option("delimiter", "|").mode("append").csv(parsing_out)

// Listar los archivos en el directorio y Filtrar el archivo que comienza con "part-"
//val files = dbutils.fs.ls(parsing_out)
//val oldFilePathOption = files.find(file => file.name.startsWith("part-"))

// Si encontramos el archivo, renombrarlo
//oldFilePathOption match {
  //case Some(oldFile) =>
    //val oldFilePath = oldFile.path
    //val newFilePath = parsing_out + filename
    // Renombrar el archivo
    //dbutils.fs.mv(oldFilePath, newFilePath)
    //println(s"El archivo ha sido renombrado a: $filename")

  //case None =>
    //println("No se encontró ningún archivo que cumpla con el criterio.")
//}

} catch {
  case e: Exception =>
    println("[ERROR] " + e)
    throw e
}

println("[INFO] PROCESO TERMINADO")


// COMMAND ----------

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

//var parsing_in = dbutils.widgets.get("parsing_in")
//var parsing_out = dbutils.widgets.get("parsing_out")
//var filename = dbutils.widgets.get("nombre_archivo")

val parsing_in = "abfss://ingestas@stbigdatadev02.dfs.core.windows.net/data/interacciones/campanas/sprinklr/sprinklr_base_loaded/parsing_in/"
val parsing_out = "abfss://ingestas@stbigdatadev02.dfs.core.windows.net/data/interacciones/campanas/sprinklr/sprinklr_base_loaded/stage/"
val filename = "Sprinklr_Base_Loaded_20241116.csv"

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
    withColumn("datainserted_datetime", to_timestamp(col("datainserted_datetime"), "yyyy-MM-dd HH:mm:ss")).
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

dfNewWithoutDuplicates_reverse.show(10, false)

  // Guarda el archivo en la carpeta de salida
  //dfNewWithoutDuplicates_reverse.repartition(1).write.option("header", "true").option("delimiter", "|").mode("append").csv(parsing_out)

// Listar los archivos en el directorio y Filtrar el archivo que comienza con "part-"
//val files = dbutils.fs.ls(parsing_out)
//val oldFilePathOption = files.find(file => file.name.startsWith("part-"))

// Si encontramos el archivo, renombrarlo
//oldFilePathOption match {
  //case Some(oldFile) =>
    //val oldFilePath = oldFile.path
    //val newFilePath = parsing_out + filename
    // Renombrar el archivo
    //dbutils.fs.mv(oldFilePath, newFilePath)
    //println(s"El archivo ha sido renombrado a: $filename")

  //case None =>
    //println("No se encontró ningún archivo que cumpla con el criterio.")
//}

} catch {
  case e: Exception =>
    println("[ERROR] " + e)
    throw e
}

println("[INFO] PROCESO TERMINADO")


// COMMAND ----------

df = spark.sql("SELECT * FROM bi_ingestas.interacciones.sprinklr_feedback WHERE call_reg_id = '2046346471.5964967931'LIMIT 10")
df.show()

// COMMAND ----------

val df = spark.sql("SELECT tiempo_primer_timbre_contacto,fecha_hora_ini_discado FROM bi_ingestas.interacciones.sprinklr_feedback WHERE call_reg_id = '2046346471.5964967931' LIMIT 10")

df.show(false)


// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT tiempo_primer_timbre_contacto, fecha_hora_ini_discado
// MAGIC FROM bi_ingestas.interacciones.sprinklr_feedback
// MAGIC WHERE call_reg_id = '2046346471.5964967931'
// MAGIC LIMIT 10
