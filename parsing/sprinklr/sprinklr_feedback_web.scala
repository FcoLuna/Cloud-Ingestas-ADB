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

// Configuración de la sesión Spark
val spark = SparkSession.builder.appName("Parsing Sprinklr Feedback Web").getOrCreate()
//spark.sparkContext.setLogLevel("INFO")

var parsing_in = dbutils.widgets.get("parsing_in")
var parsing_out = dbutils.widgets.get("parsing_out")
var filename = dbutils.widgets.get("nombre_archivo")

// Define las rutas de entrada y salida en Azure Data Lake
//val parsing_in = "abfss://ingestas@stbigdatadev02.dfs.core.windows.net/data/interacciones/canales/sprinklr/sprinklr_feedback_web/parsing_in/"
//val parsing_out = "abfss://ingestas@stbigdatadev02.dfs.core.windows.net/data/interacciones/canales/sprinklr/sprinklr_feedback_web/parsing_out/"

// Nombre del archivo de entrada
//val filename = "AD_16H_20250808.csv"

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

  //val datePattern = "^\\d{4}-\\d{2}-\\d{2}$"
  val datePattern = "^\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}$"
  val filterNumbers = "^-?\\d+(\\.\\d+)?$"

  //Columnas a Formatear
  val datetimeCols = Seq("Datetime_carga", "WsTs", "ResTsProcessed")

  // Lectura del archivo con delimitador |
  val df1 = spark.read.option("header", "true").option("inferSchema", "false").option("delimiter", "|").csv(parsing_in+filename)
  
  
  //Formateamos las columnas timestamp
  val formattedDf = datetimeCols.foldLeft(df1) { (df1, colName) =>
      df1.withColumn( 
      colName,
      date_format(
      to_timestamp(col(colName), "dd-MM-yyyy HH:mm:ss.SSS"),"yyyy-MM-dd HH:mm:ss"
    )
  )
 }

  //Se validan los nulos y el patron del formato fecha de la columna de particion 
  val df2 = formattedDf.select(formattedDf.columns.map(c => when(trim(col(c)) === "" || col(c) === "null", null).otherwise(col(c)).alias(c)): _*)
     //.withColumn("Datetime_carga", date_format(to_date(col("Datetime_carga"), "yyyy-MM-dd HH:mm:ss"), "yyyy-MM-dd"))
    .filter(col("FECHA_INICIO_LLAMADA").isNotNull)
    .filter(col("FECHA_INICIO_LLAMADA").rlike(datePattern))
    .filter(col("FECHA_INICIO_LLAMADA").cast(DateType).isNotNull)
    .dropDuplicates()
    //.filter(col("ACC_SCORE").isNull || col("ACC_SCORE").rlike(filterNumbers))
    
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

// Elimina cada archivo individualmente
files.foreach(file => dbutils.fs.rm(file.path, true))

