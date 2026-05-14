// Databricks notebook source
// DBTITLE 1,Parametros de Entrada
// Importa las bibliotecas necesarias para Spark y funciones
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DateType
import java.time.LocalDate
import java.time.format.DateTimeFormatter
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.types.{StructType, StructField, StringType}
import org.apache.spark.sql.Column
import org.apache.spark.sql.types._

// Configuración de la sesión Spark
val spark = SparkSession.builder.appName("Parsing Emergencia fono y rut").getOrCreate()
//spark.sparkContext.setLogLevel("INFO")


//Define las rutas de entrada y salida en Azure Data Lake
var parsing_in = dbutils.widgets.get("parsing_in")
var parsing_out = dbutils.widgets.get("parsing_out")
var filename = dbutils.widgets.get("nombre_archivo")
var ruta_adls = dbutils.widgets.get("ruta_adls")
var campoParticion = dbutils.widgets.get("campoParticion")

/*
var patronFormatoDate = dbutils.widgets.get("patronFormatoDate")
var patronFormatoTimestampIn = dbutils.widgets.get("patronFormatoTimestampIn")
var patronFormatoTimestampOut = dbutils.widgets.get("patronFormatoTimestampOut")
var formatearColumnas = dbutils.widgets.get("columnasAFormatear")
*/

var delimitador = dbutils.widgets.get("delimitador")
var encabezado = dbutils.widgets.get("encabezado")


// Define las rutas de entrada y salida en Azure Data Lake
/*val parsing_in = "abfss://ingestas@stbigdatadev02.dfs.core.windows.net/data/interacciones/campanas/emergencia/carga_fono_emergencia/stage/parsing_in/"
val parsing_out = "abfss://ingestas@stbigdatadev02.dfs.core.windows.net/data/interacciones/campanas/emergencia/carga_fono_emergencia/stage/"
var filename = "CARGA_FONO_gaguila_4_20250828.csv"
var ruta_adls = "abfss://ingestas@stbigdatadev02.dfs.core.windows.net/data/interacciones/campanas/emergencia/carga_fono_emergencia"
*/

//var campoParticion = "load_date"
var patronFormatoDate = "^\\d{4}-\\d{2}-\\d{2}$"
var patronFormatoTimestampIn = "dd-MM-yyyy HH:mm:ss.SSS"
var patronFormatoTimestampOut = "yyyy-MM-dd HH:mm:ss"
var formatearColumnas = ""
/*
var delimitador = "|"
var encabezado = "false"
*/


// COMMAND ----------

// DBTITLE 1,Cargamos el Schema desde ruta adls
import org.apache.spark.sql.types._
import scala.util.Try
import scala.io.Source
import scala.collection.mutable.ArrayBuffer
import scala.util.parsing.json.JSON

// Función para verificar si el archivo existe
def fileExists(path: String): Boolean = {
  Try(dbutils.fs.ls(path)).isSuccess
}

//Declaracion del Schema
var schemaFinal = StructType(Nil)


// Construir la ruta del archivo JSON
//val ruta_adls = "/mnt/datalake/estructura"
val jsonSchemaPath = s"$ruta_adls/${ruta_adls.split("/").last}.json"
println(s"Ruta: $jsonSchemaPath")

// Verificar si el archivo existe
val checkFile = fileExists(jsonSchemaPath)

if (checkFile) {
  // Leer el contenido del archivo JSON como texto
  val fileContent = spark.read.text(jsonSchemaPath).collect().map(_.getString(0)).mkString

  // Parsear el JSON a estructura Scala
  val parsed = JSON.parseFull(fileContent)

  val columnasSeleccionadas = ArrayBuffer[String]()
  val schemaFields = ArrayBuffer[StructField]()

  parsed match {
    case Some(data: Map[String, Any]) =>
      val fields = data.get("fields").getOrElse(List()).asInstanceOf[List[Map[String, Any]]]
      fields.foreach { field =>
        val columnName = field.get("name").getOrElse("").toString
        columnasSeleccionadas += columnName
        schemaFields += StructField(columnName, StringType, nullable = true)
      }

      println(s"Número inicial de columnas: ${columnasSeleccionadas.size}")

      // Verificar tipo de ingesta
      val tipoIngesta = "incremental" // o "i"
      val columnsToRemove = Set("filename_spark", "ts")

      if (tipoIngesta == "i" || tipoIngesta == "incremental") {
        println("Eliminando columnas específicas")
      }

      val columnasFinales = columnasSeleccionadas.filterNot(columnsToRemove.contains)
      schemaFinal = StructType(schemaFields.filterNot(field => columnsToRemove.contains(field.name)))

      println(s"Número de columnas después de eliminar: ${columnasFinales.size}")
      columnasFinales.foreach(println)

      // Ahora puedes usar `schemaFinal` para leer archivos con ese esquema
    case _ =>
      println("Error al parsear el JSON")
  }
}

// COMMAND ----------

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
  //val datePart = filename.split("_")(2).substring(0, 8)
  val datePart = filename.stripSuffix(".csv").split("_").last
  val formatter = DateTimeFormatter.ofPattern("yyyyMMdd")
  val currentDate = LocalDate.parse(datePart, formatter)
  var datetimeCols =  Seq.empty[String]
  
  // Nombre de la columna para capturar registros corruptos
  //val corruptColumn = "_corrupt_record"
   
   //patron fecha
   val datePattern = patronFormatoDate //"^\\d{4}-\\d{2}-\\d{2}$"
  //val datePattern = "^\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}$"
  //patron numero
  val filterNumbers = "^-?\\d+(\\.\\d+)?$"

  // Lectura del archivo con delimitador |
  val df1 = spark.read.option("header", encabezado)
           .option("inferSchema", "false")
           .option("mode", "DROPMALFORMED") // elimina filas que no se ajustan al schema
           .schema(schemaFinal) // schema definido
           .option("delimiter", delimitador)
           .csv(parsing_in+filename)
           .cache() // fuerza el parseo completo

  println("q registros " + df1.count())
  println("ruta adls archivo :"+ parsing_in+filename)

  // Separar registros válidos y corruptos
  //val dfValid = df1.filter(col(corruptColumn).isNull)
  //val dfCorrupt = df1.filter(col(corruptColumn).isNotNull)

  // Ruta HDFS para guardar registros corruptos
  //val rutaErrores = s"$ruta_adls/stage_error/${filename.replace(".csv", "_errores.csv")}"

  // Guardar registros corruptos como CSV
  /*dfCorrupt.write
      .mode("overwrite")
      .option("header", "true")
      .option("delimiter", delimitador)
      .csv(rutaErrores)
*/


  // Verificar si el DataFrame tiene registros
  if (df1.count() == 0) {
    println(" El archivo no contiene registros válidos según el esquema. Será eliminado.")

  // Eliminar el archivo malformado
   dbutils.fs.rm(parsing_in + filename, recurse = true)

  // Lanzar excepción para detener la ejecución
   throw new RuntimeException(s"Archivo inválido eliminado: ${parsing_in + filename}")
  }
  //Columnas TimeStamp a Formatear
  //val datetimeCols = Seq("Datetime_carga", "WsTs", "ResTsProcessed")
  if(formatearColumnas != ""){
     datetimeCols = formatearColumnas.split(",").map(_.trim)
  }
  
  
  //Formateamos las columnas timestamp, si no existen timestamp ejecuta de igual forma 
  val formattedDf = datetimeCols.foldLeft(df1) { (df1, colName) =>
      df1.withColumn( 
      colName,
      date_format(
        to_timestamp(col(colName), patronFormatoTimestampIn),patronFormatoTimestampOut
      //to_timestamp(col(colName), "dd-MM-yyyy HH:mm:ss.SSS"),"yyyy-MM-dd HH:mm:ss"
    )
  )
 }

  //Se validan los nulos y el patron del formato fecha de la columna de particion 
  val df2 = formattedDf.select(formattedDf.columns.map(c => when(trim(col(c)) === "" || col(c) === "null", null).otherwise(col(c)).alias(c)): _*)
     //.withColumn("Datetime_carga", date_format(to_date(col("Datetime_carga"), "yyyy-MM-dd HH:mm:ss"), "yyyy-MM-dd"))
    .filter(col(campoParticion).isNotNull)
    .filter(col(campoParticion).rlike(datePattern))
    .filter(col(campoParticion).cast(DateType).isNotNull)
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

  //Reemplaza Todos los Valores nulos Rellenandolos con strgin "null_placeholder"
  val df_filled = df_cleaned_nuevo.na.fill("null_placeholder")

 //Revierte el relleno de Null para asignarle un Null Objeto no como string
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
  dfNewWithoutDuplicates_reverse.repartition(1).write.option("header", encabezado).option("delimiter", delimitador).mode("append").csv(parsing_out)

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

