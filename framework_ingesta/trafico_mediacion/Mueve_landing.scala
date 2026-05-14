// Databricks notebook source
spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")

// COMMAND ----------

import org.apache.spark.sql.functions._
import org.apache.hadoop.fs.{FileSystem, Path}
import java.net.URI
import java.nio.file.Paths
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession
import com.databricks.dbutils_v1.DBUtilsHolder.dbutils
import org.apache.spark.sql.types.{DataType, StructType}
import org.apache.spark.sql.functions.lit


// COMMAND ----------

// MAGIC %md
// MAGIC ### **VARIABLES**

// COMMAND ----------

var pathSchema = ""
var pathStage = ""
var pathLanding = ""
pathSchema = dbutils.widgets.get("path_schema")
pathStage = dbutils.widgets.get("stage")
pathLanding = dbutils.widgets.get("landing")
val spark = SparkSession.builder.appName("AddFilenameColumnToADLSFiles").getOrCreate()
var list : Seq[com.databricks.backend.daemon.dbutils.FileInfo] = null
// Ruta del directorio de origen y destino

// COMMAND ----------

// Definir las rutas de origen y destino

// Leer el archivo schema.json como texto y luego parsear el esquema
val schemaJson = spark.read.textFile(s"$pathSchema/schema.json").collect().mkString
val schemaData = DataType.fromJson(schemaJson).asInstanceOf[StructType]

// Listar todos los archivos en el directorio de origen
val files = dbutils.fs.ls(pathStage)

files.foreach { file =>
  val nom = file.name

  // Leer cada archivo como un DataFrame
  val df = spark.read.option("header", "false").option("delimiter", ",").schema(schemaData).csv(file.path) 
  
  // Agregar una columna con el nombre del archivo
  val dfWithFileName = df.withColumn("filename", lit(nom)) 

  // Definir la ruta de destino para guardar el archivo con la nueva columna
  val destinationFilePath = s"$pathStage/tmp" 

  // Guardar el DataFrame en la carpeta tmp
  dfWithFileName.coalesce(1).write.mode("append").option("header", "false").option("delimiter", ",").csv(destinationFilePath) 

  // Listar los archivos en la ruta especificada
  val tmpFiles = dbutils.fs.ls(destinationFilePath) 

  // Filtrar los archivos que tienen la extensión `.csv`
  val csvFile = tmpFiles.filter(tmpFile => tmpFile.name.endsWith(".csv")).head 
          
  val nomTmp = csvFile.name
  val origen = s"$destinationFilePath/$nomTmp"
  val destino = s"$pathLanding/$nom"
  
  // Mover el archivo al destino
  dbutils.fs.mv(origen, destino)
  
  // Eliminar el archivo original
  val pathDelete = s"$pathStage/$nom"
  dbutils.fs.rm(pathDelete, true)
}

// Eliminar la carpeta tmp
dbutils.fs.rm(s"$pathStage/tmp", true)

