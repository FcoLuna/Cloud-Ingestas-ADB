// Databricks notebook source
spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")

// COMMAND ----------

// MAGIC %md
// MAGIC ### **librerias**

// COMMAND ----------

import org.apache.spark.sql.functions._
import org.apache.hadoop.fs.{FileSystem, Path}
import java.net.URI
import java.nio.file.Paths
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession
import com.databricks.dbutils_v1.DBUtilsHolder.dbutils
import scala.collection.JavaConverters._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.lit

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

import scala.concurrent.Await
import scala.concurrent.duration._
import scala.math.BigDecimal.RoundingMode



// COMMAND ----------

var dir_adls = ""
dir_adls = dbutils.widgets.get("dir_adls")
//val spark = SparkSession.builder.appName("AddFilenameColumnToADLSFiles").getOrCreate()
var list : Seq[com.databricks.backend.daemon.dbutils.FileInfo] = null
// Ruta del directorio de origen y destino
val sourcePath = dir_adls + "/stage"
val PathStage = dir_adls + "/cdr/stage"
val PathLanding = dir_adls + "/cdr/landing"

// COMMAND ----------

//val destinationPath = "abfss://ingestas@stbigdatadev02.dfs.core.windows.net/data/trafico/trafico_mediado_movil/mediacion/cdr/stage"

// Listar archivos en el directorio de origen
val files = dbutils.fs.ls(sourcePath)

// Lista de palabras clave a buscar
val keywords = List("ges", "mm1", "mm3", "pe4", "sm2", "sm3")

// Filtrar y mover archivos en paralelo
val futures = files.filter(file => {
  val fileName = file.name.toLowerCase
  keywords.exists(keyword => fileName.contains(keyword))
}).map(file => Future {
  val destinationFilePath = s"${PathStage}/${file.name}"
  dbutils.fs.mv(file.path, destinationFilePath)
  println(s"Moved: ${file.path} -> $destinationFilePath")
})

// Esperar a que todas las operaciones se completen

Await.result(Future.sequence(futures), Duration.Inf)


// COMMAND ----------

// Listar los archivos en la ruta especificada
val files = dbutils.fs.ls(PathStage)

// Filtrar los archivos que terminan en ".iq" y contar la cantidad
val iqFilesCount = files.count(file => file.name.endsWith(".iq"))

// Dividir la cantidad de archivos en 4 y redondear hacia arriba
val result = BigDecimal(iqFilesCount / 15.0).setScale(0, RoundingMode.UP).toInt


// COMMAND ----------

// Import necessary libraries


// Define source and destination paths
val srcPath = PathStage
val destPath = s"$PathStage/stage1"

// List all files in the source directory
val allFiles = dbutils.fs.ls(srcPath)

// Filter the files to get only .iq files
val iqFiles = allFiles.filter(file => file.name.endsWith(".iq"))

// Select the first 300 files
val filesToMove = iqFiles.take(result)

// Move the selected files to the destination directory
filesToMove.foreach { file =>
  val srcFile = s"$srcPath/${file.name}"
  val destFile = s"$destPath/${file.name}"
  
  dbutils.fs.mv(srcFile, destFile)
}


// COMMAND ----------

// Define source and destination paths
val srcPath = PathStage
val destPath = s"$PathStage/stage2"

// List all files in the source directory
val allFiles = dbutils.fs.ls(srcPath)

// Filter the files to get only .iq files
val iqFiles = allFiles.filter(file => file.name.endsWith(".iq"))

// Select the first 300 files
val filesToMove = iqFiles.take(result)

// Move the selected files to the destination directory
filesToMove.foreach { file =>
  val srcFile = s"$srcPath/${file.name}"
  val destFile = s"$destPath/${file.name}"
  
  dbutils.fs.mv(srcFile, destFile)
}


// COMMAND ----------

// Import necessary libraries


// Define source and destination paths
val srcPath = PathStage
val destPath = s"$PathStage/stage3"

// List all files in the source directory
val allFiles = dbutils.fs.ls(srcPath)

// Filter the files to get only .iq files
val iqFiles = allFiles.filter(file => file.name.endsWith(".iq"))

// Select the first 300 files
val filesToMove = iqFiles.take(result)

// Move the selected files to the destination directory
filesToMove.foreach { file =>
  val srcFile = s"$srcPath/${file.name}"
  val destFile = s"$destPath/${file.name}"
  
  dbutils.fs.mv(srcFile, destFile)
}


// COMMAND ----------

// Import necessary libraries


// Define source and destination paths
val srcPath = PathStage
val destPath = s"$PathStage/stage4"

// List all files in the source directory
val allFiles = dbutils.fs.ls(srcPath)

// Filter the files to get only .iq files
val iqFiles = allFiles.filter(file => file.name.endsWith(".iq"))

// Select the first 300 files
val filesToMove = iqFiles.take(result)

// Move the selected files to the destination directory
filesToMove.foreach { file =>
  val srcFile = s"$srcPath/${file.name}"
  val destFile = s"$destPath/${file.name}"
  
  dbutils.fs.mv(srcFile, destFile)
}


// COMMAND ----------

// Import necessary libraries


// Define source and destination paths
val srcPath = PathStage
val destPath = s"$PathStage/stage5"

// List all files in the source directory
val allFiles = dbutils.fs.ls(srcPath)

// Filter the files to get only .iq files
val iqFiles = allFiles.filter(file => file.name.endsWith(".iq"))

// Select the first 300 files
val filesToMove = iqFiles.take(result)

// Move the selected files to the destination directory
filesToMove.foreach { file =>
  val srcFile = s"$srcPath/${file.name}"
  val destFile = s"$destPath/${file.name}"
  
  dbutils.fs.mv(srcFile, destFile)
}


// COMMAND ----------

// Import necessary libraries


// Define source and destination paths
val srcPath = PathStage
val destPath = s"$PathStage/stage6"

// List all files in the source directory
val allFiles = dbutils.fs.ls(srcPath)

// Filter the files to get only .iq files
val iqFiles = allFiles.filter(file => file.name.endsWith(".iq"))

// Select the first 300 files
val filesToMove = iqFiles.take(result)

// Move the selected files to the destination directory
filesToMove.foreach { file =>
  val srcFile = s"$srcPath/${file.name}"
  val destFile = s"$destPath/${file.name}"
  
  dbutils.fs.mv(srcFile, destFile)
}


// COMMAND ----------

// Import necessary libraries


// Define source and destination paths
val srcPath = PathStage
val destPath = s"$PathStage/stage7"

// List all files in the source directory
val allFiles = dbutils.fs.ls(srcPath)

// Filter the files to get only .iq files
val iqFiles = allFiles.filter(file => file.name.endsWith(".iq"))

// Select the first 300 files
val filesToMove = iqFiles.take(result)

// Move the selected files to the destination directory
filesToMove.foreach { file =>
  val srcFile = s"$srcPath/${file.name}"
  val destFile = s"$destPath/${file.name}"
  
  dbutils.fs.mv(srcFile, destFile)
}


// COMMAND ----------

// Import necessary libraries


// Define source and destination paths
val srcPath = PathStage
val destPath = s"$PathStage/stage8"

// List all files in the source directory
val allFiles = dbutils.fs.ls(srcPath)

// Filter the files to get only .iq files
val iqFiles = allFiles.filter(file => file.name.endsWith(".iq"))

// Select the first 300 files
val filesToMove = iqFiles.take(result)

// Move the selected files to the destination directory
filesToMove.foreach { file =>
  val srcFile = s"$srcPath/${file.name}"
  val destFile = s"$destPath/${file.name}"
  
  dbutils.fs.mv(srcFile, destFile)
}


// COMMAND ----------

// Import necessary libraries


// Define source and destination paths
val srcPath = PathStage
val destPath = s"$PathStage/stage9"

// List all files in the source directory
val allFiles = dbutils.fs.ls(srcPath)

// Filter the files to get only .iq files
val iqFiles = allFiles.filter(file => file.name.endsWith(".iq"))

// Select the first 300 files
val filesToMove = iqFiles.take(result)

// Move the selected files to the destination directory
filesToMove.foreach { file =>
  val srcFile = s"$srcPath/${file.name}"
  val destFile = s"$destPath/${file.name}"
  
  dbutils.fs.mv(srcFile, destFile)
}


// COMMAND ----------

// Import necessary libraries


// Define source and destination paths
val srcPath = PathStage
val destPath = s"$PathStage/stage10"

// List all files in the source directory
val allFiles = dbutils.fs.ls(srcPath)

// Filter the files to get only .iq files
val iqFiles = allFiles.filter(file => file.name.endsWith(".iq"))

// Select the first 300 files
val filesToMove = iqFiles.take(result)

// Move the selected files to the destination directory
filesToMove.foreach { file =>
  val srcFile = s"$srcPath/${file.name}"
  val destFile = s"$destPath/${file.name}"
  
  dbutils.fs.mv(srcFile, destFile)
}


// COMMAND ----------

// Import necessary libraries


// Define source and destination paths
val srcPath = PathStage
val destPath = s"$PathStage/stage11"

// List all files in the source directory
val allFiles = dbutils.fs.ls(srcPath)

// Filter the files to get only .iq files
val iqFiles = allFiles.filter(file => file.name.endsWith(".iq"))

// Select the first 300 files
val filesToMove = iqFiles.take(result)

// Move the selected files to the destination directory
filesToMove.foreach { file =>
  val srcFile = s"$srcPath/${file.name}"
  val destFile = s"$destPath/${file.name}"
  
  dbutils.fs.mv(srcFile, destFile)
}


// COMMAND ----------

// Import necessary libraries


// Define source and destination paths
val srcPath = PathStage
val destPath = s"$PathStage/stage12"

// List all files in the source directory
val allFiles = dbutils.fs.ls(srcPath)

// Filter the files to get only .iq files
val iqFiles = allFiles.filter(file => file.name.endsWith(".iq"))

// Select the first 300 files
val filesToMove = iqFiles.take(result)

// Move the selected files to the destination directory
filesToMove.foreach { file =>
  val srcFile = s"$srcPath/${file.name}"
  val destFile = s"$destPath/${file.name}"
  
  dbutils.fs.mv(srcFile, destFile)
}


// COMMAND ----------

// Import necessary libraries


// Define source and destination paths
val srcPath = PathStage
val destPath = s"$PathStage/stage13"

// List all files in the source directory
val allFiles = dbutils.fs.ls(srcPath)

// Filter the files to get only .iq files
val iqFiles = allFiles.filter(file => file.name.endsWith(".iq"))

// Select the first 300 files
val filesToMove = iqFiles.take(result)

// Move the selected files to the destination directory
filesToMove.foreach { file =>
  val srcFile = s"$srcPath/${file.name}"
  val destFile = s"$destPath/${file.name}"
  
  dbutils.fs.mv(srcFile, destFile)
}


// COMMAND ----------

// Import necessary libraries


// Define source and destination paths
val srcPath = PathStage
val destPath = s"$PathStage/stage14"

// List all files in the source directory
val allFiles = dbutils.fs.ls(srcPath)

// Filter the files to get only .iq files
val iqFiles = allFiles.filter(file => file.name.endsWith(".iq"))

// Select the first 300 files
val filesToMove = iqFiles.take(result)

// Move the selected files to the destination directory
filesToMove.foreach { file =>
  val srcFile = s"$srcPath/${file.name}"
  val destFile = s"$destPath/${file.name}"
  
  dbutils.fs.mv(srcFile, destFile)
}


// COMMAND ----------

// Import necessary libraries


// Define source and destination paths
val srcPath = PathStage
val destPath = s"$PathStage/stage15"

// List all files in the source directory
val allFiles = dbutils.fs.ls(srcPath)

// Filter the files to get only .iq files
val iqFiles = allFiles.filter(file => file.name.endsWith(".iq"))

// Select the first 300 files
val filesToMove = iqFiles.take(result)

// Move the selected files to the destination directory
filesToMove.foreach { file =>
  val srcFile = s"$srcPath/${file.name}"
  val destFile = s"$destPath/${file.name}"
  
  dbutils.fs.mv(srcFile, destFile)
}

