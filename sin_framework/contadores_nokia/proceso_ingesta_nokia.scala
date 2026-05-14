// Databricks notebook source
// DBTITLE 1,Importar Librerías
import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql.SparkSession

import java.net.URI
import java.text.SimpleDateFormat
import java.util.Calendar
import scala.util.Try
import scala.concurrent.Await
import scala.concurrent.duration._
import scala.collection.mutable.ListBuffer
import scala.concurrent.{ExecutionContext, Future}
import java.util.concurrent.Executors

// COMMAND ----------

// DBTITLE 1,Importar Executors
implicit val customExecutionContext: ExecutionContext =
  ExecutionContext.fromExecutor(Executors.newFixedThreadPool(50))

// COMMAND ----------

// DBTITLE 1,Definir Variables
//dbutils.widgets.text("path_nokia","/data/trafico/senalizacion/contadores_nokia/stage_14")
//dbutils.widgets.text("path_adls","abfss://ingestas@stbigdatadev02.dfs.core.windows.net")

// COMMAND ----------

// DBTITLE 1,Recibir Variables desde ADF
val path_adls = dbutils.widgets.get("path_adls") 
val path_nokia = path_adls + dbutils.widgets.get("path_nokia") 

// COMMAND ----------

// DBTITLE 1,Configuraciones
spark.conf.set("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")
spark.conf.set("parquet.enable.summary-metadata", "false")
spark.conf.set("fs.azure.createRemoteFileSystemDuringInitialization", "true")
spark.conf.set("fs.azure.enable.concurrent.writes", "true")
spark.conf.set("fs.azure.enable.append.support", "true")

// COMMAND ----------

// DBTITLE 1,Separar archivos por host
//Lee la carpeta stage14 o 15 y ve que tenga datos
if (dbutils.fs.ls(path_nokia).nonEmpty) {

  // Listar archivos en la ruta de entrada
  val fileStatus = dbutils.fs.ls(path_nokia)
  val fileList = fileStatus.map(_.name).filter(_.contains("_"))
  val hostList = fileList.map(_.split("_")(2)).distinct

  println(s"Archivos detectados: ${fileList.size}")
  println(s"Hosts detectados: ${hostList.size}")

  // Crear tareas paralelas para cada archivo
  val writeFutures = fileList.map { file =>
    Future {
      val host = file.split("_")(2)
      val cant = host.lastIndexOf('.') - 4
      val pos = host.substring(cant - 1, cant).toString
      val destinationPath = s"""${path_nokia}_$pos/$file"""

      // Leer archivo
      val df = spark.read.format("xml").
      option("rowTag", "measInfo").
      load(s"$path_nokia/$file")

      // Escribir archivo directamente en su ruta destino
      df.coalesce(1).
      write.
      mode("overwrite").
      format("xml").
      option("rowTag", "measInfo").
      save(destinationPath)

      println(s"Archivo movido: $file -> $destinationPath")
    }
  }

  // Esperar que todas las tareas paralelas terminen
  Await.result(Future.sequence(writeFutures), Duration.Inf)
}


// COMMAND ----------

//Elimina el folder stage_14 o 15
dbutils.fs.rm(path_nokia,true)
