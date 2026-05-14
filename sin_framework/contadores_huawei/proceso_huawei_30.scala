// Databricks notebook source
spark.conf.set("spark.databricks.delta.optimizeWrite.enabled", "true")
spark.conf.set("spark.databricks.delta.autoCompact.enabled", "true")
spark.conf.set("spark.sql.files.maxPartitionBytes", "128m")
spark.conf.set("spark.sql.shuffle.partitions", "200")

spark.conf.set("spark.databricks.delta.concurrentWrites", "4")

// COMMAND ----------

import org.apache.hadoop.conf.Configuration
import java.net.URI
import java.text.SimpleDateFormat
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import java.util.Calendar
import scala.collection.mutable.{ArrayBuffer, ListBuffer}
import org.apache.hadoop.fs.{FileSystem, Path}
import scala.util.Try

// COMMAND ----------

import org.apache.spark.sql.SparkSession

val spark = SparkSession.builder().getOrCreate()

// COMMAND ----------

dbutils.widgets.text("path_huawei","abfss://smartcare@stbigdatadev02.dfs.core.windows.net/data/trafico/senalizacion/contadores_huawei/landing/") 
dbutils.widgets.text("path_destino","abfss://smartcare@stbigdatadev02.dfs.core.windows.net/data/trafico/senalizacion/contadores_huawei/")
dbutils.widgets.text("catalogo", "bi_ingestas")
dbutils.widgets.text("esquema", "raw_trafico")

// COMMAND ----------

val now = LocalDateTime.now()

val formatter = DateTimeFormatter.ofPattern("yyyyMMddHH")
val startTime = now.minusHours(3).format(formatter) 

val firstPeriod = startTime +"00"
val secondPeriod = startTime+"30"

// COMMAND ----------

val path_huawei = dbutils.widgets.get("path_huawei") + startTime
val path_destino = dbutils.widgets.get("path_destino")
val catalogo = dbutils.widgets.get("catalogo")
val esquema = dbutils.widgets.get("esquema")

// COMMAND ----------


val path_huawei = dbutils.widgets.get("path_huawei") + startTime
if (dbutils.fs.ls(path_huawei).nonEmpty) {
  
  // Listar archivos en la ruta de entrada
  val fileStatus = dbutils.fs.ls(path_huawei)
  val fileList = fileStatus.map(_.name).filter(_.contains("_")) 
  var hostList = fileList.map(_.split("_")).map(_(2)).distinct
  
  println(s"Archivos detectados: ${fileList.size}")
  println(s"Hosts detectados: ${hostList.size}")
}

// COMMAND ----------

val files = dbutils.fs.ls(path_huawei)
  .filter(file => file.name.endsWith(".csv")) 
  .filter(file => file.name.contains("_30_"))
  .map(file => file.path)

// COMMAND ----------

files.size

// COMMAND ----------

val filesByFamily = files.groupBy(_.split("/").last.split("_")(2))
println(filesByFamily)

// COMMAND ----------

var contador = 1
filesByFamily.foreach { case (familia, familyFiles) =>
  var tecnologia = familia.substring(0, 2) match {
    case "19"  => "5G"
    case "15"  => "4G"
    case "82" | "50" | "67" => "3G"
    case "12" => "2G"
    case "17" | "16" | "10"  => "NT"
    case _ => "Unknown" 
  }
  println("tecnologia: "+tecnologia)

  val ruta_final = path_destino  + familia + "/raw"
  val tabla_salida = catalogo + "." + esquema + ".huawei_" + tecnologia + "_" + familia

  val validFiles = familyFiles.filter { filePath =>
  Try(spark.read.option("header", true).option("inferSchema", true).csv(filePath)).isSuccess}

  println("contador: "+contador+ "Escribiendo en ruta: "+ruta_final+" y guardando en tabla: "+tabla_salida)
  // Leer todos los archivos de la familia y unirlos en un solo DataFrame
  val dfList = validFiles.map(filePath => spark.read.option("header", true).option("inferSchema", true).csv(filePath).
    withColumnRenamed("Result Time", "Result_Time").
    withColumnRenamed("Granularity Period", "Granularity_Period").
    withColumnRenamed("Object Name", "Object_Name").
    withColumn("bigdata_close_date", current_date()).
    withColumn("bigdata_ctrl_id", expr("unix_timestamp()").cast("long")).
    withColumn("year", substring(col("Result_Time"), 1, 4)).
    withColumn("month", substring(col("Result_Time"), 6, 2)).
    withColumn("day", substring(col("Result_Time"), 9, 2)).
    withColumn("hour", substring(col("Result_Time"), 12, 2))
  )
  val combinedDf = dfList.reduce(_.unionByName(_, allowMissingColumns = true))


val invalidFiles = familyFiles.filter { filePath =>
  Try(spark.read.option("header", true).option("inferSchema", true).csv(filePath)).isFailure
}

contador += 1
println(s"Archivos ignorados: ${invalidFiles.mkString(", ")}")
  // Escribir el DataFrame combinado en la tabla Delta
  if (!spark.catalog.tableExists(tabla_salida)) {
    combinedDf.repartition(5)
      .write
      .mode("overwrite")
      .format("delta")
      .partitionBy("year", "month", "day")
      .option("path", ruta_final)
      .saveAsTable(tabla_salida)
  } else {
    combinedDf.repartition(5)
      .write
      .mode("append")
      .format("delta")
      .option("path", ruta_final)
      .saveAsTable(tabla_salida)
  }
}

// COMMAND ----------

path_huawei

// COMMAND ----------

//descomentar cuando se pase a prd
//dbutils.fs.rm(path_huawei, true)
