// Databricks notebook source
spark.conf.set("spark.databricks.delta.optimizeWrite.enabled", "true")
spark.conf.set("spark.databricks.delta.autoCompact.enabled", "true")

// COMMAND ----------

import org.apache.hadoop.conf.Configuration

import java.net.URI
import java.text.SimpleDateFormat
import java.util.Calendar
import scala.collection.mutable.{ArrayBuffer, ListBuffer}
import scala.util.{Try, Success, Failure}
import org.apache.spark.sql.{DataFrame}

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

// COMMAND ----------

//dbutils.widgets.text("path_stage","/data/trafico/senalizacion/contadores_nokia/stage/")
//dbutils.widgets.text("path_adls","abfss://ingestas@stbigdatadev02.dfs.core.windows.net")
//dbutils.widgets.text("path_destino","/data/trafico/senalizacion/contadores_nokia")
//dbutils.widgets.text("catalogo","bi_ingestas")
//dbutils.widgets.text("esquema","raw_trafico")

// COMMAND ----------

val path_adls = dbutils.widgets.get("path_adls")
val path_stage = path_adls + dbutils.widgets.get("path_stage")
val path_destino = path_adls + dbutils.widgets.get("path_destino")
val catalogo = dbutils.widgets.get("catalogo")
val esquema = dbutils.widgets.get("esquema")
val bad_records_path = path_adls + path_destino +"/stage_error"

// COMMAND ----------

if (dbutils.fs.ls(path_stage).nonEmpty) {

  //Se define esquema del xml
  val schema = StructType(Array(
    StructField("_measInfoId", StringType, true),
    StructField("granPeriod", StructType(Array(
      StructField("_duration", StringType, true),
      StructField("_endTime", StringType, true)
    )), true),
    StructField("measTypes", StringType, true),
    StructField("measValue", StructType(Array(
      StructField("_measObjLdn", StringType, true),
      StructField("measResults", StringType, true)
    )), true),
    StructField("repPeriod", StructType(Array(
      StructField("_duration", StringType, true)
    )), true)
  ))

  // Lectura y transformación de los datos
  val df_measInfoG = spark.read.format("xml").option("rowTag", "measInfo").schema(schema).load(path_stage)
  .select(
    col("_measInfoId"),
    col("granPeriod._duration").as("duracion"),
    col("granPeriod._endTime").cast("string").as("endTime"),
    col("measTypes"),
    col("measValue._measObjLdn").as("measObjLdn"),
    col("measValue.measResults").as("measResults"),
    col("repPeriod._duration").as("granPeriodduracion")
  )
  .withColumn("contadores", split(col("measTypes"), " "))
  .withColumn("valores", split(col("measResults"), " "))
  .withColumn("bigdata_close_date", current_date())
  .withColumn("bigdata_ctrl_id", expr("unix_timestamp()").cast("long"))
  .withColumn("year", substring(col("endTime"), 1, 4))
  .withColumn("month", substring(col("endTime"), 6, 2))
  .withColumn("day", substring(col("endTime"), 9, 2))
  .withColumn("hour", substring(col("endTime"), 12, 2))


    //Definición de ruta y tabla delta
    val ruta_final = s"$path_destino/familias/raw"
    val tabla_salida = s"$catalogo.$esquema.nokia_nt_familias"

    println(s"Escribiendo en path: $ruta_final y en la tabla: $tabla_salida")

    //Escritura del DF
    if (!spark.catalog.tableExists(tabla_salida)) {
      df_measInfoG
      .repartition(10)
      .write
      .mode("overwrite")
      .format("delta")
      .partitionBy("year", "month", "day", "hour")
      .option("path", ruta_final)
      .saveAsTable(tabla_salida)
    } else {
      df_measInfoG
      .repartition(10)
      .write
      .mode("append")
      .format("delta")
      .option("path", ruta_final)
      .saveAsTable(tabla_salida)
    }

}

// COMMAND ----------

path_stage

// COMMAND ----------

// DBTITLE 1,Elimina ruta stage
dbutils.fs.rm(path_stage,true)
