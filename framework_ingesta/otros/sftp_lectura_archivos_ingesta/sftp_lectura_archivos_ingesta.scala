// Databricks notebook source
dbutils.widgets.text("adls_path","abfss://ingestas@stbigdatadev02.dfs.core.windows.net")
dbutils.widgets.text("catalogo","bi_ingestas")
val adls_path = dbutils.widgets.get("adls_path")
var catalogo = dbutils.widgets.get("catalogo")


// COMMAND ----------

import org.apache.spark.sql.{SparkSession, Row, DataFrame}
import org.apache.spark.sql.types.{StructType, StructField, StringType}
import com.jcraft.jsch.{ChannelSftp, JSch, Session}
import scala.collection.JavaConverters._
import java.util.Properties
import java.text.SimpleDateFormat
import java.util.Calendar
import org.apache.spark.sql.types.TimestampType
import org.apache.spark.sql.types._

import java.io.ByteArrayOutputStream
import java.nio.charset.StandardCharsets
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import java.io.ByteArrayOutputStream
import java.nio.charset.StandardCharsets

import org.json4s.jackson.Serialization
import org.json4s.jackson.Serialization.write
import org.json4s.ShortTypeHints


// COMMAND ----------

val debug = dbutils.widgets.get("debug").toInt
//sólo cuando se ejecuta desde databrick debug=3
//val debug = 5
//val debug = -1

println(s"Entro a Debug==${debug}")

val (host, port, username, secreto, secreto_scope, remoteDirectory, filter_filename, datePattern_filename, encabezado, delimitador, path_fm, table_fm, path_last_processed_date, schema_filename, particion, fecha_inicial_leer,pipelineRunId,catalog_control) = if (debug == 3 || debug == -1 || debug == 5) {
  val schema_filename = StructType(Array(
    StructField("inicio", TimestampType, true),
    StructField("fin", TimestampType, true),
    StructField("pta", StringType, true),
    StructField("nename", StringType, true),
    StructField("SLOT", IntegerType, true),
    StructField("port_pon", IntegerType, true),
    StructField("id_alarma", StringType, true),
    StructField("telefono", StringType, true),
    StructField("agencia", StringType, true),
    StructField("clientes_iptv", IntegerType, true),
    StructField("clientes_empresa", IntegerType, true),
    StructField("clientes_alto_valor", IntegerType, true),
    StructField("rut_cliente", StringType, true),
    StructField("direccion_instalacion", StringType, true),
    StructField("razon_social", StringType, true),
    StructField("bandeja_falla", StringType, true),
    StructField("codigo_ivr", IntegerType, true),
    StructField("mensaje_ivr", StringType, true),
    StructField("productos_afectados", StringType, true),
    StructField("assigned_group", StringType, true),
    StructField("assigned_group_shift_name", StringType, true),
    StructField("assigned_support_company", StringType, true),
    StructField("assigned_support_organization", StringType, true),
    StructField("assignee", StringType, true),
    StructField("categorization_tier_1", StringType, true),
    StructField("categorization_tier_2", StringType, true),
    StructField("categorization_tier_3", StringType, true),
    StructField("ci_name", StringType, true),
    StructField("closure_manufacturer", StringType, true),
    StructField("closure_product_category_tier1", StringType, true),
    StructField("closure_product_category_tier2", StringType, true),
    StructField("closure_product_category_tier3", StringType, true),
    StructField("closure_product_model_version", StringType, true),
    StructField("closure_product_name", StringType, true),
    StructField("department", StringType, true),
    StructField("first_name", StringType, true),
    StructField("last_name", StringType, true),
    StructField("manufacturer", StringType, true),
    StructField("product_categorization_tier_1", StringType, true),
    StructField("product_categorization_tier_2", StringType, true),
    StructField("product_categorization_tier_3", StringType, true),
    StructField("product_model_version", StringType, true),
    StructField("product_name", StringType, true),
    StructField("create_request", StringType, true),
    StructField("summary", StringType, true),
    StructField("notes", StringType, true),
    StructField("middle_initial", StringType, true),
    StructField("status_reason", StringType, true),
    StructField("direct_contact_middle_initial", StringType, true),
    StructField("direct_contact_first_name", StringType, true),
    StructField("direct_contact_last_name", StringType, true),
    StructField("company", StringType, true),
    StructField("impact", IntegerType, true),
    StructField("reported_source", StringType, true),
    StructField("service_type", StringType, true),
    StructField("status", StringType, true),
    StructField("urgency", StringType, true),
    StructField("work_info_type", StringType, true),
    StructField("comunication_source", StringType, true),
    StructField("view_access", StringType, true),
    StructField("username", StringType, true),
    StructField("password", StringType, true),
    StructField("required_resolution_datetime", TimestampType, true),
    StructField("priority", StringType, true),
    StructField("servicio_r", StringType, true),
    StructField("work_info_date", TimestampType, true),
    StructField("work_info_view_access", StringType, true),
    StructField("work_info_summary", StringType, true),
    StructField("workinfoattachment1data", StringType, true),
    StructField("work_info_locked", StringType, true),
    StructField("work_info_notes", StringType, true),
    StructField("work_info_source", StringType, true),
    StructField("action", StringType, true),
    StructField("cap", StringType, true),
    StructField("nro_fibra", StringType, true)
  ))

  val host = "10.186.60.40"
  val port = 22
  val username = "srv_bigd_redes"
  val secreto = "sec-sftp-srv-bigd-redes"
  val secreto_scope = "secrets-ingestas"
  val remoteDirectory = "/upload/redes"
  val filter_filename = """fallas_masivas_nuevas_reactivas.*\.csv""".r
  val datePattern_filename = """.*_(\d{2})(\d{2})(\d{4})_.*""".r
  val encabezado = "true"
  val delimitador = ";"
  val path_base = s"${adls_path}/data/gestion_recursos/operacion_red/fatem/fallas_masivas_nuevas_react_fo"
  val path_fm = path_base + "/raw"
  val path_last_processed_date = path_base + "/last_processed_date.parquet"
  val table_fm = catalogo + "." + "raw_gestion_recursos.fallas_masivas_nuevas_react_fo"
  val particion = "year,month"
  val fecha_inicial_leer = "2025-08-16 00:00:00"
  val pipelineRunId = "123"
  val catalog_control = "bidesarrollo.control.control_ingestas"
  (host, port, username, secreto, secreto_scope, remoteDirectory, filter_filename, datePattern_filename, encabezado, delimitador, path_fm, table_fm, path_last_processed_date, schema_filename, particion, fecha_inicial_leer,pipelineRunId,catalog_control)
} else {
  val schema_filename: StructType = DataType.fromJson(dbutils.widgets.get("schema_filename")).asInstanceOf[StructType]
  val host = dbutils.widgets.get("host")
  val port = dbutils.widgets.get("port").toInt
  val username = dbutils.widgets.get("username")
  val secreto = dbutils.widgets.get("secreto")
  val secreto_scope = dbutils.widgets.get("secreto_scope")
  val remoteDirectory = dbutils.widgets.get("remoteDirectory")
  val filter_filename = dbutils.widgets.get("filter_filename").r
  val datePattern_filename = dbutils.widgets.get("datePattern_filename").r
  val encabezado = dbutils.widgets.get("encabezado")
  val delimitador = dbutils.widgets.get("delimitador")
  val path_base = s"${adls_path}" + dbutils.widgets.get("path_base")
  val table_fm = catalogo + "." + dbutils.widgets.get("table_fm")
  val path_fm = path_base + "/raw"
  val path_last_processed_date = path_base + "/last_processed_date.parquet"
  val particion = dbutils.widgets.get("particion")
  val fecha_inicial_leer = dbutils.widgets.get("fecha_inicial_leer")
  val pipelineRunId = dbutils.widgets.get("pipelineRunId")
  val catalog_control = dbutils.widgets.get("catalog_control")
  (host, port, username, secreto, secreto_scope, remoteDirectory, filter_filename, datePattern_filename, encabezado, delimitador, path_fm, table_fm, path_last_processed_date, schema_filename, particion, fecha_inicial_leer,pipelineRunId,catalog_control)
}

// COMMAND ----------

//***********************************************************************
// Crea directorio path_last_processed_date sino existe, sino no hace nada
//***********************************************************************
import org.apache.spark.sql.SaveMode
// Definir esquema para DataFrame 
val schema = StructType(Array(
  StructField("last_processed_date", StringType, false)
))
// Función para crear el archivo Parquet si no existe
def createLastProcessedDateFile(path: String): Unit = {
  val files = try {
    dbutils.fs.ls(path.substring(0, path_last_processed_date.lastIndexOf("/"))).map(_.path.stripSuffix("/"))
  } catch {
    case _: Exception => Seq.empty[String]
  }
  if (!files.contains(path.stripSuffix("/"))) {
    println("creo archivo inicial")
    val initialDF = spark.createDataFrame(Seq(Row(fecha_inicial_leer)).asJava, schema)
    initialDF.write.mode(SaveMode.Overwrite).parquet(path)
  }
}

// Crear el archivo Parquet si no existe
createLastProcessedDateFile(path_last_processed_date)

// COMMAND ----------

fecha_inicial_leer.substring(0, 10)  

// COMMAND ----------

//************************************************************************
// Solo correr para borrar tabla delta y lastProcessedDateDF
// debug = 0  - Corre desde DataFactory Modo Productivo Normal sin mensajes
// debug = 1  - Corre desde DataFactory modo productivo con mensaje 
// debug = 3  - Corre solo para databricks con parametros de databricks
// debug = 4  - Corre desde DataFactory, cambia la fecha de ultimo procesamiento 
// debug = 5  - Corre desde Databricks, cambia la fecha de ultimo procesamiento 
// debug = -1  - Borrar desde Databricks la tabla, el path y la fecha de ultimo procesamiento = fecha_inicial_leer
// debug = -2  - Borrar desde Datafactory la tabla, el path y la fecha de ultimo procesamiento = fecha_inicial_leer              
//************************************************************************
if(debug == -1 || debug == -2)
{
  println("Elimina todo")
  val lastProcessedDateDF = spark.createDataFrame(Seq(Row(fecha_inicial_leer)).asJava, schema)
  lastProcessedDateDF.write.mode("overwrite").parquet(path_last_processed_date)

  spark.sql(s"drop table if exists $table_fm")
  try {
    if (dbutils.fs.ls(path_fm).nonEmpty) {
      dbutils.fs.rm(path_fm, true)
    }
  } catch {
    case _: Exception => println(s"Path $path_fm does not exist or cannot be accessed.")
  }
}

if(debug == 4 || debug == 5)
{
  println(s"Setea hora para leer desde, ultime fecha leida $fecha_inicial_leer")
  val lastProcessedDateDF=spark.createDataFrame(Seq(Row(fecha_inicial_leer)).asJava, schema)
  lastProcessedDateDF.write.mode("overwrite").parquet(path_last_processed_date)
  val fecha_formateada = fecha_inicial_leer.substring(0, 10)  
  spark.sql(s"delete from $table_fm where bigdata_close_date>='$fecha_formateada'")
}

// COMMAND ----------

//**************************************************
// LEE LA ULTIMA FECHA DE MODIFICACION 
var finalDataFrame=spark.emptyDataFrame
// LEE LA ULTIMA FECHA DE MODIFICACION 
// Definir esquema para DataFrame
val schema = StructType(Array(
  StructField("last_processed_date", StringType, false)
))

// Función para leer la última fecha procesada desde Parquet sino existe archivo asigna "2000-01-01 00:00:00"
def readLastProcessedDate(path: String): DataFrame = {
  val files = dbutils.fs.ls(path.substring(0, path.lastIndexOf("/"))).map(_.path.stripSuffix("/"))
  if (files.contains(path.stripSuffix("/"))) {
    spark.read.parquet(path)
  } else {
    spark.createDataFrame(Seq(Row(fecha_inicial_leer)).asJava, schema)
  }
}
// Leer DataFrame con última fecha procesada
var lastProcessedDateDF = readLastProcessedDate(path_last_processed_date)
lastProcessedDateDF.show()

// Obtener última fecha procesada
val lastProcessedDate = lastProcessedDateDF.select("last_processed_date").as[String].collect().head
val dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
val lastDate = dateFormat.parse(lastProcessedDate)
println(s"lastadate time ${lastDate.getTime}")

//**************************************************

//**************************************************
// LEE HASTA ULTIMA FECHA SERIA, HORA ACTUAL - 5 MINUTOS

// Obtener fecha límite (5 minutos antes de la actual) obtiene hora actual y le resta 5 minutos para que no lea archivos que recien se escriben
val calendar = Calendar.getInstance()
calendar.add(Calendar.MINUTE, -5)
val fiveMinutesAgo = calendar.getTime.getTime
//**************************************************

//**************************************************
// CONEXION SFTP Y OBTIENE ARCHIVOS QUE CUMPLEN CONDICIONES DE NOMBRE Y FECHA DE MODIFICACION
// Función para obtener sesión SFTP
def getSftpSession(host: String, port: Int, username: String, secreto: String,secreto_scope: String): Session = {
  val jsch = new JSch()
  val secreto_key = secreto
  val password = dbutils.secrets.get(scope = s"$secreto_scope", key = s"$secreto_key")
  val session = jsch.getSession(username, host, port)
  session.setPassword(password)

  val config = new Properties()
  config.put("StrictHostKeyChecking", "no")
  session.setConfig(config)
  session.connect()
  session
}

println("Conectar a SFTP")
val session = getSftpSession(host, port, username, secreto,secreto_scope)
val channel = session.openChannel("sftp").asInstanceOf[ChannelSftp]
channel.connect()
println("Obtiene archivos a insertar en tabla")
// Obtener archivos que cumplen condiciones
val newFilesWithDates = channel.ls(remoteDirectory).asScala.collect {
  case file: ChannelSftp#LsEntry if file.getAttrs.getMTime * 1000L > lastDate.getTime &&
                                     filter_filename.findFirstIn(file.getFilename).isDefined  =>
    file.getFilename -> file.getAttrs.getMTime * 1000L
}.toSeq // Convertimos a secuencia para facilitar procesamiento

println("fin lectura",newFilesWithDates.length)
//**************************************************

// COMMAND ----------

Thread.sleep(7000)

// COMMAND ----------

//**************************************************
// DE LOS ARCHIVOS SELECCIONADO ARRIBA AHORA HAGO LOS GETS DE ESOS ARCHIVOS

var fiveMinutesAgoBigInt: Long = 0L

if (newFilesWithDates.nonEmpty) {
  spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")
  import java.io.ByteArrayOutputStream
  import java.nio.charset.StandardCharsets
  import org.apache.spark.sql.functions._
  import scala.util.Try
  import java.sql.Timestamp
  import java.text.SimpleDateFormat

  spark.conf.set("spark.databricks.delta.optimizeWrite.enabled", "true")
  spark.conf.set("spark.databricks.delta.autoCompact.enabled", "true")  

  val partitionCols = particion.split(",").map(_.trim)

  val dateFormatBigInt = new SimpleDateFormat("yyyyMMddHHmmssS")
  fiveMinutesAgoBigInt = dateFormatBigInt.format(new Timestamp(fiveMinutesAgo)).toLong
 
  // UDF que usa el patrón de afuera
  val extractUDF = udf((filename: String) => {
    filename match {
      case datePattern_filename(y, m, d) if y.length == 4 => (d, m, y)
      case datePattern_filename(d, m, y) if y.length == 4 => (d, m, y)
      case _ => ("00", "00", "0000")
    }
  })

  val batchSize = 200
  // Divide archivos en lotes
  val fileBatches = newFilesWithDates.grouped(batchSize).toList

  for ((batch, index) <- fileBatches.zipWithIndex) {
    val currentTimestamp = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new java.util.Date())
    println(s"Procesando batch ${index + 1}/${fileBatches.length} con ${batch.length} archivos a las $currentTimestamp")

    val partialDFs = batch.flatMap { case (filename, _) =>
    Try {
      val outputStream = new ByteArrayOutputStream()      
      try {
        channel.get(s"$remoteDirectory/$filename", outputStream)

        val fileContent = new String(outputStream.toByteArray, StandardCharsets.UTF_8)
        val linesDS = spark.createDataset(fileContent.split("\\r?\\n").toSeq)

        val df = spark.read
          .option("header", encabezado)
          .option("delimiter", delimitador)
          .option("mode", "PERMISSIVE")
          .schema(schema_filename)
          .csv(linesDS)
          Some(df.withColumn("bigdata_archivo_entrada", lit(filename)).dropDuplicates())
        } finally {
          outputStream.close()
        }
      }.getOrElse {
        println(s"Error al procesar archivo $filename")
        None
      }
    }

    if (partialDFs.nonEmpty) {
      val batchDF = partialDFs.reduce(_ union _)

      val enrichedDF = batchDF
        .withColumn("tmp", extractUDF($"bigdata_archivo_entrada"))
        .withColumn("year", $"tmp._3")
        .withColumn("month", $"tmp._2")
        .withColumn("day", $"tmp._1")
        .withColumn("last_processed_time", lit(new Timestamp(lastDate.getTime)))
        .withColumn("bigdata_close_date", to_date(concat_ws("-", col("year"), col("month"), col("day")), "yyyy-MM-dd"))
        .withColumn("bigdata_ctrl_id", lit(fiveMinutesAgoBigInt))
        .drop("tmp")
      val currentTimestamp2 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new java.util.Date())
      println(s"Escribiendo a las  $currentTimestamp2")
      enrichedDF
        .write
        .format("delta")
        .mode("append")
        .partitionBy(partitionCols: _*)
        .option("path", path_fm)
        .saveAsTable(table_fm)
    }
  }

} else {
  println("No new files to process.")
}

// COMMAND ----------

if (newFilesWithDates.nonEmpty) {
  // particion: String with comma-separated partition columns, e.g. "year,month,day" or "year,month" or "year"

  val dfControl = spark.sql(s"""
  select
    cast(${fiveMinutesAgoBigInt} as string) as big_data_ctrl_id,
    "sftp_lectura_archivos_ingesta" as process_name,
    "file" as data_source_type,
    "sftp_fallas_masivas" as data_source_name,
    cast(null as string) as original_file_date,
    cast(null as string) as starttime_nifi,
    cast(null as string) as endtime_nifi,
    cast(null as string) as totaltime_nifi,
    cast(null as string) as starttime_spark,
    cast(null as string) as endtime_spark,
    cast(null as float) as totaltime_spark,
    cast(null as float) as totaltime_process,
    cast(null as string) as insert_data_ctrl_date,
    "normal" as process_type,
    cast(null as string) as original_file_size,
    cast(null as string) as final_file_size,
    cast(null as string) as original_row_count,
    cast(null as string) as final_row_count,
    cast(null as string) as dif_row_count,
    cast(null as string) as final_number_of_files,
    cast(null as string) as end_file_name,
    "${path_fm}" as hdfs_path,
    "${pipelineRunId}" as pipelineRunId,
    "OK" as status,
    "Proceso Exitoso" as desc_status,
    cast(null as string) as bigdata_ctrl_id
    """)
  
  dfControl.write.mode(SaveMode.Append).option("mergeSchema", "true").saveAsTable(catalog_control)
}

// COMMAND ----------

val maxNewFilesTime = (if (newFilesWithDates.nonEmpty) newFilesWithDates.map(_._2).max else lastDate.getTime) + 1000L

// COMMAND ----------

channel.disconnect()
session.disconnect()
//**************************************************
// Actualizar DataFrame con la nueva fecha leida
// 

val latestDateStr = dateFormat.format(new java.util.Date(maxNewFilesTime))
import scala.collection.JavaConverters._
lastProcessedDateDF = spark.createDataFrame(Seq(Row(latestDateStr)).toList.asJava, schema)
lastProcessedDateDF.show()

// Guardar la nueva fecha en DBFS
lastProcessedDateDF.write.mode("overwrite").parquet(path_last_processed_date)
println(s"Fecha actualizada en el archivo: ${lastProcessedDateDF.select("last_processed_date").as[String].collect().head}")


// COMMAND ----------

lastProcessedDateDF.unpersist()
spark.catalog.clearCache()
System.gc()
if (debug==0)
    dbutils.notebook.exit("OK")

// COMMAND ----------

spark.sql(s"SET table_fm = ${table_fm}")

// COMMAND ----------

// MAGIC %sql
// MAGIC --select count(distinct bigdata_archivo_entrada) from ${hivevar:table_fm}  limit 100
// MAGIC select *  from ${hivevar:table_fm}  limit 100
// MAGIC
