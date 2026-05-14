// Databricks notebook source
// MAGIC %md
// MAGIC ### Ingesta Full desde Parquet V1

// COMMAND ----------

// MAGIC %run ../funciones/funciones_genericas

// COMMAND ----------

// MAGIC %md
// MAGIC ## _Sección_ 1: Configuración de Spark y librerías

// COMMAND ----------

spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")

import org.apache.spark.sql.{SaveMode, SparkSession, DataFrame}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import java.text.SimpleDateFormat
import java.util.{Date, TimeZone}
import scala.util.{Try, Success, Failure}
import scala.jdk.CollectionConverters._

// COMMAND ----------

// MAGIC %md
// MAGIC ## Sección 2: Definir formatters y variables globales
// MAGIC

// COMMAND ----------

val format_actual = new SimpleDateFormat("yyyyMMdd HHmmss")
val format_max = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")
val format_timestamp = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'")
val format_2 = new SimpleDateFormat("yyyyMMddHHmmss")
val format_output = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
val format_date = new SimpleDateFormat("yyyy-MM-dd")
format_actual.setTimeZone(TimeZone.getTimeZone("Chile/Continental"))
format_max.setTimeZone(TimeZone.getTimeZone("Chile/Continental"))
format_timestamp.setTimeZone(TimeZone.getTimeZone("Chile/Continental"))
format_2.setTimeZone(TimeZone.getTimeZone("Chile/Continental"))
format_output.setTimeZone(TimeZone.getTimeZone("Chile/Continental"))
format_date.setTimeZone(TimeZone.getTimeZone("Chile/Continental"))

var status_ejecucion = 0
var desc_status_ejecucion = "[OK]"
var status_ejecucion_str = "OK"
var status_error: Throwable = null
// Definido con 22 columnas (incluye bigdata_ctrl_id como null)
var controlData = Seq.empty[(String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String)]
var create_table_query_hql: String = ""
var schema_data: StructType = null
var bigdata_ctrl_id: String = ""
var filename: String = ""
var process_type: String = ""
var final_file_size: Long = 0L
var final_number_of_files: String = null
var end_file_name: String = null
var insert_data_ctrl_date: String = ""
var original_row_count: Long = 0L
var final_row_count: Long = 0L
var dif_row_count: Int = 0
var process_name: String = ""
var data_source_type: String = ""
var data_source_name: String = ""
var path_data: String = ""
var starttime_spark: String = format_output.format(new Date().getTime)


// COMMAND ----------

// MAGIC %md
// MAGIC ## Sección 3: Leer parámetros desde ADF
// MAGIC

// COMMAND ----------

case class Config(
  dir_adls: String,
  formato_entrada: String,
  dataType: String,
  plataforma: String,
  categoria: String,
  loadType: String,
  periodicity: String,
  starttime_nifi: String,
  endtime_nifi: String,
  original_file_size: String,
  original_file_date: String,
  formato_salida: String,
  nomb_proc: String,
  pipelineRunId: String,
  cant_repartition: Int
)

def handleError(e: Throwable, filename: String, starttime_spark: String, totaltime_nifi: String, critical: Boolean = true): Unit = {
  if (critical) {
    status_ejecucion = 1
    desc_status_ejecucion = s"[ERROR] $e"
    status_ejecucion_str = "ERROR"
    status_error = e
  } else {
    // No añadir a controlData para advertencias no críticas
    println(s"[WARNING] $e")
    return
  }
  println(desc_status_ejecucion)
  // Ajustado a 22 columnas (bigdata_ctrl_id como null)
  controlData = controlData :+ (
    status_ejecucion_str, desc_status_ejecucion, bigdata_ctrl_id, process_name, data_source_type,
    data_source_name, config.original_file_date, config.starttime_nifi, config.endtime_nifi,
    totaltime_nifi, starttime_spark, process_type, config.original_file_size, final_file_size.toString,
    original_row_count.toString, final_row_count.toString, dif_row_count.toString,
    final_number_of_files, end_file_name, path_data, config.pipelineRunId, null
  )
}

val result = Try {
  val data_source_type = dbutils.widgets.get("data_source_type")
  val data_source_name = dbutils.widgets.get("data_source_name")
  //val data_source_type = "table_final"
  //val data_source_name = "fm_baf.AlarmDetailsHw"
  (data_source_type, data_source_name)
}

val (data_source_type, data_source_name) = result match {
  case Success((data_source_type, data_source_name)) =>
    println(s"[INFO] Parámetros de ADF cargados: data_source_type=$data_source_type, data_source_name=$data_source_name")
    (data_source_type, data_source_name)
  case Failure(e) =>
    val errorMessage = "No se pudieron cargar los parámetros 'data_source_type' o 'data_source_name' desde ADF"
    println(s"[ERROR] $errorMessage: ${e.getMessage}")
    throw new Exception(s"$errorMessage: ${e.getMessage}")
}

val config = Try {
  Config(
    dir_adls = dbutils.widgets.get("dir_adls"),
    //dir_adls = "abfss://ingestas@stbigdatadev02.dfs.core.windows.net/data/gestion_recursos/operacion_red/siiared/alarmas_olt_detalle",
    formato_entrada = dbutils.widgets.get("formato_entrada"),
    //formato_entrada = "yyyyMMdd",
    dataType = dbutils.widgets.get("dataType"),
    //dataType = "raw",
    plataforma = dbutils.widgets.get("plataforma"),
    //plataforma = "plataforma",
    categoria = dbutils.widgets.get("categoria"),
    //categoria = "categoría",
    loadType = dbutils.widgets.get("loadType"),
    //loadType = "full",
    periodicity = dbutils.widgets.get("periodicity"),
    //periodicity = "d",
    starttime_nifi = dbutils.widgets.get("starttime_nifi"),
    //starttime_nifi = "20250422 112802",
    endtime_nifi = dbutils.widgets.get("endtime_nifi"),
    //endtime_nifi = "20250422 114029",
    original_file_size = dbutils.widgets.get("original_file_size"),
    //original_file_size = "1283148395",
    original_file_date = dbutils.widgets.get("original_file_date"),
    //original_file_date = "2025-04-22T11:40:14Z",
    formato_salida = Option(dbutils.widgets.get("formato_salida")).filterNot(_.isEmpty).getOrElse("yyyy-MM-dd"),
    //formato_salida = "yyyy-MM-dd",
    nomb_proc = dbutils.widgets.get("nomb_proc"),
    //nomb_proc = "raw_gestion_recursos.alarmas_olt_detalle",
    pipelineRunId = dbutils.widgets.get("pipelineRunId"),
    //pipelineRunId = "pipeline_run_id_ejemplo",
    cant_repartition = Option(dbutils.widgets.get("cant_repartition")).filterNot(_.isEmpty).getOrElse("-1").toInt
    //cant_repartition = 0
  )
} match {
  case Success(cfg) =>
    println("[INFO] Configuración cargada exitosamente")
    cfg
  case Failure(e) =>
    val current_time = format_output.format(new Date().getTime)
    handleError(e, "", current_time, "0")
    throw e
}

process_name = s"spark_${config.nomb_proc}"
val nombre_archivo = config.dir_adls.split("/").last
val path_processing = s"${config.dir_adls}/processing/"
val path_landing = s"${config.dir_adls}/stage/"
val path_processing_error = s"${config.dir_adls}/processing_error/"
val path_log = s"${config.dir_adls}/log/"
path_data = s"${config.dir_adls}/${config.dataType}/"
val path_tmp = s"${config.dir_adls}/tmp/"

val totaltime_nifi = Try {
  val start = format_actual.parse(config.starttime_nifi).getTime
  val end = format_actual.parse(config.endtime_nifi).getTime
  ((end - start) / 1000).toString
}.getOrElse("0")


// COMMAND ----------

// MAGIC %md
// MAGIC ## Sección 4: Leer HQL y JSON, crear tabla Delta si no existe
// MAGIC

// COMMAND ----------

Try {
  val hql_path = s"${config.dir_adls}/${nombre_archivo}.hql"
  val json_path = s"${config.dir_adls}/${nombre_archivo}.json"
  
  println(s"[INFO] Leyendo HQL desde: $hql_path")
  create_table_query_hql = dbutils.fs.head(hql_path)
  if (create_table_query_hql.trim.isEmpty) {
    throw new Exception(s"Archivo HQL vacío en: $hql_path")
  }
  
  create_table_query_hql = create_table_query_hql
    .replace("LOCATIONMANAGEDCONTAINERHQL", path_data)
    .replace("NOMBREPROCUNITYCATALOGDATABRICKS", config.nomb_proc)
  
  println(s"[INFO] Leyendo JSON desde: $json_path")
  schema_data = DataType.fromJson(dbutils.fs.head(json_path)).asInstanceOf[StructType]
  
  println(s"[INFO] Ejecutando HQL para crear tabla (si no existe): ${config.nomb_proc}")
  spark.sql(create_table_query_hql)
  println(s"[INFO] Tabla ${config.nomb_proc} creada o verificada")
}.recover {
  case e: Exception =>
    // No añadir a controlData para advertencias
    println(s"[WARNING] Error al leer HQL/JSON o crear tabla (puede que ya exista): $e")
}

println(s"[INFO] create_table_query_hql: $create_table_query_hql")
println(s"[INFO] schema_data: $schema_data")

// COMMAND ----------

// MAGIC %md
// MAGIC ## Sección 5: Procesar archivos Parquet
// MAGIC

// COMMAND ----------

Try {
  println("[INFO] Iniciando procesamiento de archivos Parquet")
  delete(path_log)
  
  val processing_files = listFiles(path_processing)
  if (processing_files.nonEmpty) {
    processing_files.foreach { file =>
      val fname = file.name
      delete(s"$path_processing_error$fname")
      moverArchivoAbfs(s"$path_processing$fname", s"$path_processing_error$fname")
    }
  }
  
  val stage_files = listFiles(path_landing).sortBy(_.name)
  println(s"[INFO] Archivos en stage: ${stage_files.map(_.name).mkString(", ")}")
  if (stage_files.nonEmpty) {
    val fields = create_table_query_hql
      .substring(create_table_query_hql.indexOf("(") + 2, create_table_query_hql.indexOf(")\n"))
      .split("\n")
      .filter(_.trim.nonEmpty)
      .map(_.trim.split(" ")(0))
    val select_line = fields.map(f => s"$f,").mkString("select ", "", "").dropRight(1) + " from ing_schema"
    
    var counter = 1
    stage_files.filterNot(_.name.startsWith(".")).foreach { file =>
      filename = file.name
      println(s"[INFO] Procesando archivo: $filename")
      delete(s"$path_processing$filename")
      moverArchivoAbfs(s"$path_landing$filename", s"$path_processing$filename")
      
      // Leer desde processing
      var df = spark.read.schema(schema_data).parquet(s"$path_processing$filename")
      
      original_row_count = df.count
      
      df = df.dropDuplicates().na.drop("all")
      
      bigdata_ctrl_id = format_2.format(new Date().getTime) + f"$counter%03d"
      counter += 1
      
      val partition_value = obtenerParticiondesdeArchivo(filename, config.formato_entrada, config.formato_salida)
      df = df.withColumn("bigdata_close_date", to_date(lit(partition_value), config.formato_salida))
             .withColumn("bigdata_ctrl_id", lit(bigdata_ctrl_id).cast("long"))
      
      df.createOrReplaceTempView("ing_schema")
      
      val output_df = spark.sql(select_line)
      
      // Fijar process_type como normal (sin particiones, siempre sobrescribe)
      process_type = "normal"
      
      val numPartitions = if (config.cant_repartition > 0) {
        config.cant_repartition
      } else if (config.cant_repartition == -1) {
        output_df.write.mode(SaveMode.Overwrite).parquet(path_tmp)
        val np = numPartitionsCalc(path_tmp)
        delete(path_tmp)
        np
      } else {
        spark.conf.get("spark.sql.shuffle.partitions").toInt
      }
      
      output_df.repartition(numPartitions).write.format("delta").mode(SaveMode.Overwrite).save(path_data)
      
      final_file_size = sizeFile(path_data)
      final_row_count = spark.read.format("delta").load(path_data).count
      dif_row_count = if (original_row_count != final_row_count) 1 else 0
      insert_data_ctrl_date = format_output.format(new Date().getTime)
      end_file_name = null
      final_number_of_files = null
      
      // Ajustado a 22 columnas (bigdata_ctrl_id como null)
      controlData = controlData :+ (
        status_ejecucion_str, desc_status_ejecucion, bigdata_ctrl_id, process_name, data_source_type,
        data_source_name, config.original_file_date, config.starttime_nifi, config.endtime_nifi,
        totaltime_nifi, starttime_spark, process_type, config.original_file_size, final_file_size.toString,
        original_row_count.toString, final_row_count.toString, dif_row_count.toString,
        final_number_of_files, end_file_name, path_data, config.pipelineRunId, null
      )
      
      // Eliminar archivo de processing
      delete(s"$path_processing$filename")
      
      println(s"[INFO] Archivo procesado: $filename")
    }
  } else {
    val current_time = format_output.format(new Date().getTime)
    handleError(new Exception("No hay archivos en stage"), filename, current_time, totaltime_nifi)
    throw new Exception("No hay archivos en stage")
  }
}.recover {
  case e: Exception =>
    val current_time = format_output.format(new Date().getTime)
    handleError(e, filename, current_time, totaltime_nifi)
    throw e
}

// COMMAND ----------

// MAGIC %md
// MAGIC ## Sección 6: Generar y escribir tabla de control
// MAGIC

// COMMAND ----------

if (status_ejecucion == 1 && controlData.isEmpty) {
  val current_time = format_output.format(new Date().getTime)
  controlData = controlData :+ (
    status_ejecucion_str, desc_status_ejecucion, bigdata_ctrl_id, process_name, data_source_type,
    data_source_name, config.original_file_date, config.starttime_nifi, config.endtime_nifi,
    totaltime_nifi, starttime_spark, process_type, config.original_file_size, final_file_size.toString,
    original_row_count.toString, final_row_count.toString, dif_row_count.toString,
    final_number_of_files, end_file_name, path_data, config.pipelineRunId, null
  )
}

val control_dataframe = controlData.toDF(
  "status", "desc_status", "big_data_ctrl_id", "process_name", "data_source_type", "data_source_name",
  "original_file_date", "starttime_nifi", "endtime_nifi", "totaltime_nifi", "starttime_spark",
  "process_type", "original_file_size", "final_file_size", "original_row_count", "final_row_count",
  "dif_row_count", "final_number_of_files", "end_file_name", "hdfs_path", "pipelineRunId", "bigdata_ctrl_id"
)

val end_time = new Date().getTime
val endtime_spark = format_output.format(end_time)
val totaltime_spark = (end_time - format_output.parse(starttime_spark).getTime).toFloat / 1000
val totaltime_process = totaltime_spark + (if (totaltime_nifi.nonEmpty) totaltime_nifi.toFloat else 0)

val final_control_dataframe = control_dataframe
  .withColumn("endtime_spark", lit(endtime_spark))
  .withColumn("totaltime_spark", lit(totaltime_spark))
  .withColumn("totaltime_process", lit(totaltime_process))
  .withColumn("insert_data_ctrl_date", lit(endtime_spark))
  .withColumn("original_file_date", from_unixtime(unix_timestamp($"original_file_date", "yyyy-MM-dd'T'HH:mm:ss'Z'"), "yyyy-MM-dd HH:mm:ss"))
  .withColumn("starttime_nifi", from_unixtime(unix_timestamp($"starttime_nifi", "yyyyMMdd HHmmss"), "yyyy-MM-dd HH:mm:ss"))
  .withColumn("endtime_nifi", from_unixtime(unix_timestamp($"endtime_nifi", "yyyyMMdd HHmmss"), "yyyy-MM-dd HH:mm:ss"))
  .withColumn("status", when(lit(status_ejecucion) === 0, lit("OK")).otherwise($"status"))
  .withColumn("desc_status", when(lit(status_ejecucion) === 0, lit("[OK]")).otherwise($"desc_status"))
  .select(
    "big_data_ctrl_id", "process_name", "data_source_type", "data_source_name", "original_file_date",
    "starttime_nifi", "endtime_nifi", "totaltime_nifi", "starttime_spark", "endtime_spark",
    "totaltime_spark", "totaltime_process", "insert_data_ctrl_date", "process_type", "original_file_size",
    "final_file_size", "original_row_count", "final_row_count", "dif_row_count", "final_number_of_files",
    "end_file_name", "hdfs_path", "pipelineRunId", "status", "desc_status", "bigdata_ctrl_id"
  )

println("[INFO] Contenido de final_control_dataframe:")
final_control_dataframe.show(1, truncate = false)
final_control_dataframe.printSchema()

val catalog_control_ingestas = dbutils.widgets.get("catalog_control_ingestas")
//val catalog_control_ingestas = "bi_ingestas.control.control_ingestas"
println(s"[INFO] Nombre de la tabla recuperada del parámetro: $catalog_control_ingestas")

Try {
  final_control_dataframe.write
    .mode(SaveMode.Append)
    .option("mergeSchema", "true")
    .saveAsTable(catalog_control_ingestas)
  println(s"[INFO] Datos escritos en $catalog_control_ingestas")
}.recover {
  case e: Exception =>
    println(s"[ERROR] Error al escribir en $catalog_control_ingestas: $e")
    status_ejecucion = 1
    desc_status_ejecucion = s"[ERROR] $e"
    status_ejecucion_str = "ERROR"
    throw e
}

// COMMAND ----------

if (status_ejecucion == 0) {
  println("[INFO] Ejecución completada con éxito")
  dbutils.notebook.exit("OK")
} else {
  println("[ERROR] Ejecución fallida")
  throw new Exception("Error en el proceso.")
}
