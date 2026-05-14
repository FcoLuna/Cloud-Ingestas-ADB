// Databricks notebook source
// MAGIC %md ###Librerias

// COMMAND ----------

import org.apache.spark.sql.types._
import java.util.Calendar
import java.util.{Calendar, Date}
import org.apache.spark.sql.functions._
import java.text.SimpleDateFormat
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.monotonically_increasing_id
import org.apache.spark.sql.{Row, SaveMode}
import java.sql.{Connection, DriverManager}
// define applications
import org.apache.spark.sql.functions.col
// ALMACENAR EN EXADATA
import java.text.DateFormat
import java.sql.DriverManager
import java.time.LocalDate
import java.time.format.DateTimeFormatter
import org.apache.spark.sql.DataFrame
import java.time.ZonedDateTime

// COMMAND ----------

// MAGIC %md ###Funciones

// COMMAND ----------

// MAGIC %run ../../funciones/Manejo_ADLS

// COMMAND ----------

def directoryExists(path: String): Boolean = {
  try {
    dbutils.fs.ls(path)
    true
  } catch {
    case e: java.io.FileNotFoundException => false
    case _: Throwable => false
  }
}

def numPartitionsCalc(tmp: String): Int = {
  if (directoryExists(tmp)){
    var dataSize = dbutils.fs.ls(tmp).map(_.size).sum
    var numPartitions = 0
    if (dataSize < 1073741824L) {
      // println(dataSize.getClass.getSimpleName)
      numPartitions = scala.math.ceil(dataSize / 134217728L).toInt //128 MB por particion
      numPartitions = if (numPartitions == 0) 1 else numPartitions
    } else {
      numPartitions = scala.math.ceil(dataSize / 1073741824L).toInt // 1 GB por particion
      numPartitions = if (numPartitions == 0) 1 else numPartitions
    }
    return numPartitions
  } else {
    return 1
  }
}

def load_df(dataframe_in: DataFrame, path_hdfs_in: String, path_hdfs_out: String, val_year: String, val_month: String, val_day: String, table: String) = {

  spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
  var nump= numPartitionsCalc(path_hdfs_in)

  val df = dataframe_in
  .withColumn("year", lit(val_year))
  .withColumn("month", lit(val_month))
  .withColumn("day", lit(val_day))

  try {
    if (!spark.catalog.tableExists(table)) {
    df.repartition(nump)
      .write
      .partitionBy("year","month", "day")
      .mode("overwrite")
      .option("delimiter", "\t")
      .format("delta")
      .option("path", path_hdfs_out)
      .saveAsTable(table)
    }else{
    df.repartition(nump)
      .write
      .mode("overwrite")
      .option("delimiter", "\t")
      .format("delta")
      .option("path", path_hdfs_out)
      .insertInto(table)}
      
  } catch {
    case e: Exception => println(s"[INFO]: ERROR ESCRITURA ADFS: ${e.getMessage}")
      throw e
  } finally {
  }
}


// COMMAND ----------

// MAGIC %md ###Variables

// COMMAND ----------

println("[INFO] VARIABLES DE ENTRADA =====")
val adls_path = dbutils.widgets.get("adls_path")
val catalogo = dbutils.widgets.get("catalogo")
val original_file_date = ZonedDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss'Z'"))
val path_landing = dbutils.widgets.get("path_landing")
val path_output = dbutils.widgets.get("path_output")
val table_output = dbutils.widgets.get("table_output")

val dir_adls              = adls_path + path_output
var fecha_param           = dbutils.widgets.get("fecha_param") //"yyyy-MM-dd"
val dir_cdr_roam_in       = adls_path + path_landing //input csv
val dir_roam_med_conf     = adls_path + path_output + "/raw" //output cambiar a raw
val table_roam_med_conf   = catalogo +"."+ table_output

//val var_log_date  = mensaje_nifi.split(":")(4)
//val path_log      = dir_hdfs.concat("/").concat(var_log_date).concat("/log")
var salida        = 1
var string_fecha_archivo_procesando, fecha_ultima_ejecucion = ""
var datetime_fecha_archivo_procesando: java.util.Date = null
var datetime_fecha_ultima_ejecucion: java.util.Date = null
// Define los 3 tipos de formateo de fecha 1) 20240423 11:33:23  2) 2024-04-23T08:30:45.123Z 3) 2024-04-23T08:30:45Z
var format_actual = new java.text.SimpleDateFormat("yyyyMMdd HHmmss")
var format_max = new java.text.SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'") 
var format_timestamp = new java.text.SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'") 

// COMMAND ----------

try {
  // while (fecha_param != "2021-12-31") //ORIGINAL ES 2020  
  // {
    var year_value                  = fecha_param.slice(0,4).toString
    var month_value                 = fecha_param.slice(5,7).toString
    var day_value                   = fecha_param.slice(8,10).toString

    var path_med_rmg = dir_cdr_roam_in+"/year="+year_value+"/month="+month_value+"/day="+day_value+"/"
    var path_med_rmg_out = dir_roam_med_conf
    var df_med_rmg = spark.read.format("com.databricks.spark.csv").option("header","false").load(path_med_rmg).toDF("c1").
      select(to_timestamp(concat(col("c1").substr(147,4), lit("-"), col("c1").substr(151,2), lit("-"), col("c1").substr(153,2), lit(" "), col("c1").substr(155,2), lit(":"), col("c1").substr(157,2), lit(":"), col("c1").substr(159,2))).as("FECHA"),
        trim(col("c1").substr(1, 2)).as("record_type"),
        trim(col("c1").substr(3,1)).as("event_direction"),
        trim(col("c1").substr(4,5)).as("franchise"),
        trim(col("c1").substr(9,5)).as("incoming_operator"),
        trim(col("c1").substr(14,5)).as("outgoing_operator"),
        trim(col("c1").substr(19,14)).as("incoming_product"),
        trim(col("c1").substr(33,14)).as("outgoing_product"),
        trim(col("c1").substr(47,50)).as("anum"),
        trim(col("c1").substr(97,50)).as("bnum"),
        trim(col("c1").substr(147,8)).as("event_start_date"),
        trim(col("c1").substr(155,6)).as("event_start_time"),
        col("c1").substr(161,6).as("event_duration").cast(DataTypes.LongType),
        trim(col("c1").substr(167,6)).as("network_duration"),
        col("c1").substr(173,12).as("data_volume").cast(DataTypes.LongType),
        trim(col("c1").substr(185,8)).as("data_unit"),
        col("c1").substr(193,12).as("data_volume2").cast(DataTypes.LongType),
        trim(col("c1").substr(205,8)).as("data_unit2"),
        col("c1").substr(213,12).as("data_volume3").cast(DataTypes.LongType),
        trim(col("c1").substr(225,8)).as("data_unit3"),
        trim(col("c1").substr(233,15)).as("rev_share_amount_1"),
        trim(col("c1").substr(248,3)).as("rev_share_currency_1"),
        trim(col("c1").substr(251,15)).as("rev_share_amount_2"),
        trim(col("c1").substr(266,3)).as("rev_share_currency_2"),
        trim(col("c1").substr(269,15)).as("rev_share_amount_3"),
        trim(col("c1").substr(284,3)).as("rev_share_currency_3"),
        trim(col("c1").substr(287,1)).as("tap_file_type_ind"),
        trim(col("c1").substr(288,1)).as("test_simcard_ind"),
        trim(col("c1").substr(289,3)).as("subscriber_type"),
        trim(col("c1").substr(292,14)).as("file_reception_time"),
        trim(col("c1").substr(306,1)).as("file_direction_and_type"),
        trim(col("c1").substr(307,15)).as("imsi"),
        trim(col("c1").substr(322,15)).as("esn_imei"),
        trim(col("c1").substr(337,5)).as("time_zone"),
        trim(col("c1").substr(342,16)).as("switch"),
        trim(col("c1").substr(358,5)).as("cellid"),
        trim(col("c1").substr(363,5)).as("location_area"),
        trim(col("c1").substr(368,2)).as("tele_service_code"),
        trim(col("c1").substr(370,2)).as("bearer_service_code"),
        trim(col("c1").substr(372,2)).as("suppl_service"),
        trim(col("c1").substr(374,15)).as("pdpaddress"),
        trim(col("c1").substr(389,63)).as("apn_ni"),
        trim(col("c1").substr(452,37)).as("apn_oi"),
        trim(col("c1").substr(489,10)).as("chargingid"),
        trim(col("c1").substr(499,15)).as("ggsnaddress"),
        trim(col("c1").substr(514,15)).as("sgsnaddress"),
        trim(col("c1").substr(529,1)).as("rec_entity_type"),
        trim(col("c1").substr(530,8)).as("record_number"),
        trim(col("c1").substr(538,18)).as("filename"),
        trim(col("c1").substr(556,10)).as("camel_serv_key"),
        trim(col("c1").substr(566,1)).as("camel_serv_level"),
        trim(col("c1").substr(567,20)).as("camel_dest_number"),
        trim(col("c1").substr(587,1)).as("default_call_hand_ind"),
        trim(col("c1").substr(588,5)).as("serving_bid"),
        trim(col("c1").substr(593,30)).as("called_place"),
        trim(col("c1").substr(623,13)).as("filename2"),
        trim(col("c1").substr(636,5)).as("tap_file_seq"),
        trim(col("c1").substr(641,5)).as("rap_file_seq"),
        trim(col("c1").substr(646,8)).as("ict_input_file_seq"),
        trim(col("c1").substr(654,20)).as("dialled_digits"),
        trim(col("c1").substr(674,20)).as("third_party_number"),
        trim(col("c1").substr(694,2)).as("cause_for_termination"),
        trim(col("c1").substr(696,20)).as("filename3"),
        trim(col("c1").substr(716,40)).as("record_seq_number"),
        trim(col("c1").substr(756,8)).as("discrete_rating_parameter_1"),
        trim(col("c1").substr(764,8)).as("discrete_rating_parameter_2"),
        trim(col("c1").substr(772,8)).as("discrete_rating_parameter_3"),
        trim(col("c1").substr(780,1)).as("rate_name"),
        trim(col("c1").substr(781,1)).as("rate_owner"))

    load_df(df_med_rmg, path_med_rmg, path_med_rmg_out, year_value, month_value, day_value, table_roam_med_conf)

    // fecha_param = dateAddDays(fecha_param,-1,"yyyy-MM-dd","yyyy-MM-dd")

  // }

  salida = 0
}
catch {
  case e: Exception =>
    salida = 1
    println("[ERROR] " + e)
    throw e;
}

// COMMAND ----------

// MAGIC %md ###Actualiza Archivo LastIngest

// COMMAND ----------

  try {
    // A la fecha original del archivo en el origen, lo formatea segun las variables definidas anteriormente
    // Define la misma fecha en otro formato (format_max) para el mismo archivo de origen 
    println("original_file_date: " + original_file_date)
    datetime_fecha_archivo_procesando= format_timestamp.parse(original_file_date) 
    string_fecha_archivo_procesando = format_max.format(datetime_fecha_archivo_procesando) 

    // Hay un archivo de TAG que se llama "last_ingest_time" que le ayuda a determinar la fecha de los datos del archivo de la ultima vez que se proceso 
    // Si no existe el archivo, lo crea con la fecha del archivo a procesar y lo utilizará en la siguiente ejecución para comparar
    if(!(exists_file(dir_adls + "/last_ingest_time.txt"))){ 
        println("no existe archivo con fecha maxima  [CREANDO ARCHIVO]");
        println("crear archivo")
        makeTxtFile(dir_adls + "/last_ingest_time.txt", "timestamp;\n"+string_fecha_archivo_procesando+";")
    }else{ 
      // si existe el archivo, lo lee y compara las fechas
      // fecha del archivo almacenado de la ultima ejecución de proceso
      fecha_ultima_ejecucion = readTxtFile(dir_adls + "/last_ingest_time.txt").split("\n")(1).dropRight(1) 

      // fecha del archivo a procesar que esta en la carpeta landing
      datetime_fecha_ultima_ejecucion = format_max.parse(fecha_ultima_ejecucion) // fecha maxima en formato string
      var result: Int = 0;
      result = datetime_fecha_archivo_procesando.compareTo(datetime_fecha_ultima_ejecucion);
      
      // si el valor es mayor a 0, significa que el archivo de last_ingest_time 
      if(result >= 0) {
        println("fecha del archivo a procesar es mayor que la fecha de ultima ejecución [ACTUALIZAR FECHA DEL ARCHIVO]");
        delete(dir_adls + "/last_ingest_time.txt")
        makeTxtFile(dir_adls + "/last_ingest_time.txt", "timestamp;\n"+string_fecha_archivo_procesando+";")

      }else{
        println("fecha del archivo de proceso menor o igual que la fecha de la ultima ejecución [NO HACER NADA]");
      }
    } 
  } catch {
      case e: Exception =>
        println("[ERROR] " + e)
    }
