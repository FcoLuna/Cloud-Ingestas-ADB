// Databricks notebook source
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions._
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.functions
import scala.sys.process._
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.types._

// COMMAND ----------

  // Configuración de la sesión Spark
val spark = SparkSession.builder.appName("Parsing Sprinklr Feedback").getOrCreate()
//spark.sparkContext.setLogLevel("INFO")

var parsing_in = dbutils.widgets.get("parsing_in")
var parsing_out = dbutils.widgets.get("parsing_out")
var filename = dbutils.widgets.get("nombre_archivo")

// Define las rutas de entrada y salida en Azure Data Lake
//val parsing_in = "abfss://ingestas@stbigdatadev02.dfs.core.windows.net/data/gestion_recursos/operacion_red/calicux/actividad_usuario_iptv/parsing_in/"

//val parsing_out = "abfss://ingestas@stbigdatadev02.dfs.core.windows.net/data/gestion_recursos/operacion_red/calicux/actividad_usuario_iptv/stage/"

// Nombre del archivo de entrada
//val filename = "ChileIPTV_CALICUX_Report_Session_20241227.csv"


try {
      // Definir el esquema basado en tus datos
val schema = StructType(Seq(
  StructField("playtime", DoubleType, true),
  StructField("traffic", DoubleType, true),
  StructField("seek_count", StringType, true),
  StructField("effective_playtime", DoubleType, true),
  StructField("pause_time", DoubleType, true),
  StructField("avg_seek_time", StringType, true),
  StructField("join_time", DoubleType, true),
  StructField("interruptions", LongType, true),
  StructField("buffer_ratio", DoubleType, true),
  StructField("buffer_time", DoubleType, true),
  StructField("avg_bitrate", DoubleType, true),
  StructField("happiness_score", DoubleType, true),
  StructField("avg_buffer_duration", DoubleType, true),
  StructField("playback_stalls", DoubleType, true),
  StructField("unicast_channel_change_mean_time", LongType, true),
  StructField("multicast_channel_join_mean_time", LongType, true),
  StructField("view_lost_packets", LongType, true),
  StructField("view_fixed_packets", LongType, true),
  StructField("view_packet_loss", LongType, true),
  StructField("view_lost_bursts", LongType, true),
  StructField("view_fixed_bursts", LongType, true),
  StructField("view_failed_bursts", LongType, true),
  StructField("view_avg_rssi", DoubleType, true),
  StructField("view_avg_physical_rate", DoubleType, true),
  StructField("view_avg_channel_interference", DoubleType, true),
  StructField("startup_error_count", StringType, true),
  StructField("in_stream_error_total", StringType, true),
  StructField("media_duration", DoubleType, true),
  StructField("completion_rate", DoubleType, true),
  StructField("end_time", StringType, true),
  StructField("start_time", StringType, true),
  StructField("play_head", StringType, true),
  StructField("country", StringType, true),
  StructField("city", StringType, true),
  StructField("postal_code", StringType, true),
  StructField("state_province", StringType, true),
  StructField("region", StringType, true),
  StructField("isp", StringType, true),
  StructField("connection_type", StringType, true),
  StructField("cdn", StringType, true),
  StructField("cdn_node_host", StringType, true),
  StructField("asn", StringType, true),
  StructField("viewing_mode", StringType, true),
  StructField("title", StringType, true),
  StructField("streaming_protocol", StringType, true),
  StructField("resource_domain", StringType, true),
  StructField("type", StringType, true),
  StructField("domain", StringType, true),
  StructField("device", StringType, true),
  StructField("device_type", StringType, true),
  StructField("device_vendor", StringType, true),
  StructField("device_model", StringType, true),
  StructField("browser", StringType, true),
  StructField("browser_version", StringType, true),
  StructField("os", StringType, true),
  StructField("os_version", StringType, true),
  StructField("plugin_version", StringType, true),
  StructField("player", StringType, true),
  StructField("player_version", StringType, true),
  StructField("token", StringType, true),
  StructField("metadata", StringType, true),
  StructField("error_metadata", StringType, true),
  StructField("rendition", StringType, true),
  StructField("error_name", StringType, true),
  StructField("error_description", StringType, true),
  StructField("crash_status", StringType, true),
  StructField("user_id", StringType, true),
  StructField("transaction_id", StringType, true),
  StructField("ip", StringType, true),
  StructField("ip_version", StringType, true),
  StructField("number_instance", StringType, true),
  StructField("selected_quality", StringType, true),
  StructField("code_instance", StringType, true),
  StructField("device_type2", StringType, true),
  StructField("version", StringType, true),
  StructField("connection", StringType, true),
  StructField("commercialization_type", StringType, true),
  StructField("dimension_8", StringType, true),
  StructField("dimension_9", StringType, true),
  StructField("dimension_10", StringType, true),
  StructField("qos_network", StringType, true),
  StructField("qos_tv_model", StringType, true),
  StructField("qos_origin", StringType, true),
  StructField("qos_device", StringType, true),
  StructField("qos_mac_address", StringType, true),
  StructField("qos_topology_level1", StringType, true),
  StructField("qos_topology_level2", StringType, true),
  StructField("qos_topology_level3", StringType, true),
  StructField("qos_topology_level4", StringType, true),
  StructField("qos_topology_level5", StringType, true),
  StructField("qos_topology_level1_type_name", StringType, true),
  StructField("qos_topology_level2_type_name", StringType, true),
  StructField("qos_topology_level3_type_name", StringType, true),
  StructField("qos_topology_level4_type_name", StringType, true),
  StructField("qos_topology_level5_type_name", StringType, true),
  StructField("qos_topology_level2_slot", StringType, true),
  StructField("qos_topology_level2_port", StringType, true),
  StructField("qos_topology_level3_slot", StringType, true),
  StructField("qos_topology_level3_port", StringType, true),
  StructField("qos_topology_level4_slot", StringType, true),
  StructField("qos_topology_level4_port", StringType, true),
  StructField("qos_topology_level5_slot", StringType, true),
  StructField("qos_topology_level5_port", StringType, true),
  StructField("qos_boot_rom_version", StringType, true),
  StructField("qos_hdmi_format", StringType, true),
  StructField("qos_local_pvr", StringType, true),
  StructField("qos_software_version", StringType, true),
  StructField("qos_unique_user", StringType, true),
  StructField("qos_content_type", StringType, true),
  StructField("qos_certification", StringType, true),
  StructField("qos_device_error_code", StringType, true),
  StructField("qos_device_error_message", StringType, true),
  StructField("qos_geographic_area", StringType, true),
  StructField("qos_hgu_model", StringType, true),
  StructField("qos_stb_model", StringType, true),
  StructField("qos_multicast_group", StringType, true),
  StructField("latitude", StringType, true),
  StructField("longitude", StringType, true),
  StructField("cdn_request_type", StringType, true),
  StructField("media_resource", StringType, true),
  StructField("playback_status", StringType, true),
  StructField("playback_id", StringType, true)
))

// Leer el archivo CSV con opciones adecuadas
val df = spark.read
  .option("header", "true")
  .option("delimiter", ";") // Usa el delimitador `;`
  .option("quote", "\"") // Maneja posibles comillas en campos
  .option("escape", "\\") // Maneja caracteres escapados
  .schema(schema) // Especifica el esquema
  .csv(parsing_in + filename)

// Seleccionar únicamente las columnas definidas en el esquema
val selectedColumns = schema.fields.map(_.name)
val cleanDf = df.select(selectedColumns.map(col): _*)

val df_calicux = cleanDf.select(df.columns.map(colName => col(s"`$colName`").as(colName.replaceAll("\\.", "").replaceAll("\"", "").replaceAll(" ", "_"))): _*)

val df_calicux2 = df_calicux.select(
  df_calicux.columns.map(colName =>
    when(lower(trim(col(colName))) === "null", lit(null))
      .otherwise(col(colName))
      .alias(colName)
  ): _*)

val df_calicux3 = df_calicux2.filter(
  (col("playtime").isNull || col("playtime").cast(DoubleType).isNotNull) &&
  (col("traffic").isNull || col("traffic").cast(DoubleType).isNotNull) &&
  (col("seek_count").isNull || col("seek_count").cast(StringType).isNotNull) &&
  (col("effective_playtime").isNull || col("effective_playtime").cast(DoubleType).isNotNull) &&
  (col("pause_time").isNull || col("pause_time").cast(DoubleType).isNotNull) &&
  (col("avg_seek_time").isNull || col("avg_seek_time").cast(StringType).isNotNull) &&
  (col("join_time").isNull || col("join_time").cast(DoubleType).isNotNull) &&
  (col("interruptions").isNull || col("interruptions").cast(LongType).isNotNull) &&
  (col("buffer_ratio").isNull || col("buffer_ratio").cast(DoubleType).isNotNull) &&
  (col("buffer_time").isNull || col("buffer_time").cast(DoubleType).isNotNull) &&
  (col("avg_bitrate").isNull || col("avg_bitrate").cast(DoubleType).isNotNull) &&
  (col("happiness_score").isNull || col("happiness_score").cast(DoubleType).isNotNull) &&
  (col("avg_buffer_duration").isNull || col("avg_buffer_duration").cast(DoubleType).isNotNull) &&
  (col("playback_stalls").isNull || col("playback_stalls").cast(DoubleType).isNotNull) &&
  (col("unicast_channel_change_mean_time").isNull || col("unicast_channel_change_mean_time").cast(LongType).isNotNull) &&
  (col("multicast_channel_join_mean_time").isNull || col("multicast_channel_join_mean_time").cast(LongType).isNotNull) &&
  (col("view_lost_packets").isNull || col("view_lost_packets").cast(LongType).isNotNull) &&
  (col("view_fixed_packets").isNull || col("view_fixed_packets").cast(LongType).isNotNull) &&
  (col("view_packet_loss").isNull || col("view_packet_loss").cast(LongType).isNotNull) &&
  (col("view_lost_bursts").isNull || col("view_lost_bursts").cast(LongType).isNotNull) &&
  (col("view_fixed_bursts").isNull || col("view_fixed_bursts").cast(LongType).isNotNull) &&
  (col("view_failed_bursts").isNull || col("view_failed_bursts").cast(LongType).isNotNull) &&
  (col("view_avg_rssi").isNull || col("view_avg_rssi").cast(DoubleType).isNotNull) &&
  (col("view_avg_physical_rate").isNull || col("view_avg_physical_rate").cast(DoubleType).isNotNull) &&
  (col("view_avg_channel_interference").isNull || col("view_avg_channel_interference").cast(DoubleType).isNotNull) &&
  (col("startup_error_count").isNull || col("startup_error_count").cast(StringType).isNotNull) &&
  (col("in_stream_error_total").isNull || col("in_stream_error_total").cast(StringType).isNotNull) &&
  (col("media_duration").isNull || col("media_duration").cast(DoubleType).isNotNull) &&
  (col("completion_rate").isNull || col("completion_rate").cast(DoubleType).isNotNull) &&
  (col("end_time").isNull || col("end_time").cast(StringType).isNotNull) &&
  (col("start_time").isNull || col("start_time").cast(StringType).isNotNull) &&
  (col("play_head").isNull || col("play_head").cast(StringType).isNotNull) &&
  (col("country").isNull || col("country").cast(StringType).isNotNull) &&
  (col("city").isNull || col("city").cast(StringType).isNotNull) &&
  (col("postal_code").isNull || col("postal_code").cast(StringType).isNotNull) &&
  (col("state_province").isNull || col("state_province").cast(StringType).isNotNull) &&
  (col("region").isNull || col("region").cast(StringType).isNotNull) &&
  (col("isp").isNull || col("isp").cast(StringType).isNotNull) &&
  (col("connection_type").isNull || col("connection_type").cast(StringType).isNotNull) &&
  (col("cdn").isNull || col("cdn").cast(StringType).isNotNull) &&
  (col("cdn_node_host").isNull || col("cdn_node_host").cast(StringType).isNotNull) &&
  (col("asn").isNull || col("asn").cast(StringType).isNotNull) &&
  (col("viewing_mode").isNull || col("viewing_mode").cast(StringType).isNotNull) &&
  (col("title").isNull || col("title").cast(StringType).isNotNull) &&
  (col("streaming_protocol").isNull || col("streaming_protocol").cast(StringType).isNotNull) &&
  (col("resource_domain").isNull || col("resource_domain").cast(StringType).isNotNull) &&
  (col("type").isNull || col("type").cast(StringType).isNotNull) &&
  (col("domain").isNull || col("domain").cast(StringType).isNotNull) &&
  (col("device").isNull || col("device").cast(StringType).isNotNull) &&
  (col("device_type").isNull || col("device_type").cast(StringType).isNotNull) &&
  (col("device_vendor").isNull || col("device_vendor").cast(StringType).isNotNull) &&
  (col("device_model").isNull || col("device_model").cast(StringType).isNotNull) &&
  (col("browser").isNull || col("browser").cast(StringType).isNotNull) &&
  (col("browser_version").isNull || col("browser_version").cast(StringType).isNotNull) &&
  (col("os").isNull || col("os").cast(StringType).isNotNull) &&
  (col("os_version").isNull || col("os_version").cast(StringType).isNotNull) &&
  (col("plugin_version").isNull || col("plugin_version").cast(StringType).isNotNull) &&
  (col("player").isNull || col("player").cast(StringType).isNotNull) &&
  (col("player_version").isNull || col("player_version").cast(StringType).isNotNull) &&
  (col("token").isNull || col("token").cast(StringType).isNotNull) &&
  (col("metadata").isNull || col("metadata").cast(StringType).isNotNull) &&
  (col("error_metadata").isNull || col("error_metadata").cast(StringType).isNotNull) &&
  (col("rendition").isNull || col("rendition").cast(StringType).isNotNull) &&
  (col("error_name").isNull || col("error_name").cast(StringType).isNotNull) &&
  (col("error_description").isNull || col("error_description").cast(StringType).isNotNull) &&
  (col("crash_status").isNull || col("crash_status").cast(StringType).isNotNull) &&
  (col("user_id").isNull || col("user_id").cast(StringType).isNotNull) &&
  (col("transaction_id").isNull || col("transaction_id").cast(StringType).isNotNull) &&
  (col("ip").isNull || col("ip").cast(StringType).isNotNull) &&
  (col("ip_version").isNull || col("ip_version").cast(StringType).isNotNull) &&
  (col("number_instance").isNull || col("number_instance").cast(StringType).isNotNull) &&
  (col("selected_quality").isNull || col("selected_quality").cast(StringType).isNotNull) &&
  (col("code_instance").isNull || col("code_instance").cast(StringType).isNotNull) &&
  (col("device_type2").isNull || col("device_type2").cast(StringType).isNotNull) &&
  (col("version").isNull || col("version").cast(StringType).isNotNull) &&
  (col("connection").isNull || col("connection").cast(StringType).isNotNull) &&
  (col("commercialization_type").isNull || col("commercialization_type").cast(StringType).isNotNull) &&
  (col("dimension_8").isNull || col("dimension_8").cast(StringType).isNotNull) &&
  (col("dimension_9").isNull || col("dimension_9").cast(StringType).isNotNull) &&
  (col("dimension_10").isNull || col("dimension_10").cast(StringType).isNotNull) &&
  (col("qos_network").isNull || col("qos_network").cast(StringType).isNotNull) &&
  (col("qos_tv_model").isNull || col("qos_tv_model").cast(StringType).isNotNull) &&
  (col("qos_origin").isNull || col("qos_origin").cast(StringType).isNotNull) &&
  (col("qos_device").isNull || col("qos_device").cast(StringType).isNotNull) &&
  (col("qos_mac_address").isNull || col("qos_mac_address").cast(StringType).isNotNull) &&
  (col("qos_topology_level1").isNull || col("qos_topology_level1").cast(StringType).isNotNull) &&
  (col("qos_topology_level2").isNull || col("qos_topology_level2").cast(StringType).isNotNull) &&
  (col("qos_topology_level3").isNull || col("qos_topology_level3").cast(StringType).isNotNull) &&
  (col("qos_topology_level4").isNull || col("qos_topology_level4").cast(StringType).isNotNull) &&
  (col("qos_topology_level5").isNull || col("qos_topology_level5").cast(StringType).isNotNull) &&
  (col("qos_topology_level1_type_name").isNull || col("qos_topology_level1_type_name").cast(StringType).isNotNull) &&
  (col("qos_topology_level2_type_name").isNull || col("qos_topology_level2_type_name").cast(StringType).isNotNull) &&
  (col("qos_topology_level3_type_name").isNull || col("qos_topology_level3_type_name").cast(StringType).isNotNull) &&
  (col("qos_topology_level4_type_name").isNull || col("qos_topology_level4_type_name").cast(StringType).isNotNull) &&
  (col("qos_topology_level5_type_name").isNull || col("qos_topology_level5_type_name").cast(StringType).isNotNull) &&
  (col("qos_topology_level2_slot").isNull || col("qos_topology_level2_slot").cast(StringType).isNotNull) &&
  (col("qos_topology_level2_port").isNull || col("qos_topology_level2_port").cast(StringType).isNotNull) &&
  (col("qos_topology_level3_slot").isNull || col("qos_topology_level3_slot").cast(StringType).isNotNull) &&
  (col("qos_topology_level3_port").isNull || col("qos_topology_level3_port").cast(StringType).isNotNull) &&
  (col("qos_topology_level4_slot").isNull || col("qos_topology_level4_slot").cast(StringType).isNotNull) &&
  (col("qos_topology_level4_port").isNull || col("qos_topology_level4_port").cast(StringType).isNotNull) &&
  (col("qos_topology_level5_slot").isNull || col("qos_topology_level5_slot").cast(StringType).isNotNull) &&
  (col("qos_topology_level5_port").isNull || col("qos_topology_level5_port").cast(StringType).isNotNull) &&
  (col("qos_boot_rom_version").isNull || col("qos_boot_rom_version").cast(StringType).isNotNull) &&
  (col("qos_hdmi_format").isNull || col("qos_hdmi_format").cast(StringType).isNotNull) &&
  (col("qos_local_pvr").isNull || col("qos_local_pvr").cast(StringType).isNotNull) &&
  (col("qos_software_version").isNull || col("qos_software_version").cast(StringType).isNotNull) &&
  (col("qos_unique_user").isNull || col("qos_unique_user").cast(StringType).isNotNull) &&
  (col("qos_content_type").isNull || col("qos_content_type").cast(StringType).isNotNull) &&
  (col("qos_certification").isNull || col("qos_certification").cast(StringType).isNotNull) &&
  (col("qos_device_error_code").isNull || col("qos_device_error_code").cast(StringType).isNotNull) &&
  (col("qos_device_error_message").isNull || col("qos_device_error_message").cast(StringType).isNotNull) &&
  (col("qos_geographic_area").isNull || col("qos_geographic_area").cast(StringType).isNotNull) &&
  (col("qos_hgu_model").isNull || col("qos_hgu_model").cast(StringType).isNotNull) &&
  (col("qos_stb_model").isNull || col("qos_stb_model").cast(StringType).isNotNull) &&
  (col("qos_multicast_group").isNull || col("qos_multicast_group").cast(StringType).isNotNull) &&
  (col("latitude").isNull || col("latitude").cast(StringType).isNotNull) &&
  (col("longitude").isNull || col("longitude").cast(StringType).isNotNull) &&
  (col("cdn_request_type").isNull || col("cdn_request_type").cast(StringType).isNotNull) &&
  (col("media_resource").isNull || col("media_resource").cast(StringType).isNotNull) &&
  (col("playback_status").isNull || col("playback_status").cast(StringType).isNotNull) &&
  (col("playback_id").isNull || col("playback_id").cast(StringType).isNotNull)
  )

df_calicux3.repartition(1).write.option("header", "true").option("delimiter", ";").mode("append").csv(parsing_out)

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
files.foreach(file => dbutils.fs.rm(file.path, true))
