// Databricks notebook source
val path_adls = dbutils.widgets.get("path_adls")
val path_adls_smartcare = dbutils.widgets.get("path_adls_smartcare")
val catalogo = dbutils.widgets.get("catalogo")
val path_landing = path_adls_smartcare + dbutils.widgets.get("ruta_origen")
val tabla_salida = catalogo +"."+ dbutils.widgets.get("tabla_salida")
val path_salida = path_adls + dbutils.widgets.get("path_salida")

// COMMAND ----------

import org.apache.spark.sql.types._

val columnas = Seq("BEGIN_TIME","BEGIN_TIME_MSEL","END_TIME","END_TIME_MSEL","PROT_CATEGORY","PROT_TYPE","L7_CARRIER_PROT","MSISDN","IMSI","IMEI","ROAM_DIRECTION","MS_IP","SERVER_IP","MS_PORT","SERVER_PORT","APN","SGSN_SIG_IP","GGSN_SIG_IP","SGSN_USER_IP","GGSN_USER_IP","MCC","MNC","RAT","LAC","RAC","SAC","CI","L4_TYPE","L4_UL_THROUGHPUT","L4_DW_THROUGHPUT","L4_UL_GOODPUT","L4_DW_GOODPUT","L4_UL_PACKETS","L4_DW_PACKETS","TCP_CONN_STATES","TAC","ECI","TCP_RTT_STEP1","TCP_UL_RETRANS_WITHPL","TCP_DW_RETRANS_WITHPL","TCP_UL_PACKAGES_WITHPL","TCP_DW_PACKAGES_WITHPL","RAN_NE_USER_IP","TCP_RTT","TCP_UL_OUTOFSEQU","TCP_DW_OUTOFSEQU","TCP_WIN_SIZE","TCP_MSS","INTERFACEID","HOMEMCC","HOMEMNC","HOST","USER_AGENT","FST_URI","TETHERING_FLAG","MS_WIN_STAT_TOTAL_NUM","MS_WIN_STAT_SMALL_NUM","MS_ACK_TO_1STGET_DELAY","SERVER_ACK_TO_1STDATA_DELAY","AVG_UL_RTT","AVG_DW_RTT","UL_RTT_LONG_NUM","DW_RTT_LONG_NUM","UL_RTT_STAT_NUM","DW_RTT_STAT_NUM","USER_PROBE_UL_LOST_PKT","SERVER_PROBE_UL_LOST_PKT","SERVER_PROBE_DW_LOST_PKT","USER_PROBE_DW_LOST_PKT","L7_UL_GOODPUT_FULL_MSS","DATATRANS_UL_DURATION","L7_DW_GOODPUT_FULL_MSS","DATATRANS_DW_DURATION","UL_SMALLPACKET_NUM","DW_SMALLPACKET_NUM","DL_SERIOUS_OUT_OF_ORDER_NUM","DL_SLIGHT_OUT_OF_ORDER_NUM","DL_FLIGHT_TOTAL_SIZE","DL_FLIGHT_TOTAL_NUM","DL_MAX_FLIGHT_SIZE","UL_SERIOUS_OUT_OF_ORDER_NUM","UL_SLIGHT_OUT_OF_ORDER_NUM","UL_FLIGHT_TOTAL_SIZE","UL_FLIGHT_TOTAL_NUM","UL_MAX_FLIGHT_SIZE","USER_DL_SLIGHT_OUT_OF_ORDER_PACKETS","SERVER_UL_SLIGHT_OUT_OF_ORDER_PACKETS","DL_CONTINUOUS_RETRANSMISSION_DELAY","USER_HUNGRY_DELAY","SERVER_HUNGRY_DELAY","CHARGE_ID","SV","SUB_PROT_TYPE","CALL_DURATION","PKT_NUM_UL_STG1","PKT_NUM_UL_STG2","PKT_NUM_UL_STG3","PKT_NUM_UL_STG4","PKT_NUM_DL_STG1","PKT_NUM_DL_STG2","PKT_NUM_DL_STG3","PKT_NUM_DL_STG4","AVG_UL_PKT_SIZE","AVG_DL_PKT_SIZE","AVG_UL_INTERVAL","AVG_DL_INTERVAL","AVG_UL_JITTER","AVG_DL_JITTER","APP_ID","AVG_UL_RTT_MICRO_SEC","AVG_DW_RTT_MICRO_SEC","TRAFFIC_CLASS_NEG","THP_NEG","QCI_NEG","AVG_UL_TRANS_DELAY","AVG_DL_TRANS_DELAY","MAX_UL_JITTER","MIN_DL_JITTER","MAX_DL_JITTER","DL_DATATRANS_SEGNUM","MIN_UL_JITTER","TOTAL_TCP_RTT_STEP1","UL_DATATRANS_SEGNUM","TCP_DL_MAX_MSS","TOTAL_TCP_RTT","GET_2_FIRST_DL_DATA_DELAY","TCP_UL_MAX_MSS","SSL_HANDSHAKE_FINISH_2_FIRST_PAYLOAD","MSACK_2_FIN_DELAY","FIRST_SSL_DL_CONTINUOUS_TRANS_DELAY","LASTACK_2_CLIENTHELLO","ENCRYPTED_MODEL_FLAG","FIRST_SSL_UL_CONTINUOUS_TRANS_DELAY","ACTIVE_TRANS_NUM","SERVICE_VALID_FLAG","DL_RTT_LONG_NUM","COMBINED_FLOW_NUM","DL_RTT_TH1_TH2_NUM","SERVICE_CUTOFF_FLAG","DL_RTT_TH3_TH4_NUM","DL_RTT_LT_TH1_NUM","UL_RTT_LT_TH1_NUM","DL_RTT_TH2_TH3_NUM","UL_RTT_GT_TH2_NUM","DL_RTT_GT_TH4_NUM","CONTENT_TYPE","UL_RTT_TH1_TH2_NUM","TRANS_TYPE","_corrupt_record")

val schema = StructType(columnas.map(StructField(_, StringType, true)))

val voip = spark.readStream
  .format("cloudFiles")
  .option("cloudFiles.format", "csv") // Especifica que los archivos son CSV
  .option("header", "true") // Si los archivos tienen encabezado
  .option("delimiter", "|") // Configura el delimitador
  .option("cloudFiles.schemaLocation", s"${path_adls_smartcare}/schemas/voip") // Ruta para el esquema
  .schema(schema) // Usa el esquema definido
  .load(path_landing)
  //.toDF(columnas: _*)

// COMMAND ----------

val df = voip.
withColumn("year", date_format(from_unixtime(col("BEGIN_TIME")), "yyyy")).
withColumn("month", date_format(from_unixtime(col("BEGIN_TIME")), "MM")).
withColumn("day", date_format(from_unixtime(col("BEGIN_TIME")), "dd")).
withColumn("hour", date_format(from_unixtime(col("BEGIN_TIME")), "HH")).
select(col("BEGIN_TIME").cast("int"),col("BEGIN_TIME_MSEL"),col("END_TIME").cast("int"),col("END_TIME_MSEL"),col("PROT_CATEGORY").cast("int"),col("PROT_TYPE").cast("int"),col("L7_CARRIER_PROT"),col("MSISDN"),col("IMSI"),col("IMEI"),col("ROAM_DIRECTION"),col("MS_IP"),col("SERVER_IP"),col("MS_PORT"),col("SERVER_PORT").cast("int"),col("APN"),col("SGSN_SIG_IP"),col("GGSN_SIG_IP"),col("SGSN_USER_IP"),col("GGSN_USER_IP"),col("MCC"),col("MNC"),col("RAT").cast("short"),col("LAC"),col("RAC"),col("SAC"),col("CI"),col("L4_TYPE"),col("L4_UL_THROUGHPUT").cast("bigint"),col("L4_DW_THROUGHPUT").cast("bigint"),col("L4_UL_GOODPUT"),col("L4_DW_GOODPUT"),col("L4_UL_PACKETS"),col("L4_DW_PACKETS"),col("TCP_CONN_STATES"),col("TAC"),col("ECI"),col("TCP_RTT_STEP1").cast("bigint"),col("TCP_UL_RETRANS_WITHPL"),col("TCP_DW_RETRANS_WITHPL"),col("TCP_UL_PACKAGES_WITHPL"),col("TCP_DW_PACKAGES_WITHPL"),col("RAN_NE_USER_IP"),col("TCP_RTT").cast("bigint"),col("TCP_UL_OUTOFSEQU"),col("TCP_DW_OUTOFSEQU"),col("TCP_WIN_SIZE"),col("TCP_MSS"),col("INTERFACEID").cast("short"),col("HOMEMCC"),col("HOMEMNC"),col("HOST"),col("USER_AGENT"),col("FST_URI"),col("TETHERING_FLAG").cast("short"),col("MS_WIN_STAT_TOTAL_NUM"),col("MS_WIN_STAT_SMALL_NUM"),col("MS_ACK_TO_1STGET_DELAY"),col("SERVER_ACK_TO_1STDATA_DELAY"),col("AVG_UL_RTT"),col("AVG_DW_RTT"),col("UL_RTT_LONG_NUM"),col("DW_RTT_LONG_NUM"),col("UL_RTT_STAT_NUM"),col("DW_RTT_STAT_NUM"),col("USER_PROBE_UL_LOST_PKT"),col("SERVER_PROBE_UL_LOST_PKT"),col("SERVER_PROBE_DW_LOST_PKT"),col("USER_PROBE_DW_LOST_PKT"),col("L7_UL_GOODPUT_FULL_MSS"),col("DATATRANS_UL_DURATION"),col("L7_DW_GOODPUT_FULL_MSS"),col("DATATRANS_DW_DURATION"),col("UL_SMALLPACKET_NUM"),col("DW_SMALLPACKET_NUM"),col("DL_SERIOUS_OUT_OF_ORDER_NUM"),col("DL_SLIGHT_OUT_OF_ORDER_NUM"),col("DL_FLIGHT_TOTAL_SIZE"),col("DL_FLIGHT_TOTAL_NUM"),col("DL_MAX_FLIGHT_SIZE"),col("UL_SERIOUS_OUT_OF_ORDER_NUM"),col("UL_SLIGHT_OUT_OF_ORDER_NUM"),col("UL_FLIGHT_TOTAL_SIZE"),col("UL_FLIGHT_TOTAL_NUM"),col("UL_MAX_FLIGHT_SIZE"),col("USER_DL_SLIGHT_OUT_OF_ORDER_PACKETS"),col("SERVER_UL_SLIGHT_OUT_OF_ORDER_PACKETS"),col("DL_CONTINUOUS_RETRANSMISSION_DELAY"),col("USER_HUNGRY_DELAY"),col("SERVER_HUNGRY_DELAY"),col("CHARGE_ID"),col("SV"),col("SUB_PROT_TYPE").cast("int"),col("CALL_DURATION"),col("PKT_NUM_UL_STG1"),col("PKT_NUM_UL_STG2"),col("PKT_NUM_UL_STG3"),col("PKT_NUM_UL_STG4"),col("PKT_NUM_DL_STG1"),col("PKT_NUM_DL_STG2"),col("PKT_NUM_DL_STG3"),col("PKT_NUM_DL_STG4"),col("AVG_UL_PKT_SIZE"),col("AVG_DL_PKT_SIZE"),col("AVG_UL_INTERVAL"),col("AVG_DL_INTERVAL"),col("AVG_UL_JITTER"),col("AVG_DL_JITTER"),col("APP_ID").cast("int"),col("AVG_UL_RTT_MICRO_SEC"),col("AVG_DW_RTT_MICRO_SEC"),col("TRAFFIC_CLASS_NEG"),col("THP_NEG"),col("QCI_NEG"),col("AVG_UL_TRANS_DELAY"),col("AVG_DL_TRANS_DELAY"),col("MAX_UL_JITTER"),col("MIN_DL_JITTER"),col("MAX_DL_JITTER"),col("DL_DATATRANS_SEGNUM"),col("MIN_UL_JITTER"),col("TOTAL_TCP_RTT_STEP1"),col("UL_DATATRANS_SEGNUM"),col("TCP_DL_MAX_MSS"),col("TOTAL_TCP_RTT"),col("GET_2_FIRST_DL_DATA_DELAY"),col("TCP_UL_MAX_MSS"),col("SSL_HANDSHAKE_FINISH_2_FIRST_PAYLOAD"),col("MSACK_2_FIN_DELAY"),col("FIRST_SSL_DL_CONTINUOUS_TRANS_DELAY"),col("LASTACK_2_CLIENTHELLO"),col("ENCRYPTED_MODEL_FLAG"),col("FIRST_SSL_UL_CONTINUOUS_TRANS_DELAY"),col("ACTIVE_TRANS_NUM"),col("SERVICE_VALID_FLAG"),col("DL_RTT_LONG_NUM"),col("COMBINED_FLOW_NUM"),col("DL_RTT_TH1_TH2_NUM"),col("SERVICE_CUTOFF_FLAG"),col("DL_RTT_TH3_TH4_NUM"),col("DL_RTT_LT_TH1_NUM"),col("UL_RTT_LT_TH1_NUM"),col("DL_RTT_TH2_TH3_NUM"),col("UL_RTT_GT_TH2_NUM"),col("DL_RTT_GT_TH4_NUM"),col("CONTENT_TYPE"),col("UL_RTT_TH1_TH2_NUM"),col("TRANS_TYPE"),col("_corrupt_record"),col("year"),col("month"),col("day"),col("hour"))


// COMMAND ----------

import scala.concurrent.duration._
import org.apache.spark.sql.streaming.StreamingQuery
import org.apache.spark.sql.streaming.Trigger

// Escritura en streaming a Delta Lake
val query = df.writeStream
  .format("delta")
  .outputMode("append") // Solo agrega nuevos datos
  .partitionBy("year", "month", "day", "hour")
  .option("checkpointLocation", s"${path_adls_smartcare}/checkpoints/voip") // Ruta de checkpoints
  .option("path", path_salida)
  .option("maxFilesPerTrigger", 100)
  .trigger(Trigger.AvailableNow())
  .table(tabla_salida) // Guarda en la tabla Delta

// Monitoreo del proceso en tiempo real
while (query.isActive) {
  println("======================================")
  println(s"🔄 Estado del Streaming: ${query.status}")

  // Obtener el progreso del streaming en formato JSON
  val lastProgress = query.lastProgress

  if (lastProgress != null) {
    val progressJson = lastProgress.json  // Convertir a JSON

    println("📊 Último progreso del Streaming:")
    println(progressJson)

    // Extraer los archivos procesados desde el JSON
    val pattern = """"path":"(.*?)"""".r

    val archivosProcesados = pattern.findAllIn(progressJson).matchData.map(_.group(1)).toList

    if (archivosProcesados.nonEmpty) {
      println("📄 Archivos procesados recientemente:")
      archivosProcesados.foreach(archivo => println(s"- $archivo"))
    } else {
      println("📂 No se detectaron archivos procesados en este batch.")
    }

    // Mostrar la ubicación del último checkpoint
    println(s"📌 Último Checkpoint: ${s"${path_adls_smartcare}/checkpoints/voip"}")

  } else {
    println("⚠️ No hay progreso disponible todavía.")
  }

  println("======================================")
  Thread.sleep(10000)  // Consulta cada 10 segundos
}

//query.awaitTermination()
