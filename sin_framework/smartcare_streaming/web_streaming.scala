// Databricks notebook source
val path_adls = dbutils.widgets.get("path_adls")
val path_adls_smartcare = dbutils.widgets.get("path_adls_smartcare")
val catalogo = dbutils.widgets.get("catalogo")
val landing_path = path_adls_smartcare + dbutils.widgets.get("ruta_origen")
val tabla_salida = catalogo +"."+ dbutils.widgets.get("tabla_salida")
val path_salida = path_adls + dbutils.widgets.get("path_salida")

// COMMAND ----------

import org.apache.spark.sql.types._

val columnas = Seq("SID","INTERFACEID","BEGIN_TIME","BEGIN_TIME_MSEL","END_TIME","END_TIME_MSEL","PROT_CATEGORY","PROT_TYPE","L7_CARRIER_PROT","MSISDN","IMSI","IMEI","ROAM_DIRECTION","MS_IP","SERVER_IP","MS_PORT","SERVER_PORT","APN","SGSN_USER_IP","GGSN_USER_IP","MCC","MNC","RAT","LAC","RAC","SAC","CI","BROWSER_TYPE","L4_UL_THROUGHPUT","L4_DW_THROUGHPUT","L4_UL_GOODPUT","L4_DW_GOODPUT","L4_UL_PACKETS","L4_DW_PACKETS","TCP_CONN_STATES","TCP_RTT","TCP_UL_OUTOFSEQU","TCP_DW_OUTOFSEQU","TCP_UL_RETRANS","TCP_DW_RETRANS","TCP_WIN_SIZE","HOST","FST_URI","GET_FST_FLAG","GET_FST_FAILED_CODE","GET_FST_DELAY_MSEC","GET_NUM","GET_SUCCEED_NUM","GET_TIMEOUT_NUM","GET_OK_DELAY_TIME","POST_NUM","POST_SUCCEED_NUM","POST_TO_NUM","POST_OK_DELAY_TIME","THROUGHPUT_ALL_PAGE","TAC","ECI","TCP_RTT_STEP1","TCP_UL_RETRANS_WITHPL","TCP_DW_RETRANS_WITHPL","TCP_UL_PACKAGES_WITHPL","TCP_DW_PACKAGES_WITHPL","RAN_NE_USER_IP","FIRSTDNSDELAY","FIRSTDNSCODE","PAGESIZE","DNSNUM","DNSSUCCNUM","DNSDELAY","PAGE_SR_DELAY","PTMSI","HOMEMCC","HOMEMNC","PREPAID_FLAG","USER_AGENT","DATATRANS_UL_DURATION","DATATRANS_DW_DURATION","MS_WIN_STAT_TOTAL_NUM","MS_WIN_STAT_SMALL_NUM","MS_ACK_TO_1STGET_DELAY","SERVER_ACK_TO_1STDATA_DELAY","AVG_UL_RTT","AVG_DW_RTT","UL_RTT_LONG_NUM","DW_RTT_LONG_NUM","UL_RTT_STAT_NUM","DW_RTT_STAT_NUM","USER_PROBE_UL_LOST_PKT","SERVER_PROBE_UL_LOST_PKT","SERVER_PROBE_DW_LOST_PKT","USER_PROBE_DW_LOST_PKT","TETHERING_FLAG","CHARGING_CHARACTERISTICS","DL_SERIOUS_OUT_OF_ORDER_NUM","DL_SLIGHT_OUT_OF_ORDER_NUM","DL_FLIGHT_TOTAL_SIZE","DL_FLIGHT_TOTAL_NUM","DL_MAX_FLIGHT_SIZE","UL_SERIOUS_OUT_OF_ORDER_NUM","UL_SLIGHT_OUT_OF_ORDER_NUM","UL_FLIGHT_TOTAL_SIZE","UL_FLIGHT_TOTAL_NUM","UL_MAX_FLIGHT_SIZE","USER_DL_SLIGHT_OUT_OF_ORDER_PACKETS","SERVER_UL_SLIGHT_OUT_OF_ORDER_PACKETS","DL_CONTINUOUS_RETRANSMISSION_DELAY","USER_HUNGRY_DELAY","SERVER_HUNGRY_DELAY","SV","FIRST_DNS_RETRANS_NUM","SUB_PROT_TYPE","APP_ID","DNS2SYN","FAIL_CLASS","PAGE_SIZE_CATGORY","AVG_UL_RTT_MICRO_SEC","AVG_DW_RTT_MICRO_SEC","L7_UL_GOODPUT_FULL_MSS","QCI_NEG","TOTAL_TCP_RTT","TRANS_TYPE","CONTENT_TYPE","_corrupt_record")
val schema = StructType(columnas.map(StructField(_, StringType, true)))

val web = spark.readStream
  .format("cloudFiles")
  .option("cloudFiles.format", "csv") // Especifica que los archivos son CSV
  .option("header", "true") // Si los archivos tienen encabezado
  .option("delimiter", "|") // Configura el delimitador
  .option("fileNameOnly","true")
  .option("maxFilesPerTrigger", 1000)
  .option("lastFirst","true")
  .schema(schema) // Usa el esquema definido
  .load(landing_path)

// COMMAND ----------

val df = web.
withColumn("year", date_format(from_unixtime(col("BEGIN_TIME")), "yyyy")).
withColumn("month", date_format(from_unixtime(col("BEGIN_TIME")), "MM")).
withColumn("day", date_format(from_unixtime(col("BEGIN_TIME")), "dd")).
withColumn("hour", date_format(from_unixtime(col("BEGIN_TIME")), "HH")).
select(col("SID"),col("INTERFACEID").cast("short"),col("BEGIN_TIME").cast("int"),col("BEGIN_TIME_MSEL"),col("END_TIME").cast("int"),col("END_TIME_MSEL"),col("PROT_CATEGORY").cast("int"),col("PROT_TYPE").cast("int"),col("L7_CARRIER_PROT"),col("MSISDN"),col("IMSI"),col("IMEI"),col("ROAM_DIRECTION"),col("MS_IP"),col("SERVER_IP"),col("MS_PORT"),col("SERVER_PORT").cast("int"),col("APN"),col("SGSN_USER_IP"),col("GGSN_USER_IP"),col("MCC"),col("MNC"),col("RAT").cast("short"),col("LAC"),col("RAC"),col("SAC"),col("CI"),col("BROWSER_TYPE"),col("L4_UL_THROUGHPUT").cast("bigint"),col("L4_DW_THROUGHPUT").cast("bigint"),col("L4_UL_GOODPUT"),col("L4_DW_GOODPUT"),col("L4_UL_PACKETS"),col("L4_DW_PACKETS"),col("TCP_CONN_STATES"),col("TCP_RTT").cast("bigint"),col("TCP_UL_OUTOFSEQU"),col("TCP_DW_OUTOFSEQU"),col("TCP_UL_RETRANS"),col("TCP_DW_RETRANS"),col("TCP_WIN_SIZE"),col("HOST"),col("FST_URI"),col("GET_FST_FLAG"),col("GET_FST_FAILED_CODE"),col("GET_FST_DELAY_MSEC"),col("GET_NUM"),col("GET_SUCCEED_NUM"),col("GET_TIMEOUT_NUM"),col("GET_OK_DELAY_TIME"),col("POST_NUM"),col("POST_SUCCEED_NUM"),col("POST_TO_NUM"),col("POST_OK_DELAY_TIME"),col("THROUGHPUT_ALL_PAGE"),col("TAC"),col("ECI"),col("TCP_RTT_STEP1").cast("bigint"),col("TCP_UL_RETRANS_WITHPL"),col("TCP_DW_RETRANS_WITHPL"),col("TCP_UL_PACKAGES_WITHPL"),col("TCP_DW_PACKAGES_WITHPL"),col("RAN_NE_USER_IP"),col("FIRSTDNSDELAY"),col("FIRSTDNSCODE"),col("PAGESIZE"),col("DNSNUM"),col("DNSSUCCNUM"),col("DNSDELAY"),col("PAGE_SR_DELAY"),col("PTMSI"),col("HOMEMCC"),col("HOMEMNC"),col("PREPAID_FLAG"),col("USER_AGENT"),col("DATATRANS_UL_DURATION"),col("DATATRANS_DW_DURATION"),col("MS_WIN_STAT_TOTAL_NUM"),col("MS_WIN_STAT_SMALL_NUM"),col("MS_ACK_TO_1STGET_DELAY"),col("SERVER_ACK_TO_1STDATA_DELAY"),col("AVG_UL_RTT"),col("AVG_DW_RTT"),col("UL_RTT_LONG_NUM"),col("DW_RTT_LONG_NUM"),col("UL_RTT_STAT_NUM"),col("DW_RTT_STAT_NUM"),col("USER_PROBE_UL_LOST_PKT"),col("SERVER_PROBE_UL_LOST_PKT"),col("SERVER_PROBE_DW_LOST_PKT"),col("USER_PROBE_DW_LOST_PKT"),col("TETHERING_FLAG").cast("short"),col("CHARGING_CHARACTERISTICS"),col("DL_SERIOUS_OUT_OF_ORDER_NUM"),col("DL_SLIGHT_OUT_OF_ORDER_NUM"),col("DL_FLIGHT_TOTAL_SIZE"),col("DL_FLIGHT_TOTAL_NUM"),col("DL_MAX_FLIGHT_SIZE"),col("UL_SERIOUS_OUT_OF_ORDER_NUM"),col("UL_SLIGHT_OUT_OF_ORDER_NUM"),col("UL_FLIGHT_TOTAL_SIZE"),col("UL_FLIGHT_TOTAL_NUM"),col("UL_MAX_FLIGHT_SIZE"),col("USER_DL_SLIGHT_OUT_OF_ORDER_PACKETS"),col("SERVER_UL_SLIGHT_OUT_OF_ORDER_PACKETS"),col("DL_CONTINUOUS_RETRANSMISSION_DELAY"),col("USER_HUNGRY_DELAY"),col("SERVER_HUNGRY_DELAY"),col("SV"),col("FIRST_DNS_RETRANS_NUM"),col("SUB_PROT_TYPE").cast("int"),col("APP_ID").cast("int"),col("DNS2SYN"),col("FAIL_CLASS"),col("PAGE_SIZE_CATGORY"),col("AVG_UL_RTT_MICRO_SEC"),col("AVG_DW_RTT_MICRO_SEC"),col("L7_UL_GOODPUT_FULL_MSS"),col("QCI_NEG"),col("TOTAL_TCP_RTT"),col("TRANS_TYPE"),col("CONTENT_TYPE"),col("_corrupt_record"),col("year"),col("month"),col("day"),col("hour"))


// COMMAND ----------

import scala.concurrent.duration._
import org.apache.spark.sql.streaming.StreamingQuery
import org.apache.spark.sql.streaming.Trigger

// Escritura en streaming a Delta Lake
val query = df.writeStream
  .format("delta")
  .outputMode("append") // Solo agrega nuevos datos
  .partitionBy("year", "month", "day", "hour")
  .option("checkpointLocation", s"${path_adls_smartcare}/checkpoints/web") // Ruta de checkpoints
  .option("path", path_salida) // Ruta donde se guardará en ADLS
  .option("maxFilesPerTrigger", 1000)
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
    println(s"📌 Último Checkpoint: ${s"${path_adls_smartcare}/checkpoints/web"}")

  } else {
    println("⚠️ No hay progreso disponible todavía.")
  }

  println("======================================")
  Thread.sleep(10000)  // Consulta cada 10 segundos
}

//query.awaitTermination()
