// Databricks notebook source
import org.apache.spark.sql.types.{StructType, StructField, StringType, IntegerType, ShortType, LongType}

// COMMAND ----------


dbutils.widgets.text("path_adls","abfss://bigdatadev@stbigdatadev02.dfs.core.windows.net")
dbutils.widgets.text("path_adls_smartcare","abfss://smartcare@stbigdataprd02.dfs.core.windows.net")
dbutils.widgets.text("ruta_origen","/data/trafico/trafico_detalle/smartcare/cdr_volte_voice_quality_test/landing")
dbutils.widgets.text("path_salida","/data/trafico/trafico_detalle/smartcare/cdr_volte_voice_quality_test/raw/")
dbutils.widgets.text("catalogo","bidesarrollo")
dbutils.widgets.text("tabla_salida","raw_trafico.smartcare_cdr_volte_voice_quality")


// COMMAND ----------

val path_adls = dbutils.widgets.get("path_adls")
val path_adls_smartcare = dbutils.widgets.get("path_adls_smartcare")
val catalogo = dbutils.widgets.get("catalogo")
val landing_path = path_adls_smartcare + dbutils.widgets.get("ruta_origen")
val tabla_salida = catalogo +"."+ dbutils.widgets.get("tabla_salida")
val path_salida = path_adls + dbutils.widgets.get("path_salida")

// COMMAND ----------

val columnas = Seq("STARTTIME","STARTTIME_MS","ENDTIME","ENDTIME_MS","MSISDN","IMSI","IMEI","EGCI","LAST_EGCI","CALL_IDENTIFY","INTERFACE","QCI","CALLER_RAT_TYPE","CALLED_RAT_TYPE","CALLER_NO","CALLED_NO","SOURCE_IP","DESTINATION_IP","SOURCE_PORT","DESTINATION_PORT","CALL_DURATION","POST_DIAL_DELAY","CONN_LATENCY","FST_ENB_UE_S1AP_ID","FST_MME_UE_S1AP_ID","LAST_ENB_UE_S1AP_ID","LAST_MME_UE_S1AP_ID","RTP_PAYLOAD_TYPE","ULCODECRATE","DWCODECRATE","SSRC_CHANGE_TIMES","UL_MOS_AVG","UL_RTCP_DELAY_JITTER_DEDUCTION","RTCP_UL_DELAY_AVG","RTCP_UL_JITTER_AVG","RTCP_UL_PACKET_NUM","RTCP_UL_LOSSPACKER_NUM","DL_MOS_AVG","DW_RTCP_DELAY_JITTER_DEDUCTION","RTCP_DL_DELAY_AVG","RTCP_DL_JITTER_AVG","RTCP_DL_PACKET_NUM","RTCP_DL_LOSSPACKET_NUM","UL_MOS_BAD_NUM","RTCP_UL_MEAS_NUM","DL_MOS_BAD_NUM","RTCP_DL_PARA_MEAS_NUM","ONE_WAY_IDENTIFY","UL_IPMOS_AVG","UL_RTP_DELAY_JITTER_DEDUCTION","RTP_UL_DELAY_AVG","RTP_UL_JITTER_AVG","RTP_UL_PACKET_NUM","RTP_UL_LOSSPACKET_NUM","DL_IPMOS_AVG","DW_RTP_DELAY_JITTER_DEDUCTION","RTP_DL_DELAY_AVG","RTP_DL_JITTER_AVG","RTP_DL_PACKET_NUM","RTP_DL_LOSSPACKET_NUM","UL_IPMOS_BAD_NUM","RTP_UL_PARA_MEAS_NUM","DL_IPMOS_BAD_NUM","RTP_DL_PARA_MEAS_NUM","FIRST_LTE_RAN_NE_ID","FIRST_LTE_RAN_NE_IP","LAST_LTE_RAN_NE_ID","LAST_LTE_RAN_NE_IP","MMEGI","MMEC","SGW_ID","SGW_IP","SBC_ID","LAYER1ID","LAYER2ID","LAYER3ID","LAYER4ID","LAYER5ID","LAYER6ID","UL_RTP_ENDTIME_MS","DL_RTP_ENDTIME_MS","SIG_STARTTIME","UL_CMRREQ_FREQ_NUM","DL_CMRREQ_FREQ_NUM","HO_OUT_REQ","HO_OUT_SUCC","FAIL_CLASS","PGW_ID","PGW_IP","MGW_ID","MGW_IP","SRVCC_FLAG","PEER_CALL_TYPE","RTP_UL_PACKET_NUM_LAST","RTP_UL_LOSSPACKT_NUM_LAST","RTP_DL_PACKET_NUM_LAST","RTP_DL_LOSSPACKET_NUM_LAST","PT_CHANGE_FLAG","SERVICE_TYPE","UL_AVG_BITRATE","DL_AVG_BITRATE","UL_RESOLUTION","DL_RESOLUTION","UL_MOS_TOTAL","DL_MOS_TOTAL","UL_VOICE_FRAME_NUM","RTP_UL_VOICE_FRAME_LOST_NUM","RTCP_UL_VOICE_FRAME_LOST_NUM","DL_VOICE_FRAME_NUM","RTP_DL_VOICE_FRAME_LOST_NUM","RTCP_DL_VOICE_FRAME_LOST_NUM","UL_VIDEO_FRAME_RATE","DL_VIDEO_FRAME_RATE","VIDEO_FALLBACK","PROFILE_LEVEL_ID_UL","PROFILE_LEVEL_ID_DL","USER_TYPE","SV","CALL_HOLD","_corrupt_record")

val schema = StructType(columnas.map(StructField(_, StringType, true)))
val cdr_volte_voice_quality = spark.readStream
  .format("cloudFiles")
  .option("cloudFiles.format", "csv") // Especifica que los archivos son CSV
  .option("header", "true") // Si los archivos tienen encabezado
  .option("delimiter", "|") // Configura el delimitador
  .option("fileNameOnly","true")
  .option("maxFilesPerTrigger", 1000)
  .option("lastFirst","true")
  .schema(schema) // Usa el esquema definido
  .load(landing_path)

// val cdr_volte_voice_quality = spark.read.option("delimiter","|").csv(landing_cdr_volte_voice_quality).toDF(columnas: _*)

// COMMAND ----------

val df = cdr_volte_voice_quality.
withColumn("year", date_format(from_unixtime(col("STARTTIME")), "yyyy")).
withColumn("month", date_format(from_unixtime(col("STARTTIME")), "MM")).
withColumn("day", date_format(from_unixtime(col("STARTTIME")), "dd")).
withColumn("hour", date_format(from_unixtime(col("STARTTIME")), "HH")).
select(col("STARTTIME"),col("STARTTIME_MS"),col("ENDTIME"),col("ENDTIME_MS"),col("MSISDN"),col("IMSI"),col("IMEI"),col("EGCI"),col("LAST_EGCI"),col("CALL_IDENTIFY"),col("INTERFACE"),col("QCI"),col("CALLER_RAT_TYPE"),col("CALLED_RAT_TYPE"),col("CALLER_NO"),col("CALLED_NO"),col("SOURCE_IP"),col("DESTINATION_IP"),col("SOURCE_PORT"),col("DESTINATION_PORT"),col("CALL_DURATION"),col("POST_DIAL_DELAY"),col("CONN_LATENCY"),col("FST_ENB_UE_S1AP_ID"),col("FST_MME_UE_S1AP_ID"),col("LAST_ENB_UE_S1AP_ID"),col("LAST_MME_UE_S1AP_ID"),col("RTP_PAYLOAD_TYPE"),col("ULCODECRATE"),col("DWCODECRATE"),col("SSRC_CHANGE_TIMES"),col("UL_MOS_AVG"),col("UL_RTCP_DELAY_JITTER_DEDUCTION"),col("RTCP_UL_DELAY_AVG"),col("RTCP_UL_JITTER_AVG"),col("RTCP_UL_PACKET_NUM"),col("RTCP_UL_LOSSPACKER_NUM"),col("DL_MOS_AVG"),col("DW_RTCP_DELAY_JITTER_DEDUCTION"),col("RTCP_DL_DELAY_AVG"),col("RTCP_DL_JITTER_AVG"),col("RTCP_DL_PACKET_NUM"),col("RTCP_DL_LOSSPACKET_NUM"),col("UL_MOS_BAD_NUM"),col("RTCP_UL_MEAS_NUM"),col("DL_MOS_BAD_NUM"),col("RTCP_DL_PARA_MEAS_NUM"),col("ONE_WAY_IDENTIFY"),col("UL_IPMOS_AVG"),col("UL_RTP_DELAY_JITTER_DEDUCTION"),col("RTP_UL_DELAY_AVG"),col("RTP_UL_JITTER_AVG"),col("RTP_UL_PACKET_NUM"),col("RTP_UL_LOSSPACKET_NUM"),col("DL_IPMOS_AVG"),col("DW_RTP_DELAY_JITTER_DEDUCTION"),col("RTP_DL_DELAY_AVG"),col("RTP_DL_JITTER_AVG"),col("RTP_DL_PACKET_NUM"),col("RTP_DL_LOSSPACKET_NUM"),col("UL_IPMOS_BAD_NUM"),col("RTP_UL_PARA_MEAS_NUM"),col("DL_IPMOS_BAD_NUM"),col("RTP_DL_PARA_MEAS_NUM"),col("FIRST_LTE_RAN_NE_ID"),col("FIRST_LTE_RAN_NE_IP"),col("LAST_LTE_RAN_NE_ID"),col("LAST_LTE_RAN_NE_IP"),col("MMEGI"),col("MMEC"),col("SGW_ID"),col("SGW_IP"),col("SBC_ID"),col("LAYER1ID"),col("LAYER2ID"),col("LAYER3ID"),col("LAYER4ID"),col("LAYER5ID"),col("LAYER6ID"),col("UL_RTP_ENDTIME_MS"),col("DL_RTP_ENDTIME_MS"),col("SIG_STARTTIME"),col("UL_CMRREQ_FREQ_NUM"),col("DL_CMRREQ_FREQ_NUM"),col("HO_OUT_REQ"),col("HO_OUT_SUCC"),col("FAIL_CLASS"),col("PGW_ID"),col("PGW_IP"),col("MGW_ID"),col("MGW_IP"),col("SRVCC_FLAG"),col("PEER_CALL_TYPE"),col("RTP_UL_PACKET_NUM_LAST"),col("RTP_UL_LOSSPACKT_NUM_LAST"),col("RTP_DL_PACKET_NUM_LAST"),col("RTP_DL_LOSSPACKET_NUM_LAST"),col("PT_CHANGE_FLAG"),col("SERVICE_TYPE"),col("UL_AVG_BITRATE"),col("DL_AVG_BITRATE"),col("UL_RESOLUTION"),col("DL_RESOLUTION"),col("UL_MOS_TOTAL"),col("DL_MOS_TOTAL"),col("UL_VOICE_FRAME_NUM"),col("RTP_UL_VOICE_FRAME_LOST_NUM"),col("RTCP_UL_VOICE_FRAME_LOST_NUM"),col("DL_VOICE_FRAME_NUM"),col("RTP_DL_VOICE_FRAME_LOST_NUM"),col("RTCP_DL_VOICE_FRAME_LOST_NUM"),col("UL_VIDEO_FRAME_RATE"),col("DL_VIDEO_FRAME_RATE"),col("VIDEO_FALLBACK"),col("PROFILE_LEVEL_ID_UL"),col("PROFILE_LEVEL_ID_DL"),col("USER_TYPE"),col("SV"),col("CALL_HOLD"),col("_corrupt_record"),col("year"),col("month"),col("day"),col("hour"))

// COMMAND ----------

import scala.concurrent.duration._
import org.apache.spark.sql.streaming.StreamingQuery
import org.apache.spark.sql.streaming.Trigger

// Escritura en streaming a Delta Lake
val query = df.writeStream
  .format("delta")
  .outputMode("append") // Solo agrega nuevos datos
  .partitionBy("year", "month", "day", "hour")
  .option("checkpointLocation", s"${path_adls_smartcare}/checkpoints/cdr_volte_voice_quality") // Ruta de checkpoints
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
    println(s"📌 Último Checkpoint: ${s"${path_adls_smartcare}/checkpoints/cdr_volte_voice_quality"}")

  } else {
    println("⚠️ No hay progreso disponible todavía.")
  }

  println("======================================")
  Thread.sleep(10000)  // Consulta cada 10 segundos
}

//query.awaitTermination()
