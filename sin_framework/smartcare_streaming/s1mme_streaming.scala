// Databricks notebook source
dbutils.widgets.text("path_adls_smartcare","abfss://smartcare@stbigdataprd02.dfs.core.windows.net")
dbutils.widgets.text("path_adls","abfss://bigdatadev@stbigdatadev02.dfs.core.windows.net")
dbutils.widgets.text("ruta_origen","/data/trafico/trafico_detalle/smartcare/detail_cdr_s1mme_test/landing")
dbutils.widgets.text("path_salida","/data/trafico/trafico_detalle/smartcare/detail_cdr_s1mme_test/raw")
dbutils.widgets.text("catalogo","bidesarrollo")
dbutils.widgets.text("tabla_salida","raw_trafico.smartcare_s1mme")

// COMMAND ----------

val path_adls = dbutils.widgets.get("path_adls")
val path_adls_smartcare = dbutils.widgets.get("path_adls_smartcare")
val catalogo = dbutils.widgets.get("catalogo")
val landing_path = path_adls_smartcare + dbutils.widgets.get("ruta_origen")
val tabla_salida = catalogo +"."+ dbutils.widgets.get("tabla_salida")
val path_salida = path_adls + dbutils.widgets.get("path_salida")

// COMMAND ----------

import org.apache.spark.sql.types._

val columnas = Seq("SID","PROTOCOL_ID","PROCEDURE_ID","PROBEID","ENCRYPT_VERSION","INTERFACEID","IMSI","MSISDN","IMEI","MS_IP","MTMSI","ROAMING_TYPE","HOMEMCC","HOMEMNC","HOMEPROID","HOMEAREAID","RAT","MCC","MNC","RAI_TAI","SAI_CGI_ECGI","APN","REFID","PROC_TYPE","PROC_STARTTIME","PROC_STARTTIME_MSEC","PROC_ENDTIME","PROC_ENDTIME_MSEC","PROC_SUCCED_FLAG","FIR_FAIL_TRANS_PROT","FIR_FAIL_TRANS","FIR_FAIL_CAUSE_TYPE","FIR_FAIL_CAUSE","MME_ID","RAN_NE_ID","MME_SIG_IP","RAN_NE_SIG_IP","SGW_SIGIP_LST","PGW_SIGIP_LST","EMM_TRANS_TYPE","EMM_TRANS_SUB_TYPE","EMM_DIERECTION","EMM_REQ_TIME_SEC","EMM_REQ_TIME_MSEC","EMM_END_TIME_SEC","EMM_END_TIME_MSEC","EMM_REQ_SUCCED_FLAG","EMM_REQ_RETRANS_FLAG","EMM_CAUSE","POWEROFF_FLAG","BEARER_ACTIVE_FLAG","ACTIVE_FLAG","OLD_MCC","OLD_MNC","OLD_RAI_TAI","OLD_SAI_CGI_ECGI","IDENTITY_REQ_TIME_SEC","IDENTITY_REQ_TIME_MSEC","IDENTITY_EMD_TIME_SEC","IDENTITY_EMD_TIME_MSEC","IDENTITY_SUCCED_FLAG","IDENTITY_RETRANS_FLAG","IDENTITY_CAUSE","AUTH_REQ_TIME_SEC","AUTH_REQ_TIME_MSEC","AUTH_END_TIME_SEC","AUTH_END_TIME_MSEC","AUTH_SUCCED_FLAG","AUTH_RETRANS_FLAG","AUTH_REJ_EMM_CAUSE","SECURITY_MODE_TIME","SECURITY_MODE_TIME_MSEC","SECURITY_MODE_CMPT_TIME","SECURITY_MODE_CMPT_TIME_MSEC","SECURITY_MODE_SUCCED_FLAG","SECURITY_MODE_RESELECT_FLAG","SECURITY_MODE_CAUSE","ESM_UE_TRANS_TYPE","ESM_UE_REQ_TIME_SEC","ESM_UE_REQ_TIME_MSEC","ESM_UE_END_TIME_SEC","ESM_UE_END_TIME_MSEC","ESM_UE_SUCCED_FLAG","ESM_UE_RETRANS_FLAG","ESM_UE_REJ_CAUSE","ESM_UE_EBI","ESM_UE_LBI","ESM_UE_ESMCAUSE","ESM_NW_TRANS_TYPE","ESM_NW_REQ_TIME_SEC","ESM_NW_REQ_TIME_MSEC","ESM_NW_END_TIME_SEC","ESM_NW_END_TIME_MSEC","ESM_NW_SUCCED_FLAG","ESM_NW_RETRANS_FLAG","ESM_NW_REJ_CAUSE","ESM_NW_EBI","ESM_NW_LBI","ESM_NW_ESMCAUSE","PAGING_REQ_TIME_SEC","PAGING_REQ_TIME_MSEC","PAGING_RSP_TIME_SEC","PAGING_RSP_TIME_MSEC","PAGING_REQ_SUCCED_FLAG","PAGING_REQ_RETRANS_FLAG","PAGING_REQ_TIMEOUT_FLAG","S1AP_TRANS_TYPE","S1AP_REQ_TIME_SEC","S1AP_REQ_TIME_MSEC","S1AP_END_TIME_SEC","S1AP_END_TIME_MSEC","S1AP_SUCCED_FLAG","S1AP_RETRANS_FLAG","S1AP_CAUSE_TYPE","S1AP_CAUSE","ERAB_COUNT_TO_BE_SETUP","ERAB_COUNT_FAILED_SETUP","ERAB_COUNT_SUCCEED_SETUP","RESERVED1","RESERVED2","RESERVED3","RESERVED4","CSFB_IND","S1AP_TRANS_DIRECTION","LAYER1ID","LAYER2ID","LAYER3ID","LAYER4ID","LAYER5ID","LAYER6ID","UE_SRVCC_CAPABILITY","CSFB_RESPONSE","TMSI","OLD_TMSI","LAC","CSCALL_TTIME","CSCALL_TTIME_MS","MBR_DL_NEG","GBR_DL_NEG","MBR_UL_NEG","GBR_UL_NEG","QCI_NEG","ARP_NEG","SV","MMEGI","MMEC","ENB_UE_S1AP_ID","MME_UE_S1AP_ID","IMEI_CIPHERTEXT","PREPAID_FLAG","IMS_VOICE_SUPPORT","BEAR_TFT","BEAR_RADIO_PRIORITY","BEAR_UL_APN_AMBR","BEAR_DL_APN_AMBR","UE_CONTEXT_STATUS","UE_VOICE_PREFERENCE","SESSIONKEY","PDN_TYPE","OLD_GUTI_TYPE","USER_CATEGORY","RLS_REQ_TYPE","RLS_REQ_TIME_SEC","RLS_REQ_TIME_MSEC","RLS_TRANS_TYPE","RLS_TRANS_REQ_TIME_SEC","RLS_TRANS_REQ_TIME_MSEC","RLS_TRANS_RSP_TIME_SEC","RLS_TRANS_RSP_TIME_MSEC","RLS_TRANS_SUCCED_FLAG","RLS_TRANS_CAUSE_TYPE","RLS_TRANS_CAUSE","IMS_SERVICE_TYPE","OLD_RAT","c1")

val schema = StructType(columnas.map(StructField(_, StringType, true)))
// val s1mme = spark.read.option("delimiter","|").csv(landing_s1mme).toDF(columnas: _*)
val s1mme = spark.readStream
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

val df = s1mme.
  withColumn("year", date_format(from_unixtime(col("PROC_STARTTIME")), "yyyy")).
  withColumn("month", date_format(from_unixtime(col("PROC_STARTTIME")), "MM")).
  withColumn("day", date_format(from_unixtime(col("PROC_STARTTIME")), "dd")).
  withColumn("hour", date_format(from_unixtime(col("PROC_STARTTIME")), "HH")).
  select(
    col("APN"),col("FIR_FAIL_CAUSE"),col("FIR_FAIL_CAUSE_TYPE"),col("FIR_FAIL_TRANS"),col("FIR_FAIL_TRANS_PROT"),col("HOMEMCC"),col("HOMEMNC"),col("IMEI"),col("IMSI"),col("MSISDN"),col("PROC_ENDTIME"),col("PROC_STARTTIME"),col("PROC_SUCCED_FLAG"),col("PROC_TYPE"),col("RAI_TAI"),col("RAT"),col("ROAMING_TYPE"),col("SAI_CGI_ECGI"),col("year"),col("month"),col("day"),col("hour")
  )

// COMMAND ----------

import scala.concurrent.duration._
import org.apache.spark.sql.streaming.StreamingQuery
import org.apache.spark.sql.streaming.Trigger

// Escritura en streaming a Delta Lake
val query = df.writeStream
  .format("delta")
  .outputMode("append") // Solo agrega nuevos datos
  .partitionBy("year", "month", "day", "hour")
  .option("checkpointLocation", s"${path_adls_smartcare}/checkpoints/s1mme") // Ruta de checkpoints
  .option("path", path_salida) // Ruta donde se guardará en ADLS
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
    println(s"📌 Último Checkpoint: ${s"${path_adls_smartcare}/checkpoints/s1mme"}")

  } else {
    println("⚠️ No hay progreso disponible todavía.")
  }

  println("======================================")
  Thread.sleep(10000)  // Consulta cada 10 segundos
}

//query.awaitTermination()
