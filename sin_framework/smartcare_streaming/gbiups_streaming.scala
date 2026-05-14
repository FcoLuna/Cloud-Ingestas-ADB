// Databricks notebook source
dbutils.widgets.text("path_adls","abfss://bigdatadev@stbigdatadev02.dfs.core.windows.net")
dbutils.widgets.text("path_adls_smartcare","abfss://smartcare@stbigdataprd02.dfs.core.windows.net/")
dbutils.widgets.text("catalogo","bidesarrollo")
dbutils.widgets.text("tabla_salida","raw_trafico.smartcare_gbiups")
dbutils.widgets.text("ruta_origen","/data/trafico/trafico_detalle/smartcare/detail_cdr_gbiups_test/landing")
dbutils.widgets.text("path_salida","/data/trafico/trafico_detalle/smartcare/detail_cdr_gbiups_test/raw")

// COMMAND ----------

val path_adls = dbutils.widgets.get("path_adls")
val path_adls_smartcare = dbutils.widgets.get("path_adls_smartcare")
val catalogo = dbutils.widgets.get("catalogo")
val landing_path = path_adls_smartcare + dbutils.widgets.get("ruta_origen")
val tabla_salida = catalogo +"."+ dbutils.widgets.get("tabla_salida")
val path_salida = path_adls + dbutils.widgets.get("path_salida")

// COMMAND ----------

val columnas = Seq("SID","PROTOCOL_ID","PROCEDURE_ID","PROBEID","ENCRYPT_VERSION","INTERFACEID","IMSI","MSISDN","IMEI","MS_IP","PTMSI","ROAMING_TYPE","HOMEMCC","HOMEMNC","HOMEPROID","HOMEAREAID","RAT","MCC","MNC","LAI","RAI_TAI","SAI_CGI_ECGI","APN","REFID","PROC_TYPE","PROC_STARTTIME","PROC_STARTTIME_MSEC","PROC_ENDTIME","PROC_ENDTIME_MSEC","PROC_SUCCED_FLAG","PROC_FIRFAIL_PROT","PROC_FIRFAIL_TRANS","PROC_FIRFAIL_CAUSE_TYPE","PROC_FIRFAIL_CAUSE","SGSN_ID","RAN_NE_ID","SGSN_SIG_IP","RAN_NE_SIG_IP","SGSN_SIGIP_LST","GGSN_SIGIP_LST","NETWORK_REQ_PDP_TYPE","REQ_PDPACT_TIME_SEC","REQ_PDPACT_TIME_MSEC","REQ_PDPACT_SUCCED_FLAG","REQ_PDPACT_TIMEOUT_FLAG","PAGING_REQ_TIME_SEC","PAGING_REQ_TIME_MSEC","PAGING_RSP_TIME_SEC","PAGING_RSP_TIME_MSEC","PAGING_PS_SUCCED_FLAG","PAGING_PS_RESELECT_FLAG","PAGING_PS_TIMEOUT_FLAG","MM_TRAN_TYPE","MM_TRANS_SUB_TYPE","MM_TRANS_DIRECTION","MM_TRANS_REQ_TIME_SEC","MM_TRANS_REQ_TIME_MSEC","MM_TRANS_RSP_TIME_SEC","MM_TRANS_RSP_TIME_MSEC","MM_TRANS_SUCCED_FLAG","MM_TRANS_RESELECT_FLAG","MM_TRANS_CAUSE","POWEROFF_FLAG","RAU_WITH_PDP_FLAG","RAU_CATEGORY","OLD_MCC","OLD_MNC","OLD_LAI","OLD_RAI_TAI","OLD_SAI_CGI_ECGI","IDENTITY_REQ_TIME","IDENTITY_REQ_TIME_MSEC","IDENTITY_RSP_TIME","IDENTITY_RSP_TIME_MSEC","IDENTITY_CAUSE","IDENTITY_SUCCED_FLAG","IDENTITY_RESELECT_FLAG","AUTH_CIPHER_REQ_TIME_SEC","AUTH_CIPHER_REQ_TIME_MSEC","AUTH_CIPHER_RSP_TIME_SEC","AUTH_CIPHER_RSP_TIME_MSEC","AUTH_CIPHER_SUCCEED_FLAG","AUTH_CIPHER_RESELECT_FLAG","AUTH_CIPHER_CAUSE","SECURITY_MODE_TIME","SECURITY_MODE_TIME_MSEC","SECURITY_MODE_CMPT_TIME","SECURITY_MODE_CMPT_TIME_MSEC","SECURITY_MODE_SUCCED_FLAG","SECURITY_MODE_RESELECT_FLAG","SECURITY_MODE_CAUSE","PDP_TRANS_TYPE","SECONDARY_PDP_FLAG","PDP_REQ_TIME","PDP_REQ_TIME_MSEC","PDP_RSP_TIME","PDP_RSP_TIME_MSEC","PDP_SUCCEED_FLAG","PDP_FAIL_CAUSE","PDPADDRESS_NULL_FLAG","PDP_TRANS_DIRECTION","ASSN_TIME","ASSN_TIME_MSEC","ASSN_CMPT_TIME","ASSN_CMPT_TIME_MSEC","ASSN_SUCCED_FLAG","ASSN_CAUSE","RADIO_STATUS_TIME","RADIO_STATUS_TIME_MS","RADIO_STATUS_CAUSE","LLC_DISCARD_TIME","LLC_DISCARD_TIME_MS","LLC_FRAME_NUM","LLC_FRAME_OCT","DISCARD_FRAME_NUM","DISCARD_FRAME_OCT","RESERVED1","RESERVED2","RESERVED3","RESERVED4","HO_REFER_NUMBER","LAYER1ID","LAYER2ID","LAYER3ID","LAYER4ID","LAYER5ID","LAYER6ID","RAB_TYPE","MBR_DL_NEG","GBR_DL_NEG","MBR_UL_NEG","GBR_UL_NEG","SV","IMEI_CIPHERTEXT","PREPAID_FLAG","PTMSI_SIGNATURE","NRI","RANAP_TRANS_TYPE","RANAP_TRANS_REQ_TIME_SEC","RANAP_TRANS_REQ_TIME_MSEC","RANAP_TRANS_RSP_TIME_SEC","RANAP_TRANS_RSP_TIME_MSEC","RANAP_TRANS_SUCCED_FLAG","RANAP_TRANS_CAUSE","SESSIONKEY","PDP_TYPE","OLD_PTMSI_TYPE","USER_CATEGORY","OLD_RAT","c1")


// COMMAND ----------

import org.apache.spark.sql.types._

val schema = StructType(columnas.map(StructField(_, StringType, true)))

// Configurar las opciones de Autoloader para leer archivos incrementales
var gbiups = spark.readStream
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

val df = gbiups.
withColumn("year", date_format(from_unixtime(col("PROC_STARTTIME")), "yyyy")).
withColumn("month", date_format(from_unixtime(col("PROC_STARTTIME")), "MM")).
withColumn("day", date_format(from_unixtime(col("PROC_STARTTIME")), "dd")).
withColumn("hour", date_format(from_unixtime(col("PROC_STARTTIME")), "HH")).
select(
  col("APN"),
col("PROC_FIRFAIL_CAUSE"),
col("PROC_FIRFAIL_CAUSE_TYPE"),
col("PROC_FIRFAIL_TRANS"),
col("PROC_FIRFAIL_PROT"),
col("HOMEMCC"),
col("HOMEMNC"),
col("IMEI"),
col("IMSI"),
col("INTERFACEID"),
col("MCC"),
col("MNC"),
col("MSISDN"),
col("PROC_ENDTIME"),
col("PROC_STARTTIME"),
col("PROC_SUCCED_FLAG"),
col("PROC_TYPE"),
col("RAI_TAI"),
col("RAT"),
col("ROAMING_TYPE"),
col("SAI_CGI_ECGI"),col("year"),col("month"),col("day"),col("hour")
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
  .option("checkpointLocation", s"${path_adls_smartcare}/checkpoints/gbiups") // Ruta de checkpoints
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
    println(s"📌 Último Checkpoint: ${s"${path_adls_smartcare}/checkpoints/gbiups"}")

  } else {
    println("⚠️ No hay progreso disponible todavía.")
  }

  println("======================================")
  Thread.sleep(10000)  // Consulta cada 10 segundos
}

//query.awaitTermination()
