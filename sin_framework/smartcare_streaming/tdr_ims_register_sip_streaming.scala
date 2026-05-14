// Databricks notebook source
/*dbutils.widgets.text("path_adls","abfss://bigdatadev@stbigdatadev02.dfs.core.windows.net")
dbutils.widgets.text("path_adls_smartcare","abfss://smartcare@stbigdataprd02.dfs.core.windows.net")
dbutils.widgets.text("ruta_origen","/data/trafico/trafico_detalle/smartcare/tdr_ims_register_sip_test/landing")
dbutils.widgets.text("path_salida","/data/trafico/trafico_detalle/smartcare/tdr_ims_register_sip/raw")
dbutils.widgets.text("catalogo","bidesarrollo")
dbutils.widgets.text("tabla_salida","raw_trafico.smartcare_tdr_ims_register_sip")*/

// COMMAND ----------

val path_adls = dbutils.widgets.get("path_adls")
val path_adls_smartcare = dbutils.widgets.get("path_adls_smartcare")
val catalogo = dbutils.widgets.get("catalogo")
val landing_path = path_adls_smartcare + dbutils.widgets.get("ruta_origen")
val tabla_salida = catalogo +"."+ dbutils.widgets.get("tabla_salida")
val path_salida = path_adls + dbutils.widgets.get("path_salida")

// COMMAND ----------

import org.apache.spark.sql.types._

val columnas = Seq("REFID","CALLID","ICID","STARTTIME","MILLISEC","ENDTIME","SERVICE_TYPE","SERVICE_STATUS","ACCESS_NE_TYPE","ACCESS_NE_IP","ACCESS_NE_PORT","ACCESS_NE_ID","P_CSCF_IP","P_CSCF_PORT","P_CSCF_ID","I_CSCF_IP","I_CSCF_PORT","I_CSCF_ID","S_CSCF_IP","S_CSCF_PORT","S_CSCF_ID","HSS_IP","HSS_PORT","HSS_ID","AS_IP","AS_PORT","AS_ID","IMPI_TEL_URI","IMPI_SIP_URI","IMPU_TEL_URI1","IMPU_SIP_URI1","IMPU1_TYPE","IMPU_TEL_URI2","IMPU_SIP_URI2","IMPU2_TYPE","IMPU_TEL_URI3","IMPU_SIP_URI3","IMPU3_TYPE","IMPU_TEL_URI4","IMPU_SIP_URI4","IMPU4_TYPE","IMPU_TEL_URI5","IMPU_SIP_URI5","IMPU5_TYPE","FORMAT_IMPU","FORMAT_IMPU_TYPE","IMEI","TERM_UNTRUST_IP_ADDR","TERM_UNTRUST_PORT","TERM_TRUST_IP_ADDR","TERM_TRUST_PORT","DEVICE_TYPE","ACCESS_TYPE","ACCESS_INFO","VISIT_DOMAIN","HOME_DOMAIN","AUTH_TYPE","EXPIRES_TIME_REQ","EXPIRES_TIME_RSP","RESPONSE_CODE","FINISH_WARNING","FINISH_REASON","FIRST_FAIL_NE_TYPE","FIRST_FAIL_NE_IP","FIRFAILPROT","FIRFAILMSG","FIRFAILCAUSE","CAUSELST","PROTOCOLS","AUTH_REQ_TIME","AUTH_RSP_TIME","Layer1ID","Layer2ID","Layer3ID","Layer4ID","Layer5ID","Layer6ID","RESERVED1","RESERVED2","RESERVED3","RESERVED4","RESERVED5","RESERVED6","RESERVED7","RESERVED8","MCC","MNC","HOMEMCC","HOMEMNC","MME_ID","MME_SIG_IP","SGW_ID","SGW_IP","LTE_RAN_NE_ID","LTE_RAN_NE_IP","MME_S1APID","eNodeB_S1APID","PREPAID_FLAG","ROAMING_TYPE","ROAM_DIRECTION","LTE_TRANS_TYPE","IMS_REGISTER_REQ_TIME","FIRST_FAIL_NE_ID","TAI","SRV_ATTR_ID","USER_TYPE","SV","CUSTOMER_TYPE","FINISH_REASON_PROTOCOL","FINISH_REASON_CODE","MV_CAUSE","FIRSTFAILPD","_corrupt_record")

val schema = StructType(columnas.map(StructField(_, StringType, true)))
val df_origen = spark.readStream
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

val df = df_origen.
withColumn("year", date_format(from_unixtime(col("STARTTIME")), "yyyy")).
withColumn("month", date_format(from_unixtime(col("STARTTIME")), "MM")).
withColumn("day", date_format(from_unixtime(col("STARTTIME")), "dd")).
withColumn("hour", date_format(from_unixtime(col("STARTTIME")), "HH")).
select(col("REFID"),col("CALLID"),col("ICID"),col("STARTTIME").cast("int"),col("MILLISEC"),col("ENDTIME").cast("int"),col("SERVICE_TYPE"),col("SERVICE_STATUS"),col("ACCESS_NE_TYPE"),col("ACCESS_NE_IP"),col("ACCESS_NE_PORT"),col("ACCESS_NE_ID"),col("P_CSCF_IP"),col("P_CSCF_PORT"),col("P_CSCF_ID"),col("I_CSCF_IP"),col("I_CSCF_PORT"),col("I_CSCF_ID"),col("S_CSCF_IP"),col("S_CSCF_PORT"),col("S_CSCF_ID"),col("HSS_IP"),col("HSS_PORT"),col("HSS_ID"),col("AS_IP"),col("AS_PORT"),col("AS_ID"),col("IMPI_TEL_URI"),col("IMPI_SIP_URI"),col("IMPU_TEL_URI1"),col("IMPU_SIP_URI1"),col("IMPU1_TYPE"),col("IMPU_TEL_URI2"),col("IMPU_SIP_URI2"),col("IMPU2_TYPE"),col("IMPU_TEL_URI3"),col("IMPU_SIP_URI3"),col("IMPU3_TYPE"),col("IMPU_TEL_URI4"),col("IMPU_SIP_URI4"),col("IMPU4_TYPE"),col("IMPU_TEL_URI5"),col("IMPU_SIP_URI5"),col("IMPU5_TYPE"),col("FORMAT_IMPU"),col("FORMAT_IMPU_TYPE"),col("IMEI"),col("TERM_UNTRUST_IP_ADDR"),col("TERM_UNTRUST_PORT"),col("TERM_TRUST_IP_ADDR"),col("TERM_TRUST_PORT"),col("DEVICE_TYPE"),col("ACCESS_TYPE"),col("ACCESS_INFO"),col("VISIT_DOMAIN"),col("HOME_DOMAIN"),col("AUTH_TYPE"),col("EXPIRES_TIME_REQ"),col("EXPIRES_TIME_RSP"),col("RESPONSE_CODE"),col("FINISH_WARNING"),col("FINISH_REASON"),col("FIRST_FAIL_NE_TYPE"),col("FIRST_FAIL_NE_IP"),col("FIRFAILPROT"),col("FIRFAILMSG"),col("FIRFAILCAUSE"),col("CAUSELST"),col("PROTOCOLS"),col("AUTH_REQ_TIME"),col("AUTH_RSP_TIME"),col("Layer1ID"),col("Layer2ID"),col("Layer3ID"),col("Layer4ID"),col("Layer5ID"),col("Layer6ID"),col("RESERVED1"),col("RESERVED2"),col("RESERVED3"),col("RESERVED4"),col("RESERVED5"),col("RESERVED6"),col("RESERVED7"),col("RESERVED8"),col("MCC"),col("MNC"),col("HOMEMCC"),col("HOMEMNC"),col("MME_ID"),col("MME_SIG_IP"),col("SGW_ID"),col("SGW_IP"),col("LTE_RAN_NE_ID"),col("LTE_RAN_NE_IP"),col("MME_S1APID"),col("eNodeB_S1APID"),col("PREPAID_FLAG"),col("ROAMING_TYPE"),col("ROAM_DIRECTION"),col("LTE_TRANS_TYPE"),col("IMS_REGISTER_REQ_TIME"),col("FIRST_FAIL_NE_ID"),col("TAI"),col("SRV_ATTR_ID"),col("USER_TYPE"),col("SV"),col("CUSTOMER_TYPE"),col("FINISH_REASON_PROTOCOL"),col("FINISH_REASON_CODE"),col("MV_CAUSE"),col("FIRSTFAILPD"),col("year"),col("month"),col("day"),col("hour"))

// COMMAND ----------

import scala.concurrent.duration._
import org.apache.spark.sql.streaming.StreamingQuery
import org.apache.spark.sql.streaming.Trigger

// Escritura en streaming a Delta Lake
val query = df.writeStream
  .format("delta")
  .outputMode("append") // Solo agrega nuevos datos
  .partitionBy("year", "month", "day", "hour")
  .option("checkpointLocation", s"${path_adls_smartcare}/checkpoints/tdr_ims_register_sip") // Ruta de checkpoints
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
    println(s"📌 Último Checkpoint: ${s"${path_adls_smartcare}/checkpoints/tdr_ims_register_sip"}")

  } else {
    println("⚠️ No hay progreso disponible todavía.")
  }

  println("======================================")
  Thread.sleep(10000)  // Consulta cada 10 segundos
}

//query.awaitTermination()
