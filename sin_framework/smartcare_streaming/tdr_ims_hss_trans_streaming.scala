// Databricks notebook source
/*dbutils.widgets.text("path_adls","abfss://bigdatadev@stbigdatadev02.dfs.core.windows.net")
dbutils.widgets.text("path_adls_smartcare","abfss://smartcare@stbigdataprd02.dfs.core.windows.net")
dbutils.widgets.text("ruta_origen","/data/trafico/trafico_detalle/smartcare/tdr_ims_hss_trans_test/landing")
dbutils.widgets.text("path_salida","/data/trafico/trafico_detalle/smartcare/tdr_ims_hss_trans/raw")
dbutils.widgets.text("catalogo","bidesarrollo")
dbutils.widgets.text("tabla_salida","raw_trafico.smartcare_tdr_ims_hss_trans")*/

// COMMAND ----------

val path_adls = dbutils.widgets.get("path_adls")
val path_adls_smartcare = dbutils.widgets.get("path_adls_smartcare")
val catalogo = dbutils.widgets.get("catalogo")
val landing = path_adls_smartcare + dbutils.widgets.get("ruta_origen")
val tabla_salida = catalogo +"."+ dbutils.widgets.get("tabla_salida")
val path_salida = path_adls + dbutils.widgets.get("path_salida")

// COMMAND ----------

import org.apache.spark.sql.types._

val columnas = Seq("SID","REFID","ProbeID","STARTTIME","MILLISEC","ENDTIME","TRANS_TYPE","TRANS_SUCCED_FLAG","INTERFACE","SOURCE_NE_TYPE","SOURCE_NE_IP","SOURCE_NE_PORT","SOURCR_NE_ID","DEST_NE_TYPE","DEST_NE_IP","DEST_NE_PORT","DEST_NE_ID","IMPI_TEL_URI","IMPI_SIP_URI","IMPU_TEL_URI","IMPU_SIP_URI","IMPU_TYPE","Orig_Host","Orig_Realm","Dest_Host","Dest_Realm","Server_Name","NAI","VISITED_NETWORK_ID","RESULT_CODE","EXPERIMENTAL_RESULT","OCSISERVICEKEY","OCSISCPNO","OCSICAPVER","DCSISERVICEKEY","DCSISCPNO","DCSICAPVER","VTCSISERVICEKEY","VTCSISCPNO","VTCSICAPVER","GENERALODB","HPLMNODB","FIRFAILTIME","RESERVED1","RESERVED2","RESERVED3","RESERVED4","RESERVED5","RESERVED6","RESERVED7","RESERVED8","CTX_GROUP_NO","CTX_SUBGROUP_NO","CTX_PRIVATE_NUMBER","TADS_FLAG","TADS_RAT","TADS_IMSVOICE_FLAG","C_MSISDN","STN_SR","UE_SRVCC_CAPABILITY","REQ_TYPE","SERVICE_TYPE","CSRN","USER_CATEGORY","CS_LOC_INFO","IMEI","SRV_ATTR_ID","FAIL_CATEGORY","RULE_CODE","SUPPLEMENTARY_SERVICE_TYPE","_corrupt_record")

val schema = StructType(columnas.map(StructField(_, StringType, true)))
val df_origen = spark.readStream
  .format("cloudFiles")
  .option("cloudFiles.format", "csv") // Especifica que los archivos son CSV
  .option("header", "true") // Si los archivos tienen encabezado
  .option("delimiter", "|") // Configura el delimitador
  .option("cloudFiles.schemaLocation", s"${path_adls_smartcare}/schemas/tdr_ims_hss_trans") // Ruta para el esquema
  .schema(schema) // Usa el esquema definido
  .load(landing)
  .toDF(columnas: _*)



// COMMAND ----------

val df = df_origen.
withColumn("year", date_format(from_unixtime(col("STARTTIME")), "yyyy")).
withColumn("month", date_format(from_unixtime(col("STARTTIME")), "MM")).
withColumn("day", date_format(from_unixtime(col("STARTTIME")), "dd")).
withColumn("hour", date_format(from_unixtime(col("STARTTIME")), "HH")).
select(col("SID"),col("REFID"),col("ProbeID"),col("STARTTIME").cast("int"),col("MILLISEC"),col("ENDTIME").cast("int"),col("TRANS_TYPE"),col("TRANS_SUCCED_FLAG"),col("INTERFACE"),col("SOURCE_NE_TYPE"),col("SOURCE_NE_IP"),col("SOURCE_NE_PORT"),col("SOURCR_NE_ID"),col("DEST_NE_TYPE"),col("DEST_NE_IP"),col("DEST_NE_PORT"),col("DEST_NE_ID"),col("IMPI_TEL_URI"),col("IMPI_SIP_URI"),col("IMPU_TEL_URI"),col("IMPU_SIP_URI"),col("IMPU_TYPE"),col("Orig_Host"),col("Orig_Realm"),col("Dest_Host"),col("Dest_Realm"),col("Server_Name"),col("NAI"),col("VISITED_NETWORK_ID"),col("RESULT_CODE"),col("EXPERIMENTAL_RESULT"),col("OCSISERVICEKEY"),col("OCSISCPNO"),col("OCSICAPVER"),col("DCSISERVICEKEY"),col("DCSISCPNO"),col("DCSICAPVER"),col("VTCSISERVICEKEY"),col("VTCSISCPNO"),col("VTCSICAPVER"),col("GENERALODB"),col("HPLMNODB"),col("FIRFAILTIME"),col("RESERVED1"),col("RESERVED2"),col("RESERVED3"),col("RESERVED4"),col("RESERVED5"),col("RESERVED6"),col("RESERVED7"),col("RESERVED8"),col("CTX_GROUP_NO"),col("CTX_SUBGROUP_NO"),col("CTX_PRIVATE_NUMBER"),col("TADS_FLAG"),col("TADS_RAT"),col("TADS_IMSVOICE_FLAG"),col("C_MSISDN"),col("STN_SR"),col("UE_SRVCC_CAPABILITY"),col("REQ_TYPE"),col("SERVICE_TYPE"),col("CSRN"),col("USER_CATEGORY"),col("CS_LOC_INFO"),col("IMEI"),col("SRV_ATTR_ID"),col("FAIL_CATEGORY"),col("RULE_CODE"),col("SUPPLEMENTARY_SERVICE_TYPE"),col("year"),col("month"),col("day"),col("hour"))

// COMMAND ----------

import scala.concurrent.duration._
import org.apache.spark.sql.streaming.StreamingQuery

// Escritura en streaming a Delta Lake
val query = df.writeStream
  .format("delta")
  .outputMode("append") // Solo agrega nuevos datos
  .partitionBy("year", "month", "day", "hour")
  .option("checkpointLocation", s"${path_adls_smartcare}/checkpoints/tdr_ims_hss_trans") // Ruta de checkpoints
  .option("path", path_salida)
  .table(tabla_salida) // Guarda directamente en la tabla Delta

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
    println(s"📌 Último Checkpoint: ${s"${path_adls_smartcare}/checkpoints/tdr_ims_hss_trans"}")

  } else {
    println("⚠️ No hay progreso disponible todavía.")
  }

  println("======================================")
  Thread.sleep(10000)  // Consulta cada 10 segundos
}

query.awaitTermination()
