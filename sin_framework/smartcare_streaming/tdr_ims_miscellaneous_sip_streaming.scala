// Databricks notebook source
/*dbutils.widgets.text("path_adls","abfss://bigdatadev@stbigdatadev02.dfs.core.windows.net")
dbutils.widgets.text("path_adls_smartcare","abfss://smartcare@stbigdataprd02.dfs.core.windows.net")
dbutils.widgets.text("ruta_origen","/data/trafico/trafico_detalle/smartcare/tdr_ims_miscellaneous_sip_test/landing")
dbutils.widgets.text("path_salida","/data/trafico/trafico_detalle/smartcare/tdr_ims_miscellaneous_sip/raw")
dbutils.widgets.text("catalogo","bidesarrollo")
dbutils.widgets.text("tabla_salida","raw_trafico.smartcare_tdr_ims_miscellaneous_sip")*/

// COMMAND ----------

val path_adls = dbutils.widgets.get("path_adls")
val path_adls_smartcare = dbutils.widgets.get("path_adls_smartcare")
val catalogo = dbutils.widgets.get("catalogo")
val landing = path_adls_smartcare + dbutils.widgets.get("ruta_origen")
val tabla_salida = catalogo +"."+ dbutils.widgets.get("tabla_salida")
val path_salida = path_adls + dbutils.widgets.get("path_salida")

// COMMAND ----------

import org.apache.spark.sql.types._

val columnas = Seq("STARTTIME","MILLISEC","ENDTIME","SERVICE_TYPE","SERVICE_STATUS","ACCESS_NE_TYPE","ACCESS_NE_IP","ACCESS_NE_PORT","ACCESS_NE_ID","P_CSCF_IP","P_CSCF_PORT","P_CSCF_ID","S_CSCF_IP","S_CSCF_PORT","S_CSCF_ID","I_CSCF_IP","I_CSCF_PORT","I_CSCF_ID","AS_IP","AS_PORT","AS_ID","HSS_IP","HSS_Port","HSS_ID","CALL_SIDE","IMPI_TEL_URI","IMPI_SIP_URI","IMPU_TEL_URI","IMPU_SIP_URI","IMPU_TYPE","FORMAT_IMPU","FORMAT_IMPU_TYPE","IMEI","TERM_UNTRUST_IP_ADDR","TERM_UNTRUST_PORT","TERM_TRUST_IP_ADDR","TERM_TRUST_PORT","DEVICE_TYPE","ACCESS_TYPE","ACCESS_INFO","VISIT_DOMAIN","HOME_DOMAIN","RESPONSE_CODE","FINISH_WARNING","FINISH_REASON","FIRFAILTIME","FIRST_FAIL_NE_TYPE","FIRST_FAIL_NE_IP","FIRFAILPROT","FIRFAILMSG","FIRFAILCAUSE","CAUSELST","PROTOCOLS","Layer1ID","Layer2ID","Layer3ID","Layer4ID","Layer5ID","Layer6ID","_corrupt_record")

val schema = StructType(columnas.map(StructField(_, StringType, true)))
val df_origen = spark.readStream
  .format("cloudFiles")
  .option("cloudFiles.format", "csv") // Especifica que los archivos son CSV
  .option("header", "true") // Si los archivos tienen encabezado
  .option("delimiter", "|") // Configura el delimitador
  .option("cloudFiles.schemaLocation", s"${path_adls_smartcare}/schemas/tdr_ims_miscellaneous_sip") // Ruta para el esquema
  .schema(schema) // Usa el esquema definido
  .load(landing)
  .toDF(columnas: _*)



// COMMAND ----------

val df = df_origen.
withColumn("year", date_format(from_unixtime(col("STARTTIME")), "yyyy")).
withColumn("month", date_format(from_unixtime(col("STARTTIME")), "MM")).
withColumn("day", date_format(from_unixtime(col("STARTTIME")), "dd")).
withColumn("hour", date_format(from_unixtime(col("STARTTIME")), "HH")).
select(col("STARTTIME").cast("int"),col("MILLISEC"),col("ENDTIME").cast("int"),col("SERVICE_TYPE"),col("SERVICE_STATUS"),col("ACCESS_NE_TYPE"),col("ACCESS_NE_IP"),col("ACCESS_NE_PORT"),col("ACCESS_NE_ID"),col("P_CSCF_IP"),col("P_CSCF_PORT"),col("P_CSCF_ID"),col("S_CSCF_IP"),col("S_CSCF_PORT"),col("S_CSCF_ID"),col("I_CSCF_IP"),col("I_CSCF_PORT"),col("I_CSCF_ID"),col("AS_IP"),col("AS_PORT"),col("AS_ID"),col("HSS_IP"),col("HSS_Port"),col("HSS_ID"),col("CALL_SIDE"),col("IMPI_TEL_URI"),col("IMPI_SIP_URI"),col("IMPU_TEL_URI"),col("IMPU_SIP_URI"),col("IMPU_TYPE"),col("FORMAT_IMPU"),col("FORMAT_IMPU_TYPE"),col("IMEI"),col("TERM_UNTRUST_IP_ADDR"),col("TERM_UNTRUST_PORT"),col("TERM_TRUST_IP_ADDR"),col("TERM_TRUST_PORT"),col("DEVICE_TYPE"),col("ACCESS_TYPE"),col("ACCESS_INFO"),col("VISIT_DOMAIN"),col("HOME_DOMAIN"),col("RESPONSE_CODE"),col("FINISH_WARNING"),col("FINISH_REASON"),col("FIRFAILTIME"),col("FIRST_FAIL_NE_TYPE"),col("FIRST_FAIL_NE_IP"),col("FIRFAILPROT"),col("FIRFAILMSG"),col("FIRFAILCAUSE"),col("CAUSELST"),col("PROTOCOLS"),col("Layer1ID"),col("Layer2ID"),col("Layer3ID"),col("Layer4ID"),col("Layer5ID"),col("Layer6ID"),col("year"),col("month"),col("day"),col("hour"))

// COMMAND ----------

import scala.concurrent.duration._
import org.apache.spark.sql.streaming.StreamingQuery

// Escritura en streaming a Delta Lake
val query = df.writeStream
  .format("delta")
  .outputMode("append") // Solo agrega nuevos datos
  .partitionBy("year", "month", "day", "hour")
  .option("checkpointLocation", s"${path_adls_smartcare}/checkpoints/tdr_ims_miscellaneous_sip") // Ruta de checkpoints
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
    println(s"📌 Último Checkpoint: ${s"${path_adls_smartcare}/checkpoints/tdr_ims_miscellaneous_sip"}")

  } else {
    println("⚠️ No hay progreso disponible todavía.")
  }

  println("======================================")
  Thread.sleep(10000)  // Consulta cada 10 segundos
}

query.awaitTermination()
