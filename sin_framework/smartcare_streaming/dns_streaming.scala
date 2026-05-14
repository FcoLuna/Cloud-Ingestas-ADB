// Databricks notebook source
val path_adls = dbutils.widgets.get("path_adls")
val path_adls_smartcare = dbutils.widgets.get("path_adls_smartcare")
val catalogo = dbutils.widgets.get("catalogo")
val path_landing = path_adls_smartcare + dbutils.widgets.get("ruta_origen")
val tabla_salida = catalogo +"."+ dbutils.widgets.get("tabla_salida")
val path_salida = path_adls + dbutils.widgets.get("path_salida")

// COMMAND ----------

import org.apache.spark.sql.types._

val columnas = Seq("SID","INTERFACEID","BEGIN_TIME","BEGIN_TIME_MSEL","END_TIME","END_TIME_MSEL","PROT_CATEGORY","PROT_TYPE","MSISDN","IMSI","IMEI","ROAM_DIRECTION","MCC","MNC","RAT","LAC","RAC","SAC","CI","MS_IP","SERVER_IP","MS_PORT","SERVER_PORT","APN","SGSN_USER_IP","GGSN_USER_IP","L4_TYPE","L4_UL_THROUGHPUT","L4_DW_THROUGHPUT","L4_UL_GOODPUT","L4_DW_GOODPUT","L4_UL_PACKETS","L4_DW_PACKETS","TCP_CONN_STATES","TCP_RTT","TCP_UL_OUTOFSEQU","TCP_DW_OUTOFSEQU","TCP_UL_RETRANS","TCP_DW_RETRANS","TCP_WIN_SIZE","DNS_TRANS_NUM","DNS_SUCCEED_NUM","DNS_OK_DELAY_TIME","DNS_SOURCE_TYPE","DNS_DOMAIN","DNS_DOMAIN_IP","FST_TRANS_FLAG","ERROR_CODE_FST","TAC","ECI","TCP_RTT_STEP1","TCP_UL_RETRANS_WITHPL","TCP_DW_RETRANS_WITHPL","TCP_UL_PACKAGES_WITHPL","TCP_DW_PACKAGES_WITHPL","RAN_NE_USER_IP","HOMEMCC","HOMEMNC","PREPAID_FLAG","TETHERING_FLAG","CHARGING_CHARACTERISTICS","SV","SUB_PROT_TYPE","APP_ID","TRAFFIC_CLASS_NEG","THP_NEG","QCI_NEG","TOTAL_TCP_RTT","TOTAL_TCP_RTT_STEP1","_corrupt_record")
val schema = StructType(columnas.map(StructField(_, StringType, true)))
// val stream = spark.read.option("delimiter","|").csv(landing_stream).toDF(columnas: _*)
val dns = spark.readStream
  .format("cloudFiles")
  .option("cloudFiles.format", "csv") // Especifica que los archivos son CSV
  .option("header", "true") // Si los archivos tienen encabezado
  .option("delimiter", "|") // Configura el delimitador
  .option("cloudFiles.schemaLocation", s"${path_adls_smartcare}/schemas/dns") // Ruta para el esquema
  .schema(schema) // Usa el esquema definido
  .load(path_landing)
  //.toDF(columnas: _*)

// COMMAND ----------

val df = dns.
withColumn("year", date_format(from_unixtime(col("BEGIN_TIME")), "yyyy")).
withColumn("month", date_format(from_unixtime(col("BEGIN_TIME")), "MM")).
withColumn("day", date_format(from_unixtime(col("BEGIN_TIME")), "dd")).
withColumn("hour", date_format(from_unixtime(col("BEGIN_TIME")), "HH")).
select(col("SID"),col("INTERFACEID").cast("short"),col("BEGIN_TIME").cast("int"),col("BEGIN_TIME_MSEL"),col("END_TIME").cast("int"),col("END_TIME_MSEL"),col("PROT_CATEGORY").cast("int"),col("PROT_TYPE").cast("int"),col("MSISDN"),col("IMSI"),col("IMEI"),col("ROAM_DIRECTION"),col("MCC"),col("MNC"),col("RAT").cast("short"),col("LAC"),col("RAC"),col("SAC"),col("CI"),col("MS_IP"),col("SERVER_IP"),col("MS_PORT"),col("SERVER_PORT").cast("int"),col("APN"),col("SGSN_USER_IP"),col("GGSN_USER_IP"),col("L4_TYPE"),col("L4_UL_THROUGHPUT").cast("bigint"),col("L4_DW_THROUGHPUT").cast("bigint"),col("L4_UL_GOODPUT"),col("L4_DW_GOODPUT"),col("L4_UL_PACKETS"),col("L4_DW_PACKETS"),col("TCP_CONN_STATES"),col("TCP_RTT").cast("bigint"),col("TCP_UL_OUTOFSEQU"),col("TCP_DW_OUTOFSEQU"),col("TCP_UL_RETRANS"),col("TCP_DW_RETRANS"),col("TCP_WIN_SIZE"),col("DNS_TRANS_NUM"),col("DNS_SUCCEED_NUM"),col("DNS_OK_DELAY_TIME"),col("DNS_SOURCE_TYPE"),col("DNS_DOMAIN"),col("DNS_DOMAIN_IP"),col("FST_TRANS_FLAG"),col("ERROR_CODE_FST"),col("TAC"),col("ECI"),col("TCP_RTT_STEP1").cast("bigint"),col("TCP_UL_RETRANS_WITHPL"),col("TCP_DW_RETRANS_WITHPL"),col("TCP_UL_PACKAGES_WITHPL"),col("TCP_DW_PACKAGES_WITHPL"),col("RAN_NE_USER_IP"),col("HOMEMCC"),col("HOMEMNC"),col("PREPAID_FLAG"),col("TETHERING_FLAG").cast("short"),col("CHARGING_CHARACTERISTICS"),col("SV"),col("SUB_PROT_TYPE").cast("int"),col("APP_ID").cast("int"),col("TRAFFIC_CLASS_NEG"),col("THP_NEG"),col("QCI_NEG"),col("TOTAL_TCP_RTT"),col("TOTAL_TCP_RTT_STEP1"),col("_corrupt_record"),col("year"),col("month"),col("day"),col("hour"))


// COMMAND ----------

import scala.concurrent.duration._
import org.apache.spark.sql.streaming.StreamingQuery
import org.apache.spark.sql.streaming.Trigger

// Escritura en streaming a Delta Lake
val query = df.writeStream
  .format("delta")
  .outputMode("append") // Solo agrega nuevos datos
  .partitionBy("year", "month", "day", "hour")
  .option("checkpointLocation", s"${path_adls_smartcare}/checkpoints/dns") // Ruta de checkpoints
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
    println(s"📌 Último Checkpoint: ${s"${path_adls_smartcare}/checkpoints/dns"}")

  } else {
    println("⚠️ No hay progreso disponible todavía.")
  }

  println("======================================")
  Thread.sleep(10000)  // Consulta cada 10 segundos
}

//query.awaitTermination()
