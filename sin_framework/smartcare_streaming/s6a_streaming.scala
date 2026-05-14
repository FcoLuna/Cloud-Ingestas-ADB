// Databricks notebook source
dbutils.widgets.text("path_adls","abfss://bigdatadev@stbigdatadev02.dfs.core.windows.net")
dbutils.widgets.text("path_adls_smartcare","abfss://smartcare@stbigdataprd02.dfs.core.windows.net")
dbutils.widgets.text("ruta_origen","/data/trafico/trafico_detalle/smartcare/detail_cdr_s6a_test/landing")
dbutils.widgets.text("path_salida","/data/trafico/trafico_detalle/smartcare/detail_cdr_s6a_test/raw")
dbutils.widgets.text("catalogo","bidesarrollo")
dbutils.widgets.text("tabla_salida","raw_trafico.smartcare_s6a")

// COMMAND ----------

val path_adls = dbutils.widgets.get("path_adls")
val path_adls_smartcare = dbutils.widgets.get("path_adls_smartcare")
val catalogo = dbutils.widgets.get("catalogo")
val landing_s6a = path_adls_smartcare + dbutils.widgets.get("ruta_origen")
val tabla_salida = catalogo +"."+ dbutils.widgets.get("tabla_salida")
val path_salida = path_adls + dbutils.widgets.get("path_salida")

// COMMAND ----------

import org.apache.spark.sql.types._

val columnas = Seq("IMSI","MSISDN","IMEI","HSS_SIG_IP","MME_SIG_IP","TRANS_TYPE","TRANS_REQ_TIME_SEC","TRANS_REQ_TIME_MSEC","TRANS_RSP_TIME_SEC","TRANS_RSP_TIME_MSEC","TRANS_SUCCED_FLAG","TRANS_CAUSE_TYPE","TRANS_CAUSE","MCC","MNC","HOMEMCC","HOMEMNC","_corrupt_record")

val schema = StructType(columnas.map(StructField(_, StringType, true)))
val s6a = spark.readStream
  .format("cloudFiles")
  .option("cloudFiles.format", "csv") // Especifica que los archivos son CSV
  .option("header", "true") // Si los archivos tienen encabezado
  .option("delimiter", "|") // Configura el delimitador
  .option("cloudFiles.schemaLocation", s"${path_adls_smartcare}/schemas/s6a") // Ruta para el esquema
  .schema(schema) // Usa el esquema definido
  .load(landing_s6a)
  //.toDF(columnas: _*)

// COMMAND ----------

val df = s6a.
withColumn("year", date_format(from_unixtime(col("TRANS_REQ_TIME_SEC")), "yyyy")).
withColumn("month", date_format(from_unixtime(col("TRANS_REQ_TIME_SEC")), "MM")).
withColumn("day", date_format(from_unixtime(col("TRANS_REQ_TIME_SEC")), "dd")).
withColumn("hour", date_format(from_unixtime(col("TRANS_REQ_TIME_SEC")), "HH")).
select(col("IMSI"),col("MSISDN"),col("IMEI"),col("HSS_SIG_IP"),col("MME_SIG_IP"),col("TRANS_TYPE").cast("int"),col("TRANS_REQ_TIME_SEC").cast("int"),col("TRANS_REQ_TIME_MSEC").cast("int"),col("TRANS_RSP_TIME_SEC").cast("int"),col("TRANS_RSP_TIME_MSEC").cast("int"),col("TRANS_SUCCED_FLAG").cast("short"),col("TRANS_CAUSE_TYPE").cast("int"),col("TRANS_CAUSE").cast("int"),col("MCC"),col("MNC"),col("HOMEMCC"),col("HOMEMNC"),col("_corrupt_record"),col("year"),col("month"),col("day"),col("hour"))

// COMMAND ----------

import scala.concurrent.duration._
import org.apache.spark.sql.streaming.StreamingQuery
import org.apache.spark.sql.streaming.Trigger

// Escritura en streaming a Delta Lake
val query = df.writeStream
  .format("delta")
  .outputMode("append") // Solo agrega nuevos datos
  .partitionBy("year", "month", "day", "hour")
  .option("checkpointLocation", s"${path_adls_smartcare}/checkpoints/s6a") // Ruta de checkpoints
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
    println(s"📌 Último Checkpoint: ${s"${path_adls_smartcare}/checkpoints/s6a"}")

  } else {
    println("⚠️ No hay progreso disponible todavía.")
  }

  println("======================================")
  Thread.sleep(10000)  // Consulta cada 10 segundos
}

//query.awaitTermination()
