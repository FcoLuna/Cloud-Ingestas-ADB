// Databricks notebook source
/*dbutils.widgets.text("path_adls","abfss://bigdatadev@stbigdatadev02.dfs.core.windows.net")
dbutils.widgets.text("path_adls_smartcare","abfss://smartcare@stbigdataprd02.dfs.core.windows.net")
dbutils.widgets.text("ruta_origen","/data/trafico/trafico_detalle/smartcare/tdr_map_sms/landing")
dbutils.widgets.text("path_salida","/data/trafico/trafico_detalle/smartcare/tdr_map_sms/raw")
dbutils.widgets.text("catalogo","bidesarrollo")
dbutils.widgets.text("tabla_salida","raw_trafico.smartcare_tdr_map_sms")*/

// COMMAND ----------

val path_adls = dbutils.widgets.get("path_adls")
val path_adls_smartcare = dbutils.widgets.get("path_adls_smartcare")
val catalogo = dbutils.widgets.get("catalogo")
val landing_path = path_adls_smartcare + dbutils.widgets.get("ruta_origen")
val tabla_salida = catalogo +"."+ dbutils.widgets.get("tabla_salida")
val path_salida = path_adls + dbutils.widgets.get("path_salida")

// COMMAND ----------

import org.apache.spark.sql.types._

val columnas = Seq("TDRID","REFID","STARTTIME","MILLISEC","SRVSTAT","CDRSTAT","NI","OPC","OSSN","OGT","DPC","DSSN","DGT","LINKSETID","SRV_TYPE","IMSI","TMSI","MSISDN","MSCNO","SMSCNO","PMSISDN","SMS_LEN","END_TIME","RELTYPE","CAUSE","ENCRYPT_VERSION","RESERVED1","RESERVED2","RESERVED3","RESERVED4","RESERVED5","RESERVED6","RESERVED7","RESERVED8","FILELOCATION","OFFSET_DSI","PROBEID","GROUPID","ROAM_DIRECTION","HOMEMCC","HOMEMNC","VISITCC","VISITNDC","SIG_COLLECTION_TYPE","LNKOPC","LNKDPC","LNKSRCIP","LNKSRCPORT","LNKDESTIP","LNKDESTPORT","VASSERVICETYPE","SEGREF","SEGTOTALNUM","SEGSEQ","TPMTI","PD","OPPTYPE","FIRFAILTIME","PREPAID_FLAG","LAYER1ID","LAYER2ID","LAYER3ID","LAYER4ID","LAYER5ID","LAYER6ID","SMSC_TIMESTAMP","RSP_LNK1_TYPE","RSP_LNK1_SIG_TYPE","RSP_LNK1_OPC","RSP_LNK1_DPC","RSP_LNK1_SLC","RSP_LNK1_SRCIP","RSP_LNK1_SRCPORT","RSP_LNK1_DESTIP","RSP_LNK1_DESTPORT","RSP_LNK2_TYPE","RSP_LNK2_SIG_TYPE","RSP_LNK2_OPC","RSP_LNK2_DPC","RSP_LNK2_SLC","RSP_LNK2_SRCIP","RSP_LNK2_SRCPORT","RSP_LNK2_DESTIP","RSP_LNK2_DESTPORT","RSP_LNK3_TYPE","RSP_LNK3_SIG_TYPE","RSP_LNK3_OPC","RSP_LNK3_DPC","RSP_LNK3_SLC","RSP_LNK3_SRCIP","RSP_LNK3_SRCPORT","RSP_LNK3_DESTIP","RSP_LNK3_DESTPORT","RSP_LNK4_TYPE","RSP_LNK4_SIG_TYPE","RSP_LNK4_OPC","RSP_LNK4_DPC","RSP_LNK4_SLC","RSP_LNK4_SRCIP","RSP_LNK4_SRCPORT","RSP_LNK4_DESTIP","RSP_LNK4_DESTPORT","USER_CATEGORY","BSS_IND","IMEI","CUSTOMER_TYPE","SRV_ATTR_ID","FAIL_CATEGORY","RULE_CODE","SV","CALC_TIME","c1","c2","_corrupt_record")

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
select(col("TDRID"),col("REFID"),col("STARTTIME").cast("int"),col("MILLISEC"),col("SRVSTAT"),col("CDRSTAT"),col("NI"),col("OPC"),col("OSSN"),col("OGT"),col("DPC"),col("DSSN"),col("DGT"),col("LINKSETID"),col("SRV_TYPE"),col("IMSI"),col("TMSI"),col("MSISDN"),col("MSCNO"),col("SMSCNO"),col("PMSISDN"),col("SMS_LEN"),col("END_TIME").cast("int"),col("RELTYPE"),col("CAUSE"),col("ENCRYPT_VERSION"),col("RESERVED1"),col("RESERVED2"),col("RESERVED3"),col("RESERVED4"),col("RESERVED5"),col("RESERVED6"),col("RESERVED7"),col("RESERVED8"),col("FILELOCATION"),col("OFFSET_DSI"),col("PROBEID"),col("GROUPID"),col("ROAM_DIRECTION"),col("HOMEMCC"),col("HOMEMNC"),col("VISITCC"),col("VISITNDC"),col("SIG_COLLECTION_TYPE"),col("LNKOPC"),col("LNKDPC"),col("LNKSRCIP"),col("LNKSRCPORT"),col("LNKDESTIP"),col("LNKDESTPORT"),col("VASSERVICETYPE"),col("SEGREF"),col("SEGTOTALNUM"),col("SEGSEQ"),col("TPMTI"),col("PD"),col("OPPTYPE"),col("FIRFAILTIME"),col("PREPAID_FLAG"),col("LAYER1ID"),col("LAYER2ID"),col("LAYER3ID"),col("LAYER4ID"),col("LAYER5ID"),col("LAYER6ID"),col("SMSC_TIMESTAMP"),col("RSP_LNK1_TYPE"),col("RSP_LNK1_SIG_TYPE"),col("RSP_LNK1_OPC"),col("RSP_LNK1_DPC"),col("RSP_LNK1_SLC"),col("RSP_LNK1_SRCIP"),col("RSP_LNK1_SRCPORT"),col("RSP_LNK1_DESTIP"),col("RSP_LNK1_DESTPORT"),col("RSP_LNK2_TYPE"),col("RSP_LNK2_SIG_TYPE"),col("RSP_LNK2_OPC"),col("RSP_LNK2_DPC"),col("RSP_LNK2_SLC"),col("RSP_LNK2_SRCIP"),col("RSP_LNK2_SRCPORT"),col("RSP_LNK2_DESTIP"),col("RSP_LNK2_DESTPORT"),col("RSP_LNK3_TYPE"),col("RSP_LNK3_SIG_TYPE"),col("RSP_LNK3_OPC"),col("RSP_LNK3_DPC"),col("RSP_LNK3_SLC"),col("RSP_LNK3_SRCIP"),col("RSP_LNK3_SRCPORT"),col("RSP_LNK3_DESTIP"),col("RSP_LNK3_DESTPORT"),col("RSP_LNK4_TYPE"),col("RSP_LNK4_SIG_TYPE"),col("RSP_LNK4_OPC"),col("RSP_LNK4_DPC"),col("RSP_LNK4_SLC"),col("RSP_LNK4_SRCIP"),col("RSP_LNK4_SRCPORT"),col("RSP_LNK4_DESTIP"),col("RSP_LNK4_DESTPORT"),col("USER_CATEGORY"),col("BSS_IND"),col("IMEI"),col("CUSTOMER_TYPE"),col("SRV_ATTR_ID"),col("FAIL_CATEGORY"),col("RULE_CODE"),col("SV"),col("CALC_TIME"),col("c1"),col("c2"),col("_corrupt_record"),col("year"),col("month"),col("day"),col("hour"))

// COMMAND ----------

import scala.concurrent.duration._
import org.apache.spark.sql.streaming.StreamingQuery
import org.apache.spark.sql.streaming.Trigger

// Escritura en streaming a Delta Lake
val query = df.writeStream
  .format("delta")
  .outputMode("append") // Solo agrega nuevos datos
  .partitionBy("year", "month", "day", "hour")
  .option("checkpointLocation", s"${path_adls_smartcare}/checkpoints/tdr_map_sms") // Ruta de checkpoints
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
    println(s"📌 Último Checkpoint: ${s"${path_adls_smartcare}/checkpoints/tdr_map_sms"}")

  } else {
    println("⚠️ No hay progreso disponible todavía.")
  }

  println("======================================")
  Thread.sleep(10000)  // Consulta cada 10 segundos
}

//query.awaitTermination()
