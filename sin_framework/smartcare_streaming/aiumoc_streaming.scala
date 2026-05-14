// Databricks notebook source
import org.apache.spark.sql.types.{StructType, StructField, StringType, IntegerType, ShortType, LongType}

// COMMAND ----------


dbutils.widgets.text("path_adls_smartcare","abfss://smartcare@stbigdataprd02.dfs.core.windows.net")
dbutils.widgets.text("path_adls","abfss://bigdatadev@stbigdatadev02.dfs.core.windows.net")
dbutils.widgets.text("ruta_origen","/data/trafico/trafico_detalle/smartcare/cdr_aiu_moc_test/landing")
dbutils.widgets.text("path_salida","/data/trafico/trafico_detalle/smartcare/cdr_aiu_moc_test/raw")
dbutils.widgets.text("catalogo","bidesarrollo")
dbutils.widgets.text("tabla_salida","raw_trafico.smartcare_cdr_aiu_moc")

// COMMAND ----------

val path_adls = dbutils.widgets.get("path_adls")
val path_adls_smartcare = dbutils.widgets.get("path_adls_smartcare")
val catalogo = dbutils.widgets.get("catalogo")
val landing_path = path_adls_smartcare + dbutils.widgets.get("ruta_origen")
val tabla_salida = catalogo +"."+ dbutils.widgets.get("tabla_salida")
val path_salida = path_adls + dbutils.widgets.get("path_salida")

// COMMAND ----------

import org.apache.spark.sql.types._

val schema = StructType(Array(
    StructField("CDRID", StringType, nullable = false),
    StructField("SYSID", IntegerType, nullable = false),
    StructField("ORGCDRID", StringType, nullable = false),
    StructField("ABISCDRID", StringType, nullable = false),
    StructField("ACDRID", StringType, nullable = false),
    StructField("REFID", StringType, nullable = false),
    StructField("ZONEID", LongType, nullable = false),
    StructField("POOLID", LongType, nullable = false),
    StructField("SCCPID", StringType, nullable = false),
    StructField("STARTTIME", IntegerType, nullable = false),
    StructField("MILLISEC", IntegerType, nullable = false),
    StructField("ABIS", IntegerType, nullable = false),
    StructField("SRVSTAT", IntegerType, nullable = false),
    StructField("CDRSTAT", IntegerType, nullable = false),
    StructField("NI", IntegerType, nullable = false),
    StructField("OPC", IntegerType, nullable = false),
    StructField("DPC", IntegerType, nullable = false),
    StructField("LINKSETID", IntegerType, nullable = false),
    StructField("ACCESS_TYPE", IntegerType, nullable = false),
    StructField("SRVTYPE", IntegerType, nullable = false),
    StructField("CHTYPE", IntegerType, nullable = false),
    StructField("CICSYSNO", IntegerType, nullable = false),
    StructField("CICTSNO", IntegerType, nullable = false),
    StructField("CODEC", IntegerType, nullable = false),
    StructField("IPADDR", LongType, nullable = false),
    StructField("PORT", IntegerType, nullable = false),
    StructField("NSAP", StringType, nullable = false),
    StructField("BINDINGID", StringType, nullable = false),
    StructField("HOLD", IntegerType, nullable = false),
    StructField("IMSI", StringType, nullable = false),
    StructField("IMEI", StringType, nullable = false),
    StructField("TMSI", StringType, nullable = false),
    StructField("CALLEDNO", StringType, nullable = false),
    StructField("CLDTYPE", IntegerType, nullable = false),
    StructField("CALLEDUSRNO", StringType, nullable = false),
    StructField("CALLERNO", StringType, nullable = false),
    StructField("MCC", StringType, nullable = false),
    StructField("MNC", StringType, nullable = false),
    StructField("FIRSTLAC", StringType, nullable = false),
    StructField("FIRSTCI", StringType, nullable = false),
    StructField("LASTLAC", StringType, nullable = false),
    StructField("LASTCI", StringType, nullable = false),
    StructField("SERVICEKEY", LongType, nullable = false),
    StructField("SCPNO", StringType, nullable = false),
    StructField("CAPVERSION", IntegerType, nullable = false),
    StructField("FIRSTTEI", IntegerType, nullable = false),
    StructField("FIRSTCHANNEL", IntegerType, nullable = false),
    StructField("SUMRXLEVLUL", LongType, nullable = false),
    StructField("SUMRXLEVLDL", LongType, nullable = false),
    StructField("SUMRXQUALUL", LongType, nullable = false),
    StructField("SUMRXQUALDL", LongType, nullable = false),
    StructField("SUMBSPWR", LongType, nullable = false),
    StructField("SUMMSPWR", LongType, nullable = false),
    StructField("SUMTA", LongType, nullable = false),
    StructField("TOTALMRNO", IntegerType, nullable = false),
    StructField("TOTALMRCOUNT", IntegerType, nullable = false),
    StructField("TOTALFULLRATEMRNO", IntegerType, nullable = false),
    StructField("TOTALHALFRATEMRNO", IntegerType, nullable = false),
    StructField("SUMRXQUAUL0", IntegerType, nullable = false),
    StructField("SUMRXQUAUL1", IntegerType, nullable = false),
    StructField("SUMRXQUAUL2", IntegerType, nullable = false),
    StructField("SUMRXQUAUL3", IntegerType, nullable = false),
    StructField("SUMRXQUAUL4", IntegerType, nullable = false),
    StructField("SUMRXQUAUL5", IntegerType, nullable = false),
    StructField("SUMRXQUAUL6", IntegerType, nullable = false),
    StructField("SUMRXQUAUL7", IntegerType, nullable = false),
    StructField("SUMRXQUADL0", IntegerType, nullable = false),
    StructField("SUMRXQUADL1", IntegerType, nullable = false),
    StructField("SUMRXQUADL2", IntegerType, nullable = false),
    StructField("SUMRXQUADL3", IntegerType, nullable = false),
    StructField("SUMRXQUADL4", IntegerType, nullable = false),
    StructField("SUMRXQUADL5", IntegerType, nullable = false),
    StructField("SUMRXQUADL6", IntegerType, nullable = false),
    StructField("SUMRXQUADL7", IntegerType, nullable = false),
    StructField("TA01COUNT", IntegerType, nullable = false),
    StructField("RXLEVEL_DOWN", IntegerType, nullable = false),
    StructField("WEAK_COVERAGE", IntegerType, nullable = false),
    StructField("OVER_COVERAGE", IntegerType, nullable = false),
    StructField("IMBALANCE", IntegerType, nullable = false),
    StructField("INTERFERENCE", IntegerType, nullable = false),
    StructField("IMM_ASS_COMD_TIME", IntegerType, nullable = false),
    StructField("IMM_ASS_REJ_TIME", IntegerType, nullable = false),
    StructField("IMM_ASS_COMP_TIME", IntegerType, nullable = false),
    StructField("CM_SRVACP_TIME", IntegerType, nullable = false),
    StructField("AUTH_REQ_TIME", IntegerType, nullable = false),
    StructField("AUTH_RSP_TIME", IntegerType, nullable = false),
    StructField("IDENTITY_REQ_TIME", IntegerType, nullable = false),
    StructField("IDENTITY_RSP_TIME", IntegerType, nullable = false),
    StructField("CIPH_REQ_TIME", IntegerType, nullable = false),
    StructField("CIPH_RSP_TIME", IntegerType, nullable = false),
    StructField("SETUP_TIME", IntegerType, nullable = false),
    StructField("CALL_PROC_TIME", IntegerType, nullable = false),
    StructField("ASSN_TIME", IntegerType, nullable = false),
    StructField("ASS_COMD_TIME", IntegerType, nullable = false),
    StructField("ASSN_CMPT_TIME", IntegerType, nullable = false),
    StructField("ALERT_TIME", IntegerType, nullable = false),
    StructField("ANSWER_TIME", IntegerType, nullable = false),
    StructField("DISCONN_TIME", IntegerType, nullable = false),
    StructField("REL_TIME", IntegerType, nullable = false),
    StructField("RELCMP_TIME", IntegerType, nullable = false),
    StructField("CLR_CMD_TIME", IntegerType, nullable = false),
    StructField("CLR_CMP_TIME", IntegerType, nullable = false),
    StructField("CONNECTION_FAILURE_TIME", IntegerType, nullable = false),
    StructField("CLEAR_REQ_TIME", IntegerType, nullable = false),
    StructField("END_TIME", IntegerType, nullable = false),
    StructField("PPD", IntegerType, nullable = false),
    StructField("REL_PHASE", IntegerType, nullable = false),
    StructField("PD", IntegerType, nullable = false),
    StructField("FIRFAILMSG", IntegerType, nullable = false),
    StructField("CAUSE", IntegerType, nullable = false),
    StructField("HOMEMCC", StringType, nullable = false),
    StructField("HOMEMNC", StringType, nullable = false),
    StructField("HOMEPROID", LongType, nullable = false),
    StructField("HOMEAREAID", LongType, nullable = false),
    StructField("DESTCC", StringType, nullable = false),
    StructField("DESTAREAID", StringType, nullable = false),
    StructField("DESTNUMTYPE", StringType, nullable = false),
    StructField("DESTOPRID", StringType, nullable = false),
    StructField("ENCRYPT_VERSION", IntegerType, nullable = false),
    StructField("RESERVED1", StringType, nullable = false),
    StructField("RESERVED2", StringType, nullable = false),
    StructField("RESERVED3", StringType, nullable = false),
    StructField("RESERVED4", StringType, nullable = false),
    StructField("RESERVED5", LongType, nullable = false),
    StructField("RESERVED6", LongType, nullable = false),
    StructField("RESERVED7", LongType, nullable = false),
    StructField("RESERVED8", LongType, nullable = false),
    StructField("FILELOCATION", StringType, nullable = false),
    StructField("OFFSET_DSI", LongType, nullable = false),
    StructField("PROBEID", LongType, nullable = false),
    StructField("PROBEID2", LongType, nullable = false),
    StructField("GROUPID", IntegerType, nullable = false),
    StructField("LNKOPC", IntegerType, nullable = false),
    StructField("LNKDPC", IntegerType, nullable = false),
    StructField("LNKSRCIP", LongType, nullable = false),
    StructField("LNKSRCPORT", IntegerType, nullable = false),
    StructField("LNKDESTIP", LongType, nullable = false),
    StructField("LNKDESTPORT", IntegerType, nullable = false),
    StructField("VASSERVICETYPE", IntegerType, nullable = false),
    StructField("AFIRSTXDRID", StringType, nullable = false),
    StructField("CALLDURATION", LongType, nullable = false),
    StructField("FIRFAILTIME", IntegerType, nullable = false),
    StructField("PROGRESS_TIME", IntegerType, nullable = false),
    StructField("LAYER1ID", IntegerType, nullable = false),
    StructField("LAYER2ID", IntegerType, nullable = false),
    StructField("LAYER3ID", IntegerType, nullable = false),
    StructField("LAYER4ID", IntegerType, nullable = false),
    StructField("LAYER5ID", IntegerType, nullable = false),
    StructField("LAYER6ID", IntegerType, nullable = false),
    StructField("DESTCCID", LongType, nullable = false),
    StructField("DESTNUMID", LongType, nullable = false),
    StructField("CALLSERVICE", IntegerType, nullable = false),
    StructField("CSFBIND", ShortType, nullable = false),
    StructField("FALLBACKTIME", LongType, nullable = false),
    StructField("TAI", StringType, nullable = false),
    StructField("ECGI", StringType, nullable = false),
    StructField("MME_ID", LongType, nullable = false),
    StructField("CSFB_REF_FLAG", ShortType, nullable = false),
    StructField("LASTBSCRNC", IntegerType, nullable = false),
    StructField("LASTACCESSTYPE", ShortType, nullable = false),
    StructField("FIRSTHOTIME", LongType, nullable = false),
    StructField("LASTPOOLID", LongType, nullable = false),
    StructField("ANNOUNCEFLAG", ShortType, nullable = false),
    StructField("E2EFIRFAILPROT", LongType, nullable = false),
    StructField("E2EFIRFAILPD", IntegerType, nullable = false),
    StructField("E2EFIRFAILMSG", IntegerType, nullable = false),
    StructField("E2EFIRFAILCAUSE", LongType, nullable = false),
    StructField("E2EFIRFAILSIDE", ShortType, nullable = false),
    StructField("CONNECT_TIME", IntegerType, nullable = false),
    StructField("RABREL_TIME", IntegerType, nullable = false),
    StructField("FIRTCHTIME", LongType, nullable = false),
    StructField("DROPCAUSE", IntegerType, nullable = false),
    StructField("DRD", ShortType, nullable = false),
    StructField("FIRST_LAC", StringType, nullable = false),
    StructField("FIRST_CI", StringType, nullable = false),
    StructField("FIRST_RAT", ShortType, nullable = false),
    StructField("LAST_LAC", StringType, nullable = false),
    StructField("LAST_CI", StringType, nullable = false),
    StructField("LAST_RAT", ShortType, nullable = false),
    StructField("FIRST_LONGITUDE", LongType, nullable = false),
    StructField("FIRST_LATITUDE", LongType, nullable = false),
    StructField("FIRST_ALTITUDE", IntegerType, nullable = false),
    StructField("FIRST_RASTERLONGITUDE", LongType, nullable = false),
    StructField("FIRST_RASTERLATITUDE", LongType, nullable = false),
    StructField("FIRST_RASTERALTITUDE", IntegerType, nullable = false),
    StructField("FIRST_FREQUENCYSPOT", IntegerType, nullable = false),
    StructField("FIRST_CLUTTER", IntegerType, nullable = false),
    StructField("FIRST_USERBEHAVIOR", IntegerType, nullable = false),
    StructField("FIRST_SPEED", IntegerType, nullable = false),
    StructField("FIRST_CREDIBILITY", IntegerType, nullable = false),
    StructField("LAST_LONGITUDE", LongType, nullable = false),
    StructField("LAST_LATITUDE", LongType, nullable = false),
    StructField("LAST_ALTITUDE", IntegerType, nullable = false),
    StructField("LAST_RASTERLONGITUDE", LongType, nullable = false),
    StructField("LAST_RASTERLATITUDE", LongType, nullable = false),
    StructField("LAST_RASTERALTITUDE", IntegerType, nullable = false),
    StructField("LAST_FREQUENCYSPOT", IntegerType, nullable = false),
    StructField("LAST_CLUTTER", IntegerType, nullable = false),
    StructField("LAST_USERBEHAVIOR", IntegerType, nullable = false),
    StructField("LAST_SPEED", IntegerType, nullable = false),
    StructField("LAST_CREDIBILITY", IntegerType, nullable = false),
    StructField("OLD_TMSI", StringType, nullable = false),
    StructField("AUTH_SEC_REQ_TIME", IntegerType, nullable = false),
    StructField("CLASSMARK_REQ_TIME", IntegerType, nullable = false),
    StructField("CLASSMARK_UPDATE_TIME", IntegerType, nullable = false),
    StructField("OPPRELCGI", StringType, nullable = false),
    StructField("PREPAID_FLAG", ShortType, nullable = false),
    StructField("LASTMNC", StringType, nullable = false),
    StructField("SESSION_TERMINATE_FLAG", ShortType, nullable = false),
    StructField("SESSIONKEY", StringType, nullable = false),
    StructField("CDR_STATUS_TYPE", LongType, nullable = false),
    StructField("USER_CATEGORY", LongType, nullable = false),
    StructField("FWDNO", StringType, nullable = false),
    StructField("c01", StringType, nullable = false)
))

// COMMAND ----------

val aiumoc = spark.readStream
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

val df = aiumoc.
withColumn("year", date_format(from_unixtime(col("STARTTIME")), "yyyy")).
withColumn("month", date_format(from_unixtime(col("STARTTIME")), "MM")).
withColumn("day", date_format(from_unixtime(col("STARTTIME")), "dd")).
withColumn("hour", date_format(from_unixtime(col("STARTTIME")), "HH")).
select(col("CDRID"),col("SYSID"),col("ORGCDRID"),col("ABISCDRID"),col("ACDRID"),col("REFID"),col("ZONEID"),col("POOLID"),col("SCCPID"),col("STARTTIME"),col("MILLISEC"),col("ABIS"),col("SRVSTAT"),col("CDRSTAT"),col("NI"),col("OPC"),col("DPC"),col("LINKSETID"),col("ACCESS_TYPE"),col("SRVTYPE"),col("CHTYPE"),col("CICSYSNO"),col("CICTSNO"),col("CODEC"),col("IPADDR"),col("PORT"),col("NSAP"),col("BINDINGID"),col("HOLD"),col("IMSI"),col("IMEI"),col("TMSI"),col("CALLEDNO"),col("CLDTYPE"),col("CALLEDUSRNO"),col("CALLERNO"),col("MCC"),col("MNC"),col("FIRSTLAC"),col("FIRSTCI"),col("LASTLAC"),col("LASTCI"),col("SERVICEKEY"),col("SCPNO"),col("CAPVERSION"),col("FIRSTTEI"),col("FIRSTCHANNEL"),col("SUMRXLEVLUL"),col("SUMRXLEVLDL"),col("SUMRXQUALUL"),col("SUMRXQUALDL"),col("SUMBSPWR"),col("SUMMSPWR"),col("SUMTA"),col("TOTALMRNO"),col("TOTALMRCOUNT"),col("TOTALFULLRATEMRNO"),col("TOTALHALFRATEMRNO"),col("SUMRXQUAUL0"),col("SUMRXQUAUL1"),col("SUMRXQUAUL2"),col("SUMRXQUAUL3"),col("SUMRXQUAUL4"),col("SUMRXQUAUL5"),col("SUMRXQUAUL6"),col("SUMRXQUAUL7"),col("SUMRXQUADL0"),col("SUMRXQUADL1"),col("SUMRXQUADL2"),col("SUMRXQUADL3"),col("SUMRXQUADL4"),col("SUMRXQUADL5"),col("SUMRXQUADL6"),col("SUMRXQUADL7"),col("TA01COUNT"),col("RXLEVEL_DOWN"),col("WEAK_COVERAGE"),col("OVER_COVERAGE"),col("IMBALANCE"),col("INTERFERENCE"),col("IMM_ASS_COMD_TIME"),col("IMM_ASS_REJ_TIME"),col("IMM_ASS_COMP_TIME"),col("CM_SRVACP_TIME"),col("AUTH_REQ_TIME"),col("AUTH_RSP_TIME"),col("IDENTITY_REQ_TIME"),col("IDENTITY_RSP_TIME"),col("CIPH_REQ_TIME"),col("CIPH_RSP_TIME"),col("SETUP_TIME"),col("CALL_PROC_TIME"),col("ASSN_TIME"),col("ASS_COMD_TIME"),col("ASSN_CMPT_TIME"),col("ALERT_TIME"),col("ANSWER_TIME"),col("DISCONN_TIME"),col("REL_TIME"),col("RELCMP_TIME"),col("CLR_CMD_TIME"),col("CLR_CMP_TIME"),col("CONNECTION_FAILURE_TIME"),col("CLEAR_REQ_TIME"),col("END_TIME"),col("PPD"),col("REL_PHASE"),col("PD"),col("FIRFAILMSG"),col("CAUSE"),col("HOMEMCC"),col("HOMEMNC"),col("HOMEPROID"),col("HOMEAREAID"),col("DESTCC"),col("DESTAREAID"),col("DESTNUMTYPE"),col("DESTOPRID"),col("ENCRYPT_VERSION"),col("RESERVED1"),col("RESERVED2"),col("RESERVED3"),col("RESERVED4"),col("RESERVED5"),col("RESERVED6"),col("RESERVED7"),col("RESERVED8"),col("FILELOCATION"),col("OFFSET_DSI"),col("PROBEID"),col("PROBEID2"),col("GROUPID"),col("LNKOPC"),col("LNKDPC"),col("LNKSRCIP"),col("LNKSRCPORT"),col("LNKDESTIP"),col("LNKDESTPORT"),col("VASSERVICETYPE"),col("AFIRSTXDRID"),col("CALLDURATION"),col("FIRFAILTIME"),col("PROGRESS_TIME"),col("LAYER1ID"),col("LAYER2ID"),col("LAYER3ID"),col("LAYER4ID"),col("LAYER5ID"),col("LAYER6ID"),col("DESTCCID"),col("DESTNUMID"),col("CALLSERVICE"),col("CSFBIND"),col("FALLBACKTIME"),col("TAI"),col("ECGI"),col("MME_ID"),col("CSFB_REF_FLAG"),col("LASTBSCRNC"),col("LASTACCESSTYPE"),col("FIRSTHOTIME"),col("LASTPOOLID"),col("ANNOUNCEFLAG"),col("E2EFIRFAILPROT"),col("E2EFIRFAILPD"),col("E2EFIRFAILMSG"),col("E2EFIRFAILCAUSE"),col("E2EFIRFAILSIDE"),col("CONNECT_TIME"),col("RABREL_TIME"),col("FIRTCHTIME"),col("DROPCAUSE"),col("DRD"),col("FIRST_LAC"),col("FIRST_CI"),col("FIRST_RAT"),col("LAST_LAC"),col("LAST_CI"),col("LAST_RAT"),col("FIRST_LONGITUDE"),col("FIRST_LATITUDE"),col("FIRST_ALTITUDE"),col("FIRST_RASTERLONGITUDE"),col("FIRST_RASTERLATITUDE"),col("FIRST_RASTERALTITUDE"),col("FIRST_FREQUENCYSPOT"),col("FIRST_CLUTTER"),col("FIRST_USERBEHAVIOR"),col("FIRST_SPEED"),col("FIRST_CREDIBILITY"),col("LAST_LONGITUDE"),col("LAST_LATITUDE"),col("LAST_ALTITUDE"),col("LAST_RASTERLONGITUDE"),col("LAST_RASTERLATITUDE"),col("LAST_RASTERALTITUDE"),col("LAST_FREQUENCYSPOT"),col("LAST_CLUTTER"),col("LAST_USERBEHAVIOR"),col("LAST_SPEED"),col("LAST_CREDIBILITY"),col("OLD_TMSI"),col("AUTH_SEC_REQ_TIME"),col("CLASSMARK_REQ_TIME"),col("CLASSMARK_UPDATE_TIME"),col("OPPRELCGI"),col("PREPAID_FLAG"),col("LASTMNC"),col("SESSION_TERMINATE_FLAG"),col("SESSIONKEY"),col("CDR_STATUS_TYPE"),col("USER_CATEGORY"),col("FWDNO"),col("c01").as("_corrupt_record"),col("year"),col("month"),col("day"),col("hour"))

// COMMAND ----------

import scala.concurrent.duration._
import org.apache.spark.sql.streaming.StreamingQuery
import org.apache.spark.sql.streaming.Trigger

// Escritura en streaming a Delta Lake
val query = df.writeStream
  .format("delta")
  .outputMode("append") // Solo agrega nuevos datos
  .partitionBy("year", "month", "day", "hour")
  .option("checkpointLocation", s"${path_adls_smartcare}/checkpoints/aiumoc") // Ruta de checkpoints
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
    println(s"📌 Último Checkpoint: ${s"${path_adls_smartcare}/checkpoints/aiumoc"}")

  } else {
    println("⚠️ No hay progreso disponible todavía.")
  }

  println("======================================")
  Thread.sleep(10000)  // Consulta cada 10 segundos
}

//query.awaitTermination()
