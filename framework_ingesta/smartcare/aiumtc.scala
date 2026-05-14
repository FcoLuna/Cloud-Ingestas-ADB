// Databricks notebook source
import org.apache.spark.sql.types.{StructType, StructField, StringType, IntegerType, ShortType, LongType}

// COMMAND ----------

/*
dbutils.widgets.text("moment","2024/11/05/15/0/")
dbutils.widgets.text("path_adls","abfss://bigdatadev@stbigdatadev02.dfs.core.windows.net")
dbutils.widgets.text("ruta_origen","/data/trafico/trafico_detalle/smartcare/landing/cdr_aiu_mtc/")
dbutils.widgets.text("path_salida","/data/trafico/trafico_detalle/smartcare/cdr_aiu_mtc/raw/")
dbutils.widgets.text("catalogo","bi-ingestas")
dbutils.widgets.text("tabla_salida","raw_trafico.smartcare_cdr_aiu_mtc")
*/

// COMMAND ----------

val path_adls = dbutils.widgets.get("path_adls")
val catalogo = dbutils.widgets.get("catalogo")
val landing_aiumtc = path_adls + dbutils.widgets.get("ruta_origen")
val tabla_salida = catalogo +"."+ dbutils.widgets.get("tabla_salida")
val path_salida = path_adls + dbutils.widgets.get("path_salida")

// COMMAND ----------

val schema = StructType(Array(
    StructField("CDRID", StringType, nullable = false),
    StructField("SYSID", StringType, nullable = false),
    StructField("ORGCDRID", StringType, nullable = false),
    StructField("ABISCDRID", StringType, nullable = false),
    StructField("ACDRID", StringType, nullable = false),
    StructField("REFID", StringType, nullable = false),
    StructField("ZONEID", StringType, nullable = false),
    StructField("POOLID", StringType, nullable = false),
    StructField("SCCPID", StringType, nullable = false),
    StructField("STARTTIME", IntegerType, nullable = false),
    StructField("MILLISEC", StringType, nullable = false),
    StructField("ABIS", StringType, nullable = false),
    StructField("SRVSTAT", StringType, nullable = false),
    StructField("CDRSTAT", StringType, nullable = false),
    StructField("NI", StringType, nullable = false),
    StructField("OPC", StringType, nullable = false),
    StructField("DPC", StringType, nullable = false),
    StructField("ACCESS_TYPE", StringType, nullable = false),
    StructField("SRVTYPE", StringType, nullable = false),
    StructField("CHTYPE", StringType, nullable = false),
    StructField("CICSYSNO", StringType, nullable = false),
    StructField("CICTSNO", StringType, nullable = false),
    StructField("CODEC", StringType, nullable = false),
    StructField("IPADDR", StringType, nullable = false),
    StructField("PORT", StringType, nullable = false),
    StructField("NSAP", StringType, nullable = false),
    StructField("BINDINGID", StringType, nullable = false),
    StructField("HOLD", StringType, nullable = false),
    StructField("IMSI", StringType, nullable = false),
    StructField("IMEI", StringType, nullable = false),
    StructField("TMSI", StringType, nullable = false),
    StructField("CALLEDNO", StringType, nullable = false),
    StructField("CALLERNO", StringType, nullable = false),
    StructField("MCC", StringType, nullable = false),
    StructField("MNC", StringType, nullable = false),
    StructField("FirstLAC", StringType, nullable = false),
    StructField("FirstCI", StringType, nullable = false),
    StructField("LastLAC", StringType, nullable = false),
    StructField("LastCI", StringType, nullable = false),
    StructField("SERVICEKEY", StringType, nullable = false),
    StructField("SCPNO", StringType, nullable = false),
    StructField("CapVersion", StringType, nullable = false),
    StructField("FirstTEI", StringType, nullable = false),
    StructField("Firstchannel", StringType, nullable = false),
    StructField("SumRxlevlUL", StringType, nullable = false),
    StructField("SumRxlevlDL", StringType, nullable = false),
    StructField("SumRxqualUL", StringType, nullable = false),
    StructField("SumRxqualDL", StringType, nullable = false),
    StructField("SumBspwr", StringType, nullable = false),
    StructField("SumMspwr", StringType, nullable = false),
    StructField("SumTA", StringType, nullable = false),
    StructField("TotalMRno", StringType, nullable = false),
    StructField("TOTALMRCOUNT", StringType, nullable = false),
    StructField("TotalFullRateMRno", StringType, nullable = false),
    StructField("TotalHalfRateMRno", StringType, nullable = false),
    StructField("SumRxquaUL0", StringType, nullable = false),
    StructField("SumRxquaUL1", StringType, nullable = false),
    StructField("SumRxquaUL2", StringType, nullable = false),
    StructField("SumRxquaUL3", StringType, nullable = false),
    StructField("SumRxquaUL4", StringType, nullable = false),
    StructField("SumRxquaUL5", StringType, nullable = false),
    StructField("SumRxquaUL6", StringType, nullable = false),
    StructField("SumRxquaUL7", StringType, nullable = false),
    StructField("SumRxquaDL0", StringType, nullable = false),
    StructField("SumRxquaDL1", StringType, nullable = false),
    StructField("SumRxquaDL2", StringType, nullable = false),
    StructField("SumRxquaDL3", StringType, nullable = false),
    StructField("SumRxquaDL4", StringType, nullable = false),
    StructField("SumRxquaDL5", StringType, nullable = false),
    StructField("SumRxquaDL6", StringType, nullable = false),
    StructField("SumRxquaDL7", StringType, nullable = false),
    StructField("TA01Count", StringType, nullable = false),
    StructField("Rxlevel_Down", StringType, nullable = false),
    StructField("Weak_coverage", StringType, nullable = false),
    StructField("Over_coverage", StringType, nullable = false),
    StructField("Imbalance", StringType, nullable = false),
    StructField("Interference", StringType, nullable = false),
    StructField("Imm_Ass_Comd_Time", StringType, nullable = false),
    StructField("Imm_Ass_Rej_Time", StringType, nullable = false),
    StructField("Imm_Ass_Comp_Time", StringType, nullable = false),
    StructField("AUTH_REQ_TIME", StringType, nullable = false),
    StructField("AUTH_RSP_TIME", StringType, nullable = false),
    StructField("IDENTITY_REQ_TIME", StringType, nullable = false),
    StructField("IDENTITY_RSP_TIME", StringType, nullable = false),
    StructField("CIPH_REQ_TIME", StringType, nullable = false),
    StructField("CIPH_RSP_TIME", StringType, nullable = false),
    StructField("SETUP_TIME", StringType, nullable = false),
    StructField("CALL_PROC_TIME", StringType, nullable = false),
    StructField("ASSN_TIME", StringType, nullable = false),
    StructField("Ass_Comd_Time", StringType, nullable = false),
    StructField("ASSN_CMPT_TIME", StringType, nullable = false),
    StructField("ALERT_TIME", StringType, nullable = false),
    StructField("ANSWER_TIME", IntegerType, nullable = false),
    StructField("DISCONN_TIME", StringType, nullable = false),
    StructField("REL_TIME", StringType, nullable = false),
    StructField("RELCMP_TIME", StringType, nullable = false),
    StructField("CLR_CMD_TIME", StringType, nullable = false),
    StructField("CLR_CMP_TIME", StringType, nullable = false),
    StructField("FIRPAGING_TIME", StringType, nullable = false),
    StructField("SECPAGING_TIME", StringType, nullable = false),
    StructField("THIRDPAGING_TIME", StringType, nullable = false),
    StructField("FOURTHPAGING_TIME", StringType, nullable = false),
    StructField("FIFTHPAGING_TIME", StringType, nullable = false),
    StructField("PAGINGRSP_TIME", StringType, nullable = false),
    StructField("Connection_Failure_Time", StringType, nullable = false),
    StructField("CLEAR_REQ_TIME", StringType, nullable = false),
    StructField("END_TIME", IntegerType, nullable = false),
    StructField("PPD", StringType, nullable = false),
    StructField("REL_PHASE", StringType, nullable = false),
    StructField("PD", StringType, nullable = false),
    StructField("FIRFAILMSG", StringType, nullable = false),
    StructField("CAUSE", StringType, nullable = false),
    StructField("HOMEMCC", StringType, nullable = false),
    StructField("HOMEMNC", StringType, nullable = false),
    StructField("HOMEPROID", StringType, nullable = false),
    StructField("HOMEAREAID", StringType, nullable = false),
    StructField("ENCRYPT_VERSION", StringType, nullable = false),
    StructField("RESERVED1", StringType, nullable = false),
    StructField("RESERVED2", StringType, nullable = false),
    StructField("RESERVED3", StringType, nullable = false),
    StructField("RESERVED4", StringType, nullable = false),
    StructField("RESERVED5", StringType, nullable = false),
    StructField("RESERVED6", StringType, nullable = false),
    StructField("RESERVED7", StringType, nullable = false),
    StructField("RESERVED8", StringType, nullable = false),
    StructField("FILELOCATION", StringType, nullable = false),
    StructField("OFFSET", StringType, nullable = false),
    StructField("PROBEID", StringType, nullable = false),
    StructField("PROBEID2", StringType, nullable = false),
    StructField("GROUPID", StringType, nullable = false),
    StructField("LNKOPC", StringType, nullable = false),
    StructField("LNKDPC", StringType, nullable = false),
    StructField("LNKSRCIP", StringType, nullable = false),
    StructField("LNKSRCPORT", StringType, nullable = false),
    StructField("LNKDESTIP", StringType, nullable = false),
    StructField("LNKDESTPORT", StringType, nullable = false),
    StructField("CLERTYPE", StringType, nullable = false),
    StructField("LINKTYPE", StringType, nullable = false),
    StructField("aFirstXdrID", StringType, nullable = false),
    StructField("CALLDURATION", StringType, nullable = false),
    StructField("FIRFAILTIME", StringType, nullable = false),
    StructField("Layer1ID", StringType, nullable = false),
    StructField("Layer2ID", StringType, nullable = false),
    StructField("Layer3ID", StringType, nullable = false),
    StructField("Layer4ID", StringType, nullable = false),
    StructField("Layer5ID", StringType, nullable = false),
    StructField("Layer6ID", StringType, nullable = false),
    StructField("CSFBIND", StringType, nullable = false),
    StructField("FALLBACKTIME", StringType, nullable = false),
    StructField("TAI", StringType, nullable = false),
    StructField("ECGI", StringType, nullable = false),
    StructField("MME_ID", StringType, nullable = false),
    StructField("CSFB_REF_FLAG", StringType, nullable = false),
    StructField("LastBscRnc", StringType, nullable = false),
    StructField("LastAccessType", StringType, nullable = false),
    StructField("FirstHOTime", StringType, nullable = false),
    StructField("LastPoolID", StringType, nullable = false),
    StructField("E2EFIRFAILPROT", StringType, nullable = false),
    StructField("E2EFIRFAILPD", StringType, nullable = false),
    StructField("E2EFIRFAILMSG", StringType, nullable = false),
    StructField("E2EFIRFAILCAUSE", StringType, nullable = false),
    StructField("E2EFIRFAILSIDE", StringType, nullable = false),
    StructField("Connect_Time", StringType, nullable = false),
    StructField("RabRel_Time", StringType, nullable = false),
    StructField("FirTCHTIME", StringType, nullable = false),
    StructField("Dropcause", StringType, nullable = false),
    StructField("DRD", StringType, nullable = false),
    StructField("FIRST_LAC", StringType, nullable = false),
    StructField("FIRST_CI", StringType, nullable = false),
    StructField("FIRST_RAT", StringType, nullable = false),
    StructField("LAST_LAC", StringType, nullable = false),
    StructField("LAST_CI", StringType, nullable = false),
    StructField("LAST_RAT", StringType, nullable = false),
    StructField("FIRST_LONGITUDE", StringType, nullable = false),
    StructField("FIRST_LATITUDE", StringType, nullable = false),
    StructField("FIRST_ALTITUDE", StringType, nullable = false),
    StructField("FIRST_RASTERLONGITUDE", StringType, nullable = false),
    StructField("FIRST_RASTERLATITUDE", StringType, nullable = false),
    StructField("FIRST_RASTERALTITUDE", StringType, nullable = false),
    StructField("FIRST_FREQUENCYSPOT", StringType, nullable = false),
    StructField("FIRST_CLUTTER", StringType, nullable = false),
    StructField("FIRST_USERBEHAVIOR", StringType, nullable = false),
    StructField("FIRST_SPEED", StringType, nullable = false),
    StructField("FIRST_CREDIBILITY", StringType, nullable = false),
    StructField("LAST_LONGITUDE", StringType, nullable = false),
    StructField("LAST_LATITUDE", StringType, nullable = false),
    StructField("LAST_ALTITUDE", StringType, nullable = false),
    StructField("LAST_RASTERLONGITUDE", StringType, nullable = false),
    StructField("LAST_RASTERLATITUDE", StringType, nullable = false),
    StructField("LAST_RASTERALTITUDE", StringType, nullable = false),
    StructField("LAST_FREQUENCYSPOT", StringType, nullable = false),
    StructField("LAST_CLUTTER", StringType, nullable = false),
    StructField("LAST_USERBEHAVIOR", StringType, nullable = false),
    StructField("LAST_SPEED", StringType, nullable = false),
    StructField("LAST_CREDIBILITY", StringType, nullable = false),
    StructField("OLD_TMSI", StringType, nullable = false),
    StructField("AUTH_SEC_REQ_TIME", StringType, nullable = false),
    StructField("CLASSMARK_REQ_TIME", StringType, nullable = false),
    StructField("CLASSMARK_UPDATE_TIME", StringType, nullable = false),
    StructField("OPPRELCGI", StringType, nullable = false),
    StructField("PREPAID_FLAG", StringType, nullable = false),
    StructField("LASTMNC", StringType, nullable = false),
    StructField("SESSION_TERMINATE_FLAG", StringType, nullable = false),
    StructField("SESSIONKEY", StringType, nullable = false),
    StructField("CALLERCC", StringType, nullable = false),
    StructField("CALLEROPRID", StringType, nullable = false),
    StructField("CDR_STATUS_TYPE", StringType, nullable = false),
    StructField("USER_CATEGORY", StringType, nullable = false),
    StructField("c01", StringType, nullable = false)
))

// COMMAND ----------

val aiumtc = spark.read.option("delimiter","|").schema(schema).csv(landing_aiumtc)

// COMMAND ----------

val df = aiumtc.
withColumn("year", date_format(from_unixtime(col("STARTTIME")), "yyyy")).
withColumn("month", date_format(from_unixtime(col("STARTTIME")), "MM")).
withColumn("day", date_format(from_unixtime(col("STARTTIME")), "dd")).
withColumn("hour", date_format(from_unixtime(col("STARTTIME")), "HH")).
select(col("CDRID"),col("SYSID"),col("ORGCDRID"),col("ABISCDRID"),col("ACDRID"),col("REFID"),col("ZONEID"),col("POOLID"),col("SCCPID"),col("STARTTIME"),col("MILLISEC"),col("ABIS"),col("SRVSTAT"),col("CDRSTAT"),col("NI"),col("OPC"),col("DPC"),col("ACCESS_TYPE"),col("SRVTYPE"),col("CHTYPE"),col("CICSYSNO"),col("CICTSNO"),col("CODEC"),col("IPADDR"),col("PORT"),col("NSAP"),col("BINDINGID"),col("HOLD"),col("IMSI"),col("IMEI"),col("TMSI"),col("CALLEDNO"),col("CALLERNO"),col("MCC"),col("MNC"),col("FirstLAC"),col("FirstCI"),col("LastLAC"),col("LastCI"),col("SERVICEKEY"),col("SCPNO"),col("CapVersion"),col("FirstTEI"),col("Firstchannel"),col("SumRxlevlUL"),col("SumRxlevlDL"),col("SumRxqualUL"),col("SumRxqualDL"),col("SumBspwr"),col("SumMspwr"),col("SumTA"),col("TotalMRno"),col("TOTALMRCOUNT"),col("TotalFullRateMRno"),col("TotalHalfRateMRno"),col("SumRxquaUL0"),col("SumRxquaUL1"),col("SumRxquaUL2"),col("SumRxquaUL3"),col("SumRxquaUL4"),col("SumRxquaUL5"),col("SumRxquaUL6"),col("SumRxquaUL7"),col("SumRxquaDL0"),col("SumRxquaDL1"),col("SumRxquaDL2"),col("SumRxquaDL3"),col("SumRxquaDL4"),col("SumRxquaDL5"),col("SumRxquaDL6"),col("SumRxquaDL7"),col("TA01Count"),col("Rxlevel_Down"),col("Weak_coverage"),col("Over_coverage"),col("Imbalance"),col("Interference"),col("Imm_Ass_Comd_Time"),col("Imm_Ass_Rej_Time"),col("Imm_Ass_Comp_Time"),col("AUTH_REQ_TIME"),col("AUTH_RSP_TIME"),col("IDENTITY_REQ_TIME"),col("IDENTITY_RSP_TIME"),col("CIPH_REQ_TIME"),col("CIPH_RSP_TIME"),col("SETUP_TIME"),col("CALL_PROC_TIME"),col("ASSN_TIME"),col("Ass_Comd_Time"),col("ASSN_CMPT_TIME"),col("ALERT_TIME"),col("ANSWER_TIME"),col("DISCONN_TIME"),col("REL_TIME"),col("RELCMP_TIME"),col("CLR_CMD_TIME"),col("CLR_CMP_TIME"),col("FIRPAGING_TIME"),col("SECPAGING_TIME"),col("THIRDPAGING_TIME"),col("FOURTHPAGING_TIME"),col("FIFTHPAGING_TIME"),col("PAGINGRSP_TIME"),col("Connection_Failure_Time"),col("CLEAR_REQ_TIME"),col("END_TIME"),col("PPD"),col("REL_PHASE"),col("PD"),col("FIRFAILMSG"),col("CAUSE"),col("HOMEMCC"),col("HOMEMNC"),col("HOMEPROID"),col("HOMEAREAID"),col("ENCRYPT_VERSION"),col("RESERVED1"),col("RESERVED2"),col("RESERVED3"),col("RESERVED4"),col("RESERVED5"),col("RESERVED6"),col("RESERVED7"),col("RESERVED8"),col("FILELOCATION"),col("OFFSET"),col("PROBEID"),col("PROBEID2"),col("GROUPID"),col("LNKOPC"),col("LNKDPC"),col("LNKSRCIP"),col("LNKSRCPORT"),col("LNKDESTIP"),col("LNKDESTPORT"),col("CLERTYPE"),col("LINKTYPE"),col("aFirstXdrID"),col("CALLDURATION"),col("FIRFAILTIME"),col("Layer1ID"),col("Layer2ID"),col("Layer3ID"),col("Layer4ID"),col("Layer5ID"),col("Layer6ID"),col("CSFBIND"),col("FALLBACKTIME"),col("TAI"),col("ECGI"),col("MME_ID"),col("CSFB_REF_FLAG"),col("LastBscRnc"),col("LastAccessType"),col("FirstHOTime"),col("LastPoolID"),col("E2EFIRFAILPROT"),col("E2EFIRFAILPD"),col("E2EFIRFAILMSG"),col("E2EFIRFAILCAUSE"),col("E2EFIRFAILSIDE"),col("Connect_Time"),col("RabRel_Time"),col("FirTCHTIME"),col("Dropcause"),col("DRD"),col("FIRST_LAC"),col("FIRST_CI"),col("FIRST_RAT"),col("LAST_LAC"),col("LAST_CI"),col("LAST_RAT"),col("FIRST_LONGITUDE"),col("FIRST_LATITUDE"),col("FIRST_ALTITUDE"),col("FIRST_RASTERLONGITUDE"),col("FIRST_RASTERLATITUDE"),col("FIRST_RASTERALTITUDE"),col("FIRST_FREQUENCYSPOT"),col("FIRST_CLUTTER"),col("FIRST_USERBEHAVIOR"),col("FIRST_SPEED"),col("FIRST_CREDIBILITY"),col("LAST_LONGITUDE"),col("LAST_LATITUDE"),col("LAST_ALTITUDE"),col("LAST_RASTERLONGITUDE"),col("LAST_RASTERLATITUDE"),col("LAST_RASTERALTITUDE"),col("LAST_FREQUENCYSPOT"),col("LAST_CLUTTER"),col("LAST_USERBEHAVIOR"),col("LAST_SPEED"),col("LAST_CREDIBILITY"),col("OLD_TMSI"),col("AUTH_SEC_REQ_TIME"),col("CLASSMARK_REQ_TIME"),col("CLASSMARK_UPDATE_TIME"),col("OPPRELCGI"),col("PREPAID_FLAG"),col("LASTMNC"),col("SESSION_TERMINATE_FLAG"),col("SESSIONKEY"),col("CALLERCC"),col("CALLEROPRID"),col("CDR_STATUS_TYPE"),col("USER_CATEGORY"),col("c01").as("_corrupt_record"),col("year"),col("month"),col("day"),col("hour"))

// COMMAND ----------

if (!spark.catalog.tableExists(tabla_salida)) {
    df.write.
      mode("overwrite").
      format("delta").
      partitionBy("year","month","day","hour").
      option("path", path_salida).
      saveAsTable(tabla_salida)
    }else{
    df.write.
      mode("overwrite").
      format("delta").
      option("path", path_salida).
      insertInto(tabla_salida)
}
