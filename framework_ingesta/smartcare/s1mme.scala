// Databricks notebook source
/*dbutils.widgets.text("moment","2024/11/05/15/0/")
dbutils.widgets.text("path_adls","abfss://bigdatadev@stbigdatadev02.dfs.core.windows.net")
dbutils.widgets.text("ruta_origen","/data/trafico/trafico_detalle/smartcare/landing_traf_cert/s1mme/")
dbutils.widgets.text("path_salida","/data/trafico/trafico_detalle/smartcare/landing_traf_cert/s1mme/")
dbutils.widgets.text("catalogo","bidesarrollo")
dbutils.widgets.text("tabla_salida","trafico_cert.smartcare_s1mme") */

// COMMAND ----------

val path_adls = dbutils.widgets.get("path_adls")
val catalogo = dbutils.widgets.get("catalogo")
val landing_s1mme = path_adls + dbutils.widgets.get("ruta_origen")
val tabla_salida = catalogo +"."+ dbutils.widgets.get("tabla_salida")
val path_salida = path_adls + dbutils.widgets.get("path_salida")

// COMMAND ----------

val columnas = Seq("SID","PROTOCOL_ID","PROCEDURE_ID","PROBEID","ENCRYPT_VERSION","INTERFACEID","IMSI","MSISDN","IMEI","MS_IP","MTMSI","ROAMING_TYPE","HOMEMCC","HOMEMNC","HOMEPROID","HOMEAREAID","RAT","MCC","MNC","RAI_TAI","SAI_CGI_ECGI","APN","REFID","PROC_TYPE","PROC_STARTTIME","PROC_STARTTIME_MSEC","PROC_ENDTIME","PROC_ENDTIME_MSEC","PROC_SUCCED_FLAG","FIR_FAIL_TRANS_PROT","FIR_FAIL_TRANS","FIR_FAIL_CAUSE_TYPE","FIR_FAIL_CAUSE","MME_ID","RAN_NE_ID","MME_SIG_IP","RAN_NE_SIG_IP","SGW_SIGIP_LST","PGW_SIGIP_LST","EMM_TRANS_TYPE","EMM_TRANS_SUB_TYPE","EMM_DIERECTION","EMM_REQ_TIME_SEC","EMM_REQ_TIME_MSEC","EMM_END_TIME_SEC","EMM_END_TIME_MSEC","EMM_REQ_SUCCED_FLAG","EMM_REQ_RETRANS_FLAG","EMM_CAUSE","POWEROFF_FLAG","BEARER_ACTIVE_FLAG","ACTIVE_FLAG","OLD_MCC","OLD_MNC","OLD_RAI_TAI","OLD_SAI_CGI_ECGI","IDENTITY_REQ_TIME_SEC","IDENTITY_REQ_TIME_MSEC","IDENTITY_EMD_TIME_SEC","IDENTITY_EMD_TIME_MSEC","IDENTITY_SUCCED_FLAG","IDENTITY_RETRANS_FLAG","IDENTITY_CAUSE","AUTH_REQ_TIME_SEC","AUTH_REQ_TIME_MSEC","AUTH_END_TIME_SEC","AUTH_END_TIME_MSEC","AUTH_SUCCED_FLAG","AUTH_RETRANS_FLAG","AUTH_REJ_EMM_CAUSE","SECURITY_MODE_TIME","SECURITY_MODE_TIME_MSEC","SECURITY_MODE_CMPT_TIME","SECURITY_MODE_CMPT_TIME_MSEC","SECURITY_MODE_SUCCED_FLAG","SECURITY_MODE_RESELECT_FLAG","SECURITY_MODE_CAUSE","ESM_UE_TRANS_TYPE","ESM_UE_REQ_TIME_SEC","ESM_UE_REQ_TIME_MSEC","ESM_UE_END_TIME_SEC","ESM_UE_END_TIME_MSEC","ESM_UE_SUCCED_FLAG","ESM_UE_RETRANS_FLAG","ESM_UE_REJ_CAUSE","ESM_UE_EBI","ESM_UE_LBI","ESM_UE_ESMCAUSE","ESM_NW_TRANS_TYPE","ESM_NW_REQ_TIME_SEC","ESM_NW_REQ_TIME_MSEC","ESM_NW_END_TIME_SEC","ESM_NW_END_TIME_MSEC","ESM_NW_SUCCED_FLAG","ESM_NW_RETRANS_FLAG","ESM_NW_REJ_CAUSE","ESM_NW_EBI","ESM_NW_LBI","ESM_NW_ESMCAUSE","PAGING_REQ_TIME_SEC","PAGING_REQ_TIME_MSEC","PAGING_RSP_TIME_SEC","PAGING_RSP_TIME_MSEC","PAGING_REQ_SUCCED_FLAG","PAGING_REQ_RETRANS_FLAG","PAGING_REQ_TIMEOUT_FLAG","S1AP_TRANS_TYPE","S1AP_REQ_TIME_SEC","S1AP_REQ_TIME_MSEC","S1AP_END_TIME_SEC","S1AP_END_TIME_MSEC","S1AP_SUCCED_FLAG","S1AP_RETRANS_FLAG","S1AP_CAUSE_TYPE","S1AP_CAUSE","ERAB_COUNT_TO_BE_SETUP","ERAB_COUNT_FAILED_SETUP","ERAB_COUNT_SUCCEED_SETUP","RESERVED1","RESERVED2","RESERVED3","RESERVED4","CSFB_IND","S1AP_TRANS_DIRECTION","LAYER1ID","LAYER2ID","LAYER3ID","LAYER4ID","LAYER5ID","LAYER6ID","UE_SRVCC_CAPABILITY","CSFB_RESPONSE","TMSI","OLD_TMSI","LAC","CSCALL_TTIME","CSCALL_TTIME_MS","MBR_DL_NEG","GBR_DL_NEG","MBR_UL_NEG","GBR_UL_NEG","QCI_NEG","ARP_NEG","SV","MMEGI","MMEC","ENB_UE_S1AP_ID","MME_UE_S1AP_ID","IMEI_CIPHERTEXT","PREPAID_FLAG","IMS_VOICE_SUPPORT","BEAR_TFT","BEAR_RADIO_PRIORITY","BEAR_UL_APN_AMBR","BEAR_DL_APN_AMBR","UE_CONTEXT_STATUS","UE_VOICE_PREFERENCE","SESSIONKEY","PDN_TYPE","OLD_GUTI_TYPE","USER_CATEGORY","RLS_REQ_TYPE","RLS_REQ_TIME_SEC","RLS_REQ_TIME_MSEC","RLS_TRANS_TYPE","RLS_TRANS_REQ_TIME_SEC","RLS_TRANS_REQ_TIME_MSEC","RLS_TRANS_RSP_TIME_SEC","RLS_TRANS_RSP_TIME_MSEC","RLS_TRANS_SUCCED_FLAG","RLS_TRANS_CAUSE_TYPE","RLS_TRANS_CAUSE","IMS_SERVICE_TYPE","OLD_RAT","c1")
val s1mme = spark.read.option("delimiter","|").csv(landing_s1mme).toDF(columnas: _*)

// COMMAND ----------

val df = s1mme.
withColumn("year", date_format(from_unixtime(col("PROC_STARTTIME")), "yyyy")).
withColumn("month", date_format(from_unixtime(col("PROC_STARTTIME")), "MM")).
withColumn("day", date_format(from_unixtime(col("PROC_STARTTIME")), "dd")).
withColumn("hour", date_format(from_unixtime(col("PROC_STARTTIME")), "HH")).
select(
  col("SID"),col("PROTOCOL_ID"),col("PROCEDURE_ID"),col("PROBEID"),col("ENCRYPT_VERSION"),col("INTERFACEID"),col("IMSI"),col("MSISDN"),col("IMEI"),col("MS_IP"),col("MTMSI"),col("ROAMING_TYPE").cast("short"),col("HOMEMCC"),col("HOMEMNC"),col("HOMEPROID"),col("HOMEAREAID"),col("RAT").cast("bigint"),col("MCC"),col("MNC"),col("RAI_TAI"),col("SAI_CGI_ECGI"),col("APN"),col("REFID"),col("PROC_TYPE").cast("bigint"),col("PROC_STARTTIME").cast("bigint"),col("PROC_STARTTIME_MSEC"),col("PROC_ENDTIME").cast("bigint"),col("PROC_ENDTIME_MSEC"),col("PROC_SUCCED_FLAG").cast("short"),col("FIR_FAIL_TRANS_PROT").cast("bigint"),col("FIR_FAIL_TRANS").cast("bigint"),col("FIR_FAIL_CAUSE_TYPE").cast("bigint"),col("FIR_FAIL_CAUSE").cast("bigint"),col("MME_ID"),col("RAN_NE_ID"),col("MME_SIG_IP"),col("RAN_NE_SIG_IP"),col("SGW_SIGIP_LST"),col("PGW_SIGIP_LST"),col("EMM_TRANS_TYPE"),col("EMM_TRANS_SUB_TYPE"),col("EMM_DIERECTION"),col("EMM_REQ_TIME_SEC"),col("EMM_REQ_TIME_MSEC"),col("EMM_END_TIME_SEC"),col("EMM_END_TIME_MSEC"),col("EMM_REQ_SUCCED_FLAG"),col("EMM_REQ_RETRANS_FLAG"),col("EMM_CAUSE"),col("POWEROFF_FLAG"),col("BEARER_ACTIVE_FLAG"),col("ACTIVE_FLAG"),col("OLD_MCC"),col("OLD_MNC"),col("OLD_RAI_TAI"),col("OLD_SAI_CGI_ECGI"),col("IDENTITY_REQ_TIME_SEC"),col("IDENTITY_REQ_TIME_MSEC"),col("IDENTITY_EMD_TIME_SEC"),col("IDENTITY_EMD_TIME_MSEC"),col("IDENTITY_SUCCED_FLAG"),col("IDENTITY_RETRANS_FLAG"),col("IDENTITY_CAUSE"),col("AUTH_REQ_TIME_SEC"),col("AUTH_REQ_TIME_MSEC"),col("AUTH_END_TIME_SEC"),col("AUTH_END_TIME_MSEC"),col("AUTH_SUCCED_FLAG"),col("AUTH_RETRANS_FLAG"),col("AUTH_REJ_EMM_CAUSE"),col("SECURITY_MODE_TIME"),col("SECURITY_MODE_TIME_MSEC"),col("SECURITY_MODE_CMPT_TIME"),col("SECURITY_MODE_CMPT_TIME_MSEC"),col("SECURITY_MODE_SUCCED_FLAG"),col("SECURITY_MODE_RESELECT_FLAG"),col("SECURITY_MODE_CAUSE"),col("ESM_UE_TRANS_TYPE"),col("ESM_UE_REQ_TIME_SEC"),col("ESM_UE_REQ_TIME_MSEC"),col("ESM_UE_END_TIME_SEC"),col("ESM_UE_END_TIME_MSEC"),col("ESM_UE_SUCCED_FLAG"),col("ESM_UE_RETRANS_FLAG"),col("ESM_UE_REJ_CAUSE"),col("ESM_UE_EBI"),col("ESM_UE_LBI"),col("ESM_UE_ESMCAUSE"),col("ESM_NW_TRANS_TYPE"),col("ESM_NW_REQ_TIME_SEC"),col("ESM_NW_REQ_TIME_MSEC"),col("ESM_NW_END_TIME_SEC"),col("ESM_NW_END_TIME_MSEC"),col("ESM_NW_SUCCED_FLAG"),col("ESM_NW_RETRANS_FLAG"),col("ESM_NW_REJ_CAUSE"),col("ESM_NW_EBI"),col("ESM_NW_LBI"),col("ESM_NW_ESMCAUSE"),col("PAGING_REQ_TIME_SEC"),col("PAGING_REQ_TIME_MSEC"),col("PAGING_RSP_TIME_SEC"),col("PAGING_RSP_TIME_MSEC"),col("PAGING_REQ_SUCCED_FLAG"),col("PAGING_REQ_RETRANS_FLAG"),col("PAGING_REQ_TIMEOUT_FLAG"),col("S1AP_TRANS_TYPE"),col("S1AP_REQ_TIME_SEC"),col("S1AP_REQ_TIME_MSEC"),col("S1AP_END_TIME_SEC"),col("S1AP_END_TIME_MSEC"),col("S1AP_SUCCED_FLAG"),col("S1AP_RETRANS_FLAG"),col("S1AP_CAUSE_TYPE"),col("S1AP_CAUSE"),col("ERAB_COUNT_TO_BE_SETUP"),col("ERAB_COUNT_FAILED_SETUP"),col("ERAB_COUNT_SUCCEED_SETUP"),col("RESERVED1"),col("RESERVED2"),col("RESERVED3"),col("RESERVED4"),col("CSFB_IND"),col("S1AP_TRANS_DIRECTION"),col("LAYER1ID"),col("LAYER2ID"),col("LAYER3ID"),col("LAYER4ID"),col("LAYER5ID"),col("LAYER6ID"),col("UE_SRVCC_CAPABILITY"),col("CSFB_RESPONSE"),col("TMSI"),col("OLD_TMSI"),col("LAC"),col("CSCALL_TTIME"),col("CSCALL_TTIME_MS"),col("MBR_DL_NEG"),col("GBR_DL_NEG"),col("MBR_UL_NEG"),col("GBR_UL_NEG"),col("QCI_NEG"),col("ARP_NEG"),col("SV"),col("MMEGI"),col("MMEC"),col("ENB_UE_S1AP_ID"),col("MME_UE_S1AP_ID"),col("IMEI_CIPHERTEXT"),col("PREPAID_FLAG"),col("IMS_VOICE_SUPPORT"),col("BEAR_TFT"),col("BEAR_RADIO_PRIORITY"),col("BEAR_UL_APN_AMBR"),col("BEAR_DL_APN_AMBR"),col("UE_CONTEXT_STATUS"),col("UE_VOICE_PREFERENCE"),col("SESSIONKEY"),col("PDN_TYPE"),col("OLD_GUTI_TYPE"),col("USER_CATEGORY"),col("RLS_REQ_TYPE"),col("RLS_REQ_TIME_SEC"),col("RLS_REQ_TIME_MSEC"),col("RLS_TRANS_TYPE"),col("RLS_TRANS_REQ_TIME_SEC"),col("RLS_TRANS_REQ_TIME_MSEC"),col("RLS_TRANS_RSP_TIME_SEC"),col("RLS_TRANS_RSP_TIME_MSEC"),col("RLS_TRANS_SUCCED_FLAG"),col("RLS_TRANS_CAUSE_TYPE"),col("RLS_TRANS_CAUSE"),col("IMS_SERVICE_TYPE"),col("OLD_RAT"),col("c1").as("_corrupt_record"),col("year"),col("month"),col("day"),col("hour"))

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
