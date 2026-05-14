// Databricks notebook source
/*dbutils.widgets.text("moment","2024/11/05/15/")
dbutils.widgets.text("path_adls","abfss://bigdatadev@stbigdatadev02.dfs.core.windows.net")
dbutils.widgets.text("ruta_origen","/data/trafico/trafico_detalle/smartcare/landing_traf_cert/cdr_ims_inf_call_sip/")
dbutils.widgets.text("path_salida","/data/trafico/trafico_detalle/smartcare/landing_traf_cert/cdr_ims_inf_call_sip/")
dbutils.widgets.text("catalogo","bidesarrollo")
dbutils.widgets.text("tabla_salida","trafico_cert.smartcare_cdr_ims_inf_call_sip") //raw_trafico.aiumoc (cuando pase a prod)*/

// COMMAND ----------

val path_adls = dbutils.widgets.get("path_adls")
val catalogo = dbutils.widgets.get("catalogo")
val landing_cdr_ims_inf_call_sip = path_adls + dbutils.widgets.get("ruta_origen")
val tabla_salida = catalogo +"."+ dbutils.widgets.get("tabla_salida")
val path_salida = path_adls + dbutils.widgets.get("path_salida")

// COMMAND ----------

val columnas = Seq("SID","REFID","ProbeID","CALLID","ICID","STARTTIME","MILLISEC","ENDTIME","SERVICE_TYPE","SERVICE_STATUS","INTERFACE","SOURCE_NE_TYPE","SOURCE_NE_IP","SOURCE_NE_PORT","SOURCR_NE_ID","DEST_NE_TYPE","DEST_NE_IP","DEST_NE_PORT","DEST_NE_ID","CALL_SIDE","IMPI_TEL_URI","IMPI_SIP_URI","IMPU_TEL_URI","IMPU_SIP_URI","IMPU_TYPE","IMEI","TERM_UNTRUST_IP_ADDR","TERM_UNTRUST_PORT","TERM_TRUST_IP_ADDR","TERM_TRUST_PORT","DEVICE_TYPE","ACCESS_TYPE","ACCESS_INFO","VISIT_DOMAIN","HOME_DOMAIN","CALLING_PARTY_ADDRESS","CALLING_PARTY_TYPE","CALLING_PARTY_SIP_URI","DIAL_NUMBER","DIAL_NUMBER_TYPE","DIAL_NUMBER_SIP_URI","CALLED_PARTY_ADDRESS","CALLED_PARTY_TYPE","CALLED_PARTY_SIP_URI","CALLING_AUDIO_SDP_IP_ADDR","CALLING_AUDIO_SDP_PORT","CALLED_AUDIO_SDP_IP_ADDR","CALLED_AUDIO_SDP_PORT","AUDIO_CODEC","CALLING_VIDEO_SDP_IP_ADDR","CALLING_VIDEO_SDP_PORT","CALLED_VIDEO_IP_ADDR","CALLED_VIDEO_PORT","VIDEO_CODEC","CALLING_ADDR_IDENTITY","CALLED_ADDR_IDENTITY","REDIRECTING_PARTY_ADDRESS","ORIGINAL_PARTY_ADDRESS","REDIRECT_REASON","CALL_DIRECT_NUMBER","ONLY_MAIN_NUMBER","CONF_URI","SESSION_TERMINATE_FLAG","RELEASE_EARLY","RESPONSE_CODE","Q850_Cause","FINISH_WARNING","FINISH_REASON","FIRFAILTIME","FIRST_FAIL_NE_TYPE","FIRST_FAIL_NE_IP","FIRST_FAIL_NE_NI","FIRST_FAIL_NE_PC","ALERTING_TIME","ANSWER_TIME","RELEASE_TIME","CALL_DURATION","RESERVED1","RESERVED2","RESERVED3","RESERVED4","RESERVED5","RESERVED6","RESERVED7","RESERVED8","RELEASE_MESSAGE","UE_ABILITY_FLAG","IWF_ABILITY_FLAG","FIRST_FAIL_NE_ID","TAI","CTX_GROUP_NO","CTX_SUBGROUP_NO","CTX_PRIVATE_NUMBER","CTX_CALL_TYPE","PLAY_TONE","PLAY_TONE_CAUSE","PLAY_TONE_PROTOCOL","CS_RETRY_DELAY","TRIGGER_CS_RETRY_MSG","USER_CATEGORY","SIP_183_DELAY","PRACK_DELAY","PRACK_200_OK_DELAY","UPDATE_DELAY","CALLER_HIDE_FLAG","LTE_RAN_NE_ID","Layer1ID","Layer2ID","Layer3ID","Layer4ID","Layer5ID","Layer6ID","AREA_CODE","REDIRECT_COUNTER","CONF_USER_TYPE","SRV_ATTR_ID","FAIL_CATEGORY","RULE_CODE","USER_TYPE","_corrupt_record")
val cdr_ims_inf_call_sip = spark.read.option("delimiter","|").csv(landing_cdr_ims_inf_call_sip).toDF(columnas: _*)


// COMMAND ----------

val df = cdr_ims_inf_call_sip.
withColumn("year", date_format(from_unixtime(col("STARTTIME")), "yyyy")).
withColumn("month", date_format(from_unixtime(col("STARTTIME")), "MM")).
withColumn("day", date_format(from_unixtime(col("STARTTIME")), "dd")).
withColumn("hour", date_format(from_unixtime(col("STARTTIME")), "HH")).
select(col("SID"),col("REFID"),col("ProbeID"),col("CALLID"),col("ICID"),col("STARTTIME").cast("int"),col("MILLISEC"),col("ENDTIME").cast("int"),col("SERVICE_TYPE"),col("SERVICE_STATUS"),col("INTERFACE"),col("SOURCE_NE_TYPE"),col("SOURCE_NE_IP"),col("SOURCE_NE_PORT"),col("SOURCR_NE_ID"),col("DEST_NE_TYPE"),col("DEST_NE_IP"),col("DEST_NE_PORT"),col("DEST_NE_ID"),col("CALL_SIDE"),col("IMPI_TEL_URI"),col("IMPI_SIP_URI"),col("IMPU_TEL_URI"),col("IMPU_SIP_URI"),col("IMPU_TYPE"),col("IMEI"),col("TERM_UNTRUST_IP_ADDR"),col("TERM_UNTRUST_PORT"),col("TERM_TRUST_IP_ADDR"),col("TERM_TRUST_PORT"),col("DEVICE_TYPE"),col("ACCESS_TYPE"),col("ACCESS_INFO"),col("VISIT_DOMAIN"),col("HOME_DOMAIN"),col("CALLING_PARTY_ADDRESS"),col("CALLING_PARTY_TYPE"),col("CALLING_PARTY_SIP_URI"),col("DIAL_NUMBER"),col("DIAL_NUMBER_TYPE"),col("DIAL_NUMBER_SIP_URI"),col("CALLED_PARTY_ADDRESS"),col("CALLED_PARTY_TYPE"),col("CALLED_PARTY_SIP_URI"),col("CALLING_AUDIO_SDP_IP_ADDR"),col("CALLING_AUDIO_SDP_PORT"),col("CALLED_AUDIO_SDP_IP_ADDR"),col("CALLED_AUDIO_SDP_PORT"),col("AUDIO_CODEC"),col("CALLING_VIDEO_SDP_IP_ADDR"),col("CALLING_VIDEO_SDP_PORT"),col("CALLED_VIDEO_IP_ADDR"),col("CALLED_VIDEO_PORT"),col("VIDEO_CODEC"),col("CALLING_ADDR_IDENTITY"),col("CALLED_ADDR_IDENTITY"),col("REDIRECTING_PARTY_ADDRESS"),col("ORIGINAL_PARTY_ADDRESS"),col("REDIRECT_REASON"),col("CALL_DIRECT_NUMBER"),col("ONLY_MAIN_NUMBER"),col("CONF_URI"),col("SESSION_TERMINATE_FLAG"),col("RELEASE_EARLY"),col("RESPONSE_CODE"),col("Q850_Cause"),col("FINISH_WARNING"),col("FINISH_REASON"),col("FIRFAILTIME"),col("FIRST_FAIL_NE_TYPE"),col("FIRST_FAIL_NE_IP"),col("FIRST_FAIL_NE_NI"),col("FIRST_FAIL_NE_PC"),col("ALERTING_TIME"),col("ANSWER_TIME"),col("RELEASE_TIME"),col("CALL_DURATION"),col("RESERVED1"),col("RESERVED2"),col("RESERVED3"),col("RESERVED4"),col("RESERVED5"),col("RESERVED6"),col("RESERVED7"),col("RESERVED8"),col("RELEASE_MESSAGE"),col("UE_ABILITY_FLAG"),col("IWF_ABILITY_FLAG"),col("FIRST_FAIL_NE_ID"),col("TAI"),col("CTX_GROUP_NO"),col("CTX_SUBGROUP_NO"),col("CTX_PRIVATE_NUMBER"),col("CTX_CALL_TYPE"),col("PLAY_TONE"),col("PLAY_TONE_CAUSE"),col("PLAY_TONE_PROTOCOL"),col("CS_RETRY_DELAY"),col("TRIGGER_CS_RETRY_MSG"),col("USER_CATEGORY"),col("SIP_183_DELAY"),col("PRACK_DELAY"),col("PRACK_200_OK_DELAY"),col("UPDATE_DELAY"),col("CALLER_HIDE_FLAG"),col("LTE_RAN_NE_ID"),col("Layer1ID"),col("Layer2ID"),col("Layer3ID"),col("Layer4ID"),col("Layer5ID"),col("Layer6ID"),col("AREA_CODE"),col("REDIRECT_COUNTER"),col("CONF_USER_TYPE"),col("SRV_ATTR_ID"),col("FAIL_CATEGORY"),col("RULE_CODE"),col("USER_TYPE"),col("_corrupt_record"),col("year"),col("month"),col("day"),col("hour"))


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
      mode("append").
      format("delta").
      option("path", path_salida).
      insertInto(tabla_salida)
}
