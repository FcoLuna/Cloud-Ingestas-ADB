// Databricks notebook source
import org.apache.spark.sql.types.{StructType, StructField, StringType, IntegerType, ShortType, LongType}

// COMMAND ----------

/*
dbutils.widgets.text("path_adls","abfss://bigdatadev@stbigdatadev02.dfs.core.windows.net")
dbutils.widgets.text("ruta_origen","/data/trafico/trafico_detalle/smartcare/landing/cdr_volte_voice_quality/")
dbutils.widgets.text("path_salida","/data/trafico/trafico_detalle/smartcare/cdr_volte_voice_quality/raw/")
dbutils.widgets.text("catalogo","bi_ingestas")
dbutils.widgets.text("tabla_salida","trafico_cert.smartcare_cdr_volte_voice_quality")
*/

// COMMAND ----------

val path_adls = dbutils.widgets.get("path_adls")
val catalogo = dbutils.widgets.get("catalogo")
val landing_cdr_volte_voice_quality = path_adls + dbutils.widgets.get("ruta_origen")
val tabla_salida = catalogo +"."+ dbutils.widgets.get("tabla_salida")
val path_salida = path_adls + dbutils.widgets.get("path_salida")

// COMMAND ----------

val columnas = Seq("CDR_ID","REFID","STARTTIME","STARTTIME_MS","ENDTIME","ENDTIME_MS","MSISDN","IMSI","IMEI","APN","EGCI","LAST_EGCI","CALL_IDENTIFY","INTERFACE","QCI","CAllER_RAT_TYPE","CAllED_RAT_TYPE","CALLER_NO","CALLED_NO","SOURCE_IP","DESTINATION_IP","SOURCE_PORT","DESTINATION_PORT","CALL_DURATION","POST_DIAL_DELAY","CONN_LATENCY","FST_ENB_UE_S1AP_ID","FST_MME_UE_S1AP_ID","LAST_ENB_UE_S1AP_ID","LAST_MME_UE_S1AP_ID","FST_ENB_TEID","FST_SGW_TEID","LAST_ENB_TEID","LAST_SGW_TEID","RTP_Payload_Type","ULCODECRATE","DWCODECRATE","SSRC_CHANGE_TIMES","RTCP_RTP_PERIOD","UL_MOS_AVG","UL_RTCP_R_Factor","Ul_RTCP_DELAY_JITTER_DEDUCTION","RTCP_UL_DELAY_AVG","RTCP_UL_Jitter_AVG","RTCP_UL_PACKET_NUM"  ,"RTCP_UL_LOSSPACKET_NUM","DL_MOS_AVG","DW_RTCP_R_Factor","DW_RTCP_DELAY_JITTER_DEDUCTION","RTCP_DL_DELAY_AVG","RTCP_DL_Jitter_AVG","RTCP_DL_PACKET_NUM"  ,"RTCP_DL_LOSSPACKET_NUM","UL_MOS_BAD_NUM","RTCP_UL_MEAS_NUM","DL_MOS_BAD_NUM","RTCP_DL_PARA_MEAS_NUM","ONE_WAY_IDENTIFY","UL_ONE_WAY_NUM","DW_ONE_WAY_NUM","UL_IPMOS_AVG","UL_RTP_R_Factor","Ul_RTP_DELAY_JITTER_DEDUCTION","RTP_UL_DELAY_AVG","RTP_UL_JITTER_AVG","RTP_UL_PACKET_NUM","RTP_UL_LOSSPACKET_NUM","DL_IPMOS_AVG","DW_RTP_R_Factor","DW_RTP_DELAY_JITTER_DEDUCTION","RTP_DL_DELAY_AVG","RTP_DL_JITTER_AVG","RTP_DL_PACKET_NUM","RTP_DL_LOSSPACKET_NUM","UL_IPMOS_BAD_NUM","RTP_UL_PARA_MEAS_NUM","DL_IPMOS_BAD_NUM","RTP_DL_PARA_MEAS_NUM","RESERVED1","RESERVED2","RESERVED3","RESERVED4","RESERVED5","RESERVED6","RESERVED7","RESERVED8","RTCP_UL_ONE_WAY_NUM","RTCP_DW_ONE_WAY_NUM","FIRST_LTE_RAN_NE_ID","FIRST_LTE_RAN_NE_IP","LAST_LTE_RAN_NE_ID","LAST_LTE_RAN_NE_IP","MMEGI","MMEC","SGW_ID","SGW_IP","SBC_ID","Layer1ID","Layer2ID","Layer3ID","Layer4ID","Layer5ID","Layer6ID","ProbeID","UL_RTP_ENDTIME_MS","DL_RTP_ENDTIME_MS","SIG_STARTTIME","UL_CMRREQ_FREQ_NUM","DL_CMRREQ_FREQ_NUM","HO_OUT_REQ","HO_OUT_SUCC","SESSIONKEY","FIRST_UCELLID","LAST_UCELLID","FIRST_RASTERLONGITUDE","FIRST_RASTERLATITUDE","FIRST_RASTERALTITUDE","LAST_RASTERLONGITUDE","LAST_RASTERLATITUDE","LAST_RASTERALTITUDE","FAIL_CLASS","USER_CATEGORY","PGW_ID","PGW_IP","MGW_ID","MGW_IP","SRVCC_FLAG","PEER_CALL_TYPE","RTP_UL_PACKET_NUM_LAST","RTP_UL_LOSSPACKT_NUM_LAST","RTP_DL_PACKET_NUM_LAST","RTP_DL_LOSSPACKET_NUM_LAST","PT_CHANGE_FLAG","UL_VOICE_BREAK_NUM","UL_WORDS_SWALLOW_NUM","DL_VOICE_BREAK_NUM","DL_WORDS_SWALLOW_NUM","Service_Type","UL_AVG_BITRATE","DL_AVG_BITRATE","UL_Resolution","DL_Resolution","UL_MOS_TOTAL","DL_MOS_TOTAL","UL_VOICE_FRAME_NUM","RTP_UL_VOICE_FRAME_LOST_NUM","RTCP_UL_VOICE_FRAME_LOST_NUM","DL_VOICE_FRAME_NUM","RTP_DL_VOICE_FRAME_LOST_NUM","RTCP_DL_VOICE_FRAME_LOST_NUM","UL_VIDEO_FRAME_RATE","DL_VIDEO_FRAME_RATE","Video_Fallback","Profile_Level_ID_UL","Profile_Level_ID_DL","FAIL_CATEGORY","RULE_CODE","USER_TYPE","SV","CUSTOMER_TYPE","DEST_NUM_ATTR","DEST_OPRID","DEST_CC","CALL_HOLD","UP_TRAFFIC","DOWN_TRAFFIC","_corrupt_record")
val cdr_volte_voice_quality = spark.read.option("delimiter","|").csv(landing_cdr_volte_voice_quality).toDF(columnas: _*)

// COMMAND ----------

val df = cdr_volte_voice_quality.
withColumn("year", date_format(from_unixtime(col("STARTTIME")), "yyyy")).
withColumn("month", date_format(from_unixtime(col("STARTTIME")), "MM")).
withColumn("day", date_format(from_unixtime(col("STARTTIME")), "dd")).
withColumn("hour", date_format(from_unixtime(col("STARTTIME")), "HH")).
select(col("CDR_ID"),col("REFID"),col("STARTTIME"),col("STARTTIME_MS"),col("ENDTIME"),col("ENDTIME_MS"),col("MSISDN"),col("IMSI"),col("IMEI"),col("APN"),col("EGCI"),col("LAST_EGCI"),col("CALL_IDENTIFY"),col("INTERFACE"),col("QCI"),col("CAllER_RAT_TYPE"),col("CAllED_RAT_TYPE"),col("CALLER_NO"),col("CALLED_NO"),col("SOURCE_IP"),col("DESTINATION_IP"),col("SOURCE_PORT"),col("DESTINATION_PORT"),col("CALL_DURATION"),col("POST_DIAL_DELAY"),col("CONN_LATENCY"),col("FST_ENB_UE_S1AP_ID"),col("FST_MME_UE_S1AP_ID"),col("LAST_ENB_UE_S1AP_ID"),col("LAST_MME_UE_S1AP_ID"),col("FST_ENB_TEID"),col("FST_SGW_TEID"),col("LAST_ENB_TEID"),col("LAST_SGW_TEID"),col("RTP_Payload_Type"),col("ULCODECRATE"),col("DWCODECRATE"),col("SSRC_CHANGE_TIMES"),col("RTCP_RTP_PERIOD"),col("UL_MOS_AVG"),col("UL_RTCP_R_Factor"),col("Ul_RTCP_DELAY_JITTER_DEDUCTION"),col("RTCP_UL_DELAY_AVG"),col("RTCP_UL_Jitter_AVG"),col("RTCP_UL_PACKET_NUM")  ,col("RTCP_UL_LOSSPACKET_NUM"),col("DL_MOS_AVG"),col("DW_RTCP_R_Factor"),col("DW_RTCP_DELAY_JITTER_DEDUCTION"),col("RTCP_DL_DELAY_AVG"),col("RTCP_DL_Jitter_AVG"),col("RTCP_DL_PACKET_NUM")  ,col("RTCP_DL_LOSSPACKET_NUM"),col("UL_MOS_BAD_NUM"),col("RTCP_UL_MEAS_NUM"),col("DL_MOS_BAD_NUM"),col("RTCP_DL_PARA_MEAS_NUM"),col("ONE_WAY_IDENTIFY"),col("UL_ONE_WAY_NUM"),col("DW_ONE_WAY_NUM"),col("UL_IPMOS_AVG"),col("UL_RTP_R_Factor"),col("Ul_RTP_DELAY_JITTER_DEDUCTION"),col("RTP_UL_DELAY_AVG"),col("RTP_UL_JITTER_AVG"),col("RTP_UL_PACKET_NUM"),col("RTP_UL_LOSSPACKET_NUM"),col("DL_IPMOS_AVG"),col("DW_RTP_R_Factor"),col("DW_RTP_DELAY_JITTER_DEDUCTION"),col("RTP_DL_DELAY_AVG"),col("RTP_DL_JITTER_AVG"),col("RTP_DL_PACKET_NUM"),col("RTP_DL_LOSSPACKET_NUM"),col("UL_IPMOS_BAD_NUM"),col("RTP_UL_PARA_MEAS_NUM"),col("DL_IPMOS_BAD_NUM"),col("RTP_DL_PARA_MEAS_NUM"),col("RESERVED1"),col("RESERVED2"),col("RESERVED3"),col("RESERVED4"),col("RESERVED5"),col("RESERVED6"),col("RESERVED7"),col("RESERVED8"),col("RTCP_UL_ONE_WAY_NUM"),col("RTCP_DW_ONE_WAY_NUM"),col("FIRST_LTE_RAN_NE_ID"),col("FIRST_LTE_RAN_NE_IP"),col("LAST_LTE_RAN_NE_ID"),col("LAST_LTE_RAN_NE_IP"),col("MMEGI"),col("MMEC"),col("SGW_ID"),col("SGW_IP"),col("SBC_ID"),col("Layer1ID"),col("Layer2ID"),col("Layer3ID"),col("Layer4ID"),col("Layer5ID"),col("Layer6ID"),col("ProbeID"),col("UL_RTP_ENDTIME_MS"),col("DL_RTP_ENDTIME_MS"),col("SIG_STARTTIME"),col("UL_CMRREQ_FREQ_NUM"),col("DL_CMRREQ_FREQ_NUM"),col("HO_OUT_REQ"),col("HO_OUT_SUCC"),col("SESSIONKEY"),col("FIRST_UCELLID"),col("LAST_UCELLID"),col("FIRST_RASTERLONGITUDE"),col("FIRST_RASTERLATITUDE"),col("FIRST_RASTERALTITUDE"),col("LAST_RASTERLONGITUDE"),col("LAST_RASTERLATITUDE"),col("LAST_RASTERALTITUDE"),col("FAIL_CLASS"),col("USER_CATEGORY"),col("PGW_ID"),col("PGW_IP"),col("MGW_ID"),col("MGW_IP"),col("SRVCC_FLAG"),col("PEER_CALL_TYPE"),col("RTP_UL_PACKET_NUM_LAST"),col("RTP_UL_LOSSPACKT_NUM_LAST"),col("RTP_DL_PACKET_NUM_LAST"),col("RTP_DL_LOSSPACKET_NUM_LAST"),col("PT_CHANGE_FLAG"),col("UL_VOICE_BREAK_NUM"),col("UL_WORDS_SWALLOW_NUM"),col("DL_VOICE_BREAK_NUM"),col("DL_WORDS_SWALLOW_NUM"),col("Service_Type"),col("UL_AVG_BITRATE"),col("DL_AVG_BITRATE"),col("UL_Resolution"),col("DL_Resolution"),col("UL_MOS_TOTAL"),col("DL_MOS_TOTAL"),col("UL_VOICE_FRAME_NUM"),col("RTP_UL_VOICE_FRAME_LOST_NUM"),col("RTCP_UL_VOICE_FRAME_LOST_NUM"),col("DL_VOICE_FRAME_NUM"),col("RTP_DL_VOICE_FRAME_LOST_NUM"),col("RTCP_DL_VOICE_FRAME_LOST_NUM"),col("UL_VIDEO_FRAME_RATE"),col("DL_VIDEO_FRAME_RATE"),col("Video_Fallback"),col("Profile_Level_ID_UL"),col("Profile_Level_ID_DL"),col("FAIL_CATEGORY"),col("RULE_CODE"),col("USER_TYPE"),col("SV"),col("CUSTOMER_TYPE"),col("DEST_NUM_ATTR"),col("DEST_OPRID"),col("DEST_CC"),col("CALL_HOLD"),col("UP_TRAFFIC"),col("DOWN_TRAFFIC"),col("_corrupt_record"),col("year"),col("month"),col("day"),col("hour"))

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

// COMMAND ----------

dbutils.fs.rm(landing_cdr_volte_voice_quality, true)
