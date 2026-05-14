// Databricks notebook source
/*dbutils.widgets.text("moment","2024/11/05/15/")
dbutils.widgets.text("path_adls","abfss://bigdatadev@stbigdatadev02.dfs.core.windows.net")
dbutils.widgets.text("ruta_origen","/data/trafico/trafico_detalle/smartcare/landing_traf_cert/cdr_ims_call_delay")
dbutils.widgets.text("path_salida","/data/trafico/trafico_detalle/smartcare/landing_traf_cert/cdr_ims_call_delay/")
dbutils.widgets.text("catalogo","bidesarrollo")
dbutils.widgets.text("tabla_salida","trafico_cert.smartcare_cdr_ims_call_delay") //raw_trafico.aiumoc (cuando pase a prod)*/

// COMMAND ----------

val path_adls = dbutils.widgets.get("path_adls")
val catalogo = dbutils.widgets.get("catalogo")
val landing_cdr_ims_call_delay = path_adls + dbutils.widgets.get("ruta_origen")
val tabla_salida = catalogo +"."+ dbutils.widgets.get("tabla_salida")
val path_salida = path_adls + dbutils.widgets.get("path_salida")

// COMMAND ----------

val columnas = Seq("STARTTIME","MILLISEC","ENDTIME","SERVICE_TYPE","SERVICE_STATUS","CALL_SIDE","CALLER_IMPI_TEL_URI","CALLER_IMPU_TEL_URI","CALLER_IMEI","CALLED_IMPI_TEL_URI","CALLED_IMPU_TEL_URI","CALLED_IMEI","CALLER_FIRST_LTE_ECGI","CALLED_FIRST_LTE_ECGI","CALLER_Layer1ID","CALLER_Layer2ID","CALLER_Layer3ID","CALLED_Layer1ID","CALLED_Layer2ID","CALLED_Layer3ID","CALLER_ACCESS_TYPE","CALLED_ACCESS_TYPE","CALLER_CALL_TYPE","CALLED_CALL_TYPE","CALLED_CALL_SUB_TYPE","CALLED_REDIRECTING_FLAG","SIP_183_TIME","PRACK_TIME","ALERTING_TIME","ANSWER_TIME","CALLER_SBC_IP","CALLER_SBC_ID","CALLER_SBC_INVITE_TIME","CALLER_SBC_180_TIME","CALLER_SCCAS_IP","CALLER_SCCAS_ID","CALLER_SCCAS_INVITE_DELAY","CALLER_SCPAS_IP","CALLER_SCPAS_ID","CALLER_SCPAS_INVITE_DELAY","CALLED_SCSCF_IP","CALLED_SCSCF_ID","CALLED_SCSCF_INVITE_TIME","CALLED_SCSCF_180_TIME","CALLED_SCPAS_IP","CALLED_SCPAS_ID","CALLED_SCPAS_INVITE_DELAY","CALLED_CATAS_IP","CALLED_CATAS_ID","CALLED_CATAS_INVITE_DELAY","CALLED_CATAS_180_DELAY","CALLED_SCCAS_IP","CALLED_SCCAS_ID","CALLED_SCCAS_INVITE_DELAY","CALLED_SBC_IP","CALLED_SBC_ID","CALLED_SBC_INVITE_TIME","CALLED_SBC_180_TIME","CALLED_MMTELAS_IP","CALLED_MMTELAS_ID","CALLED_MMTELAS_180_DELAY","CALLED_CALLSIGNAS_IP","CALLED_CALLSIGNAS_ID","CALLED_CALLSIGNAS_180_DELAY","CALLED_MW2SCPAS_180_DELAY","CALLED_SCCAS_180_DELAY","CALLED_SCPAS_180_DELAY","CALLED_MW2GM_180_DELAY","CALLED_SCCAS2MI_180_DELAY","CALLED_BGCF_IP","CALLED_BGCF_ID","CALLED_SCCAS2MJ_180_DELAY","CALLED_MGCF_IP","CALLED_MGCF_ID","CALLED_MJ2NC_180_DELAY","CALLED_NC2MG_180_DELAY","CALLED_MG2SCPAS_180_DELAY","CALLED_MG2SCCAS_180_DELAY","CALLED_CX_LIR_DELAY","CALLED_SCCAS_UDR_DELAY","CALLED_SCPAS_UDR_DELAY","CALLED_MMTELAS_UDR_DELAY","CALLED_CALLSIGNAS_UDR_DELAY","CALLED_CATAS_UDR_DELAY","CALLED_RX_AAR_DELAY","CALLED_RX_RAR_DELAY","CALLER_MMTELAS_IP","CALLER_MMTELAS_ID","CALLER_MMTELAS_180_DELAY","CALLER_CALLSIGNAS_IP","CALLER_CALLSIGNAS_ID","CALLER_CALLSIGNAS_180_DELAY","CALLER_RX_AAR_DELAY","CALLER_RX_RAR_DELAY","CALLER_GM_180_DELAY","CALLER_GM2MW_180_DELAY","CALLER_SCSCF_IP","CALLER_SCSCF_ID","CALLER_MW2SCCAS_180_DELAY","CALLER_SCCAS_180_DELAY","CALLER_SCPAS_180_DELAY","CALLER_Layer4ID","CALLER_Layer5ID","CALLER_Layer6ID","CALLED_Layer4ID","CALLED_Layer5ID","CALLED_Layer6ID","CALLER_SRV_ATTR_ID","CALLED_SRV_ATTR_ID","FAIL_CATEGORY","RULE_CODE","CALLER_PCSCF_IP","CALLER_PCSCF_ID","CALLED_PCSCF_IP","CALLED_PCSCF_ID","CALLER_GM2MW_","INVITE_DELAY","_corrupt_record")
val cdr_ims_call_delay = spark.read.option("delimiter","|").csv(landing_cdr_ims_call_delay).toDF(columnas: _*)


// COMMAND ----------

val df = cdr_ims_call_delay.
withColumn("year", date_format(from_unixtime(col("STARTTIME")), "yyyy")).
withColumn("month", date_format(from_unixtime(col("STARTTIME")), "MM")).
withColumn("day", date_format(from_unixtime(col("STARTTIME")), "dd")).
withColumn("hour", date_format(from_unixtime(col("STARTTIME")), "HH")).
select(col("STARTTIME").cast("int"),col("MILLISEC"),col("ENDTIME").cast("int"),col("SERVICE_TYPE"),col("SERVICE_STATUS"),col("CALL_SIDE"),col("CALLER_IMPI_TEL_URI"),col("CALLER_IMPU_TEL_URI"),col("CALLER_IMEI"),col("CALLED_IMPI_TEL_URI"),col("CALLED_IMPU_TEL_URI"),col("CALLED_IMEI"),col("CALLER_FIRST_LTE_ECGI"),col("CALLED_FIRST_LTE_ECGI"),col("CALLER_Layer1ID"),col("CALLER_Layer2ID"),col("CALLER_Layer3ID"),col("CALLED_Layer1ID"),col("CALLED_Layer2ID"),col("CALLED_Layer3ID"),col("CALLER_ACCESS_TYPE"),col("CALLED_ACCESS_TYPE"),col("CALLER_CALL_TYPE"),col("CALLED_CALL_TYPE"),col("CALLED_CALL_SUB_TYPE"),col("CALLED_REDIRECTING_FLAG"),col("SIP_183_TIME"),col("PRACK_TIME"),col("ALERTING_TIME"),col("ANSWER_TIME"),col("CALLER_SBC_IP"),col("CALLER_SBC_ID"),col("CALLER_SBC_INVITE_TIME"),col("CALLER_SBC_180_TIME"),col("CALLER_SCCAS_IP"),col("CALLER_SCCAS_ID"),col("CALLER_SCCAS_INVITE_DELAY"),col("CALLER_SCPAS_IP"),col("CALLER_SCPAS_ID"),col("CALLER_SCPAS_INVITE_DELAY"),col("CALLED_SCSCF_IP"),col("CALLED_SCSCF_ID"),col("CALLED_SCSCF_INVITE_TIME"),col("CALLED_SCSCF_180_TIME"),col("CALLED_SCPAS_IP"),col("CALLED_SCPAS_ID"),col("CALLED_SCPAS_INVITE_DELAY"),col("CALLED_CATAS_IP"),col("CALLED_CATAS_ID"),col("CALLED_CATAS_INVITE_DELAY"),col("CALLED_CATAS_180_DELAY"),col("CALLED_SCCAS_IP"),col("CALLED_SCCAS_ID"),col("CALLED_SCCAS_INVITE_DELAY"),col("CALLED_SBC_IP"),col("CALLED_SBC_ID"),col("CALLED_SBC_INVITE_TIME"),col("CALLED_SBC_180_TIME"),col("CALLED_MMTELAS_IP"),col("CALLED_MMTELAS_ID"),col("CALLED_MMTELAS_180_DELAY"),col("CALLED_CALLSIGNAS_IP"),col("CALLED_CALLSIGNAS_ID"),col("CALLED_CALLSIGNAS_180_DELAY"),col("CALLED_MW2SCPAS_180_DELAY"),col("CALLED_SCCAS_180_DELAY"),col("CALLED_SCPAS_180_DELAY"),col("CALLED_MW2GM_180_DELAY"),col("CALLED_SCCAS2MI_180_DELAY"),col("CALLED_BGCF_IP"),col("CALLED_BGCF_ID"),col("CALLED_SCCAS2MJ_180_DELAY"),col("CALLED_MGCF_IP"),col("CALLED_MGCF_ID"),col("CALLED_MJ2NC_180_DELAY"),col("CALLED_NC2MG_180_DELAY"),col("CALLED_MG2SCPAS_180_DELAY"),col("CALLED_MG2SCCAS_180_DELAY"),col("CALLED_CX_LIR_DELAY"),col("CALLED_SCCAS_UDR_DELAY"),col("CALLED_SCPAS_UDR_DELAY"),col("CALLED_MMTELAS_UDR_DELAY"),col("CALLED_CALLSIGNAS_UDR_DELAY"),col("CALLED_CATAS_UDR_DELAY"),col("CALLED_RX_AAR_DELAY"),col("CALLED_RX_RAR_DELAY"),col("CALLER_MMTELAS_IP"),col("CALLER_MMTELAS_ID"),col("CALLER_MMTELAS_180_DELAY"),col("CALLER_CALLSIGNAS_IP"),col("CALLER_CALLSIGNAS_ID"),col("CALLER_CALLSIGNAS_180_DELAY"),col("CALLER_RX_AAR_DELAY"),col("CALLER_RX_RAR_DELAY"),col("CALLER_GM_180_DELAY"),col("CALLER_GM2MW_180_DELAY"),col("CALLER_SCSCF_IP"),col("CALLER_SCSCF_ID"),col("CALLER_MW2SCCAS_180_DELAY"),col("CALLER_SCCAS_180_DELAY"),col("CALLER_SCPAS_180_DELAY"),col("CALLER_Layer4ID"),col("CALLER_Layer5ID"),col("CALLER_Layer6ID"),col("CALLED_Layer4ID"),col("CALLED_Layer5ID"),col("CALLED_Layer6ID"),col("CALLER_SRV_ATTR_ID"),col("CALLED_SRV_ATTR_ID"),col("FAIL_CATEGORY"),col("RULE_CODE"),col("CALLER_PCSCF_IP"),col("CALLER_PCSCF_ID"),col("CALLED_PCSCF_IP"),col("CALLED_PCSCF_ID"),col("CALLER_GM2MW_"), col("INVITE_DELAY"),col("_corrupt_record"),col("year"),col("month"),col("day"),col("hour"))


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
