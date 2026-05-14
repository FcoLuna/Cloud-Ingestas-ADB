// Databricks notebook source
/*dbutils.widgets.text("moment","2024/11/05/15/0/")
dbutils.widgets.text("path_adls","abfss://bigdatadev@stbigdatadev02.dfs.core.windows.net")
dbutils.widgets.text("ruta_origen","/data/trafico/trafico_detalle/smartcare/landing_traf_cert/tdr_ims_inf_register_sip/")
dbutils.widgets.text("path_salida","/data/trafico/trafico_detalle/smartcare/landing_traf_cert/tdr_ims_inf_register_sip/")
dbutils.widgets.text("catalogo","bidesarrollo")
dbutils.widgets.text("tabla_salida","trafico_cert.smartcare_tdr_ims_inf_register_sip") //raw_trafico.aiumoc (cuando pase a prod)*/

// COMMAND ----------

val path_adls = dbutils.widgets.get("path_adls")
val catalogo = dbutils.widgets.get("catalogo")
val landing_tdr_ims_inf_register_sip = path_adls + dbutils.widgets.get("ruta_origen") 
val tabla_salida = catalogo +"."+ dbutils.widgets.get("tabla_salida")
val path_salida = path_adls + dbutils.widgets.get("path_salida")

// COMMAND ----------

val columnas = Seq("SID","REFID","ProbeID","CALLID","ICID","STARTTIME","MILLISEC","ENDTIME","SERVICE_TYPE","SERVICE_STATUS","INTERFACE","SOURCE_NE_TYPE","SOURCE_NE_IP","SOURCE_NE_PORT","SOURCR_NE_ID","DEST_NE_TYPE","DEST_NE_IP","DEST_NE_PORT","DEST_NE_ID","IMPI_TEL_URI","IMPI_SIP_URI","IMPU_TEL_URI1","IMPU_SIP_URI1","IMPU1_TYPE","IMPU_TEL_URI2","IMPU_SIP_URI2","IMPU2_TYPE","IMPU_TEL_URI3","IMPU_SIP_URI3","IMPU3_TYPE","IMPU_TEL_URI4","IMPU_SIP_URI4","IMPU4_TYPE","IMPU_TEL_URI5","IMPU_SIP_URI5","IMPU5_TYPE","IMEI","TERM_UNTRUST_IP_ADDR","TERM_UNTRUST_PORT","TERM_TRUST_IP_ADDR","TERM_TRUST_PORT","DEVICE_TYPE","ACCESS_TYPE","ACCESS_INFO","VISIT_DOMAIN","HOME_DOMAIN","AUTH_TYPE","EXPIRES_TIME_REQ","EXPIRES_TIME_RSP","RESPONSE_CODE","FINISH_WARNING","FINISH_REASON","FIRFAILTIME","FIRST_FAIL_NE_TYPE","FIRST_FAIL_NE_IP","AUTH_REQ_TIME","AUTH_RSP_TIME","RESERVED1","RESERVED2","RESERVED3","RESERVED4","RESERVED5","RESERVED6","RESERVED7","RESERVED8","FIRST_FAIL_NE_ID","TAI","USER_CATEGORY","LTE_RAN_NE_ID","Layer1ID","Layer2ID","Layer3ID","Layer4ID","Layer5ID","Layer6ID","AREA_CODE")
val tdr_ims_inf_register_sip = spark.read.option("delimiter","|").csv(landing_tdr_ims_inf_register_sip).toDF(columnas: _*)

// COMMAND ----------

val df = tdr_ims_inf_register_sip.
withColumn("year", date_format(from_unixtime(col("STARTTIME")), "yyyy")).
withColumn("month", date_format(from_unixtime(col("STARTTIME")), "MM")).
withColumn("day", date_format(from_unixtime(col("STARTTIME")), "dd")).
withColumn("hour", date_format(from_unixtime(col("STARTTIME")), "HH")).
select(col("SID"),col("REFID"),col("ProbeID"),col("CALLID"),col("ICID"),col("STARTTIME").cast("int"),col("MILLISEC"),col("ENDTIME").cast("int"),col("SERVICE_TYPE"),col("SERVICE_STATUS"),col("INTERFACE"),col("SOURCE_NE_TYPE"),col("SOURCE_NE_IP"),col("SOURCE_NE_PORT"),col("SOURCR_NE_ID"),col("DEST_NE_TYPE"),col("DEST_NE_IP"),col("DEST_NE_PORT"),col("DEST_NE_ID"),col("IMPI_TEL_URI"),col("IMPI_SIP_URI"),col("IMPU_TEL_URI1"),col("IMPU_SIP_URI1"),col("IMPU1_TYPE"),col("IMPU_TEL_URI2"),col("IMPU_SIP_URI2"),col("IMPU2_TYPE"),col("IMPU_TEL_URI3"),col("IMPU_SIP_URI3"),col("IMPU3_TYPE"),col("IMPU_TEL_URI4"),col("IMPU_SIP_URI4"),col("IMPU4_TYPE"),col("IMPU_TEL_URI5"),col("IMPU_SIP_URI5"),col("IMPU5_TYPE"),col("IMEI"),col("TERM_UNTRUST_IP_ADDR"),col("TERM_UNTRUST_PORT"),col("TERM_TRUST_IP_ADDR"),col("TERM_TRUST_PORT"),col("DEVICE_TYPE"),col("ACCESS_TYPE"),col("ACCESS_INFO"),col("VISIT_DOMAIN"),col("HOME_DOMAIN"),col("AUTH_TYPE"),col("EXPIRES_TIME_REQ"),col("EXPIRES_TIME_RSP"),col("RESPONSE_CODE"),col("FINISH_WARNING"),col("FINISH_REASON"),col("FIRFAILTIME"),col("FIRST_FAIL_NE_TYPE"),col("FIRST_FAIL_NE_IP"),col("AUTH_REQ_TIME"),col("AUTH_RSP_TIME"),col("RESERVED1"),col("RESERVED2"),col("RESERVED3"),col("RESERVED4"),col("RESERVED5"),col("RESERVED6"),col("RESERVED7"),col("RESERVED8"),col("FIRST_FAIL_NE_ID"),col("TAI"),col("USER_CATEGORY"),col("LTE_RAN_NE_ID"),col("Layer1ID"),col("Layer2ID"),col("Layer3ID"),col("Layer4ID"),col("Layer5ID"),col("Layer6ID"),col("AREA_CODE"),col("year"),col("month"),col("day"),col("hour"))


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
