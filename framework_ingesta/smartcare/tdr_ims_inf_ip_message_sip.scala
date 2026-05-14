// Databricks notebook source
/*dbutils.widgets.text("moment","2024/11/05/15/")
dbutils.widgets.text("path_adls","abfss://bigdatadev@stbigdatadev02.dfs.core.windows.net")
dbutils.widgets.text("ruta_origen","/data/trafico/trafico_detalle/smartcare/landing_traf_cert/tdr_ims_inf_ip_message_sip/")
dbutils.widgets.text("path_salida","/data/trafico/trafico_detalle/smartcare/landing_traf_cert/tdr_ims_inf_ip_message_sip/")
dbutils.widgets.text("catalogo","bidesarrollo")
dbutils.widgets.text("tabla_salida","trafico_cert.smartcare_tdr_ims_inf_ip_message_sip") //raw_trafico.aiumoc (cuando pase a prod)*/

// COMMAND ----------

val path_adls = dbutils.widgets.get("path_adls")
val catalogo = dbutils.widgets.get("catalogo")
val landing_tdr_ims_inf_ip_message_sip = path_adls + dbutils.widgets.get("ruta_origen")
val tabla_salida = catalogo +"."+ dbutils.widgets.get("tabla_salida")
val path_salida = path_adls + dbutils.widgets.get("path_salida")

// COMMAND ----------

val columnas = Seq("STARTTIME","MILLISEC","ENDTIME","SERVICE_TYPE","SERVICE_STATUS","INTERFACE","SOURCE_NE_TYPE","SOURCE_NE_IP","SOURCE_NE_PORT","SOURCR_NE_ID","DEST_NE_TYPE","DEST_NE_IP","DEST_NE_PORT","DEST_NE_ID","IMPI_TEL_URI","IMPI_SIP_URI","IMPU_TEL_URI","IMPU_SIP_URI","IMPU_TYPE","IMEI","TERM_UNTRUST_IP_ADDR","TERM_UNTRUST_PORT","TERM_TRUST_IP_ADDR","TERM_TRUST_PORT","DEVICE_TYPE","ACCESS_TYPE","ACCESS_INFO","VISIT_DOMAIN","HOME_DOMAIN","CALLING_PARTY_ADDRESS","CALLING_PARTY_TYPE","CALLING_SIP_URI","CALLED_PARTY_ADDRESS","CALLED_PARTY_TYPE","CALLED_SIP_URI","SMSC_ADDR","MESSAGE_LENGTH","RESPONSE_CODE","RP_ERROR","TP_ERROR","FINISH_WARNING","FINISH_REASON","FIRFAILTIME","FIRST_FAIL_NE_TYPE","FIRST_FAIL_NE_IP","FIRST_FAIL_NE_NI","FIRST_FAIL_NE_PC","RESERVED1","RESERVED2","RESERVED3")
val tdr_ims_inf_ip_message_sip = spark.read.option("delimiter","|").csv(landing_tdr_ims_inf_ip_message_sip).toDF(columnas: _*)

// COMMAND ----------

val df = tdr_ims_inf_ip_message_sip.
withColumn("year", date_format(from_unixtime(col("STARTTIME")), "yyyy")).
withColumn("month", date_format(from_unixtime(col("STARTTIME")), "MM")).
withColumn("day", date_format(from_unixtime(col("STARTTIME")), "dd")).
withColumn("hour", date_format(from_unixtime(col("STARTTIME")), "HH")).
select(col("STARTTIME").cast("int"),col("MILLISEC"),col("ENDTIME").cast("int"),col("SERVICE_TYPE"),col("SERVICE_STATUS"),col("INTERFACE"),col("SOURCE_NE_TYPE"),col("SOURCE_NE_IP"),col("SOURCE_NE_PORT"),col("SOURCR_NE_ID"),col("DEST_NE_TYPE"),col("DEST_NE_IP"),col("DEST_NE_PORT"),col("DEST_NE_ID"),col("IMPI_TEL_URI"),col("IMPI_SIP_URI"),col("IMPU_TEL_URI"),col("IMPU_SIP_URI"),col("IMPU_TYPE"),col("IMEI"),col("TERM_UNTRUST_IP_ADDR"),col("TERM_UNTRUST_PORT"),col("TERM_TRUST_IP_ADDR"),col("TERM_TRUST_PORT"),col("DEVICE_TYPE"),col("ACCESS_TYPE"),col("ACCESS_INFO"),col("VISIT_DOMAIN"),col("HOME_DOMAIN"),col("CALLING_PARTY_ADDRESS"),col("CALLING_PARTY_TYPE"),col("CALLING_SIP_URI"),col("CALLED_PARTY_ADDRESS"),col("CALLED_PARTY_TYPE"),col("CALLED_SIP_URI"),col("SMSC_ADDR"),col("MESSAGE_LENGTH"),col("RESPONSE_CODE"),col("RP_ERROR"),col("TP_ERROR"),col("FINISH_WARNING"),col("FINISH_REASON"),col("FIRFAILTIME"),col("FIRST_FAIL_NE_TYPE"),col("FIRST_FAIL_NE_IP"),col("FIRST_FAIL_NE_NI"),col("FIRST_FAIL_NE_PC"),col("RESERVED1"),col("RESERVED2"),col("RESERVED3"),col("year"),col("month"),col("day"),col("hour"))


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
