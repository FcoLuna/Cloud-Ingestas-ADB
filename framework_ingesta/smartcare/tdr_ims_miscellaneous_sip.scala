// Databricks notebook source
/*dbutils.widgets.text("moment","2024/11/05/15/")
dbutils.widgets.text("path_adls","abfss://bigdatadev@stbigdatadev02.dfs.core.windows.net")
dbutils.widgets.text("ruta_origen","/data/trafico/trafico_detalle/smartcare/landing_traf_cert/tdr_ims_miscellaneous_sip/")
dbutils.widgets.text("path_salida","/data/trafico/trafico_detalle/smartcare/landing_traf_cert/tdr_ims_miscellaneous_sip/")
dbutils.widgets.text("catalogo","bidesarrollo")
dbutils.widgets.text("tabla_salida","trafico_cert.smartcare_tdr_ims_miscellaneous_sip") //raw_trafico.aiumoc (cuando pase a prod)*/

// COMMAND ----------

val path_adls = dbutils.widgets.get("path_adls")
val catalogo = dbutils.widgets.get("catalogo")
val landing_tdr_ims_miscellaneous_sip = path_adls + dbutils.widgets.get("ruta_origen")
val tabla_salida = catalogo +"."+ dbutils.widgets.get("tabla_salida")
val path_salida = path_adls + dbutils.widgets.get("path_salida")

// COMMAND ----------

val columnas = Seq("STARTTIME","MILLISEC","ENDTIME","SERVICE_TYPE","SERVICE_STATUS","ACCESS_NE_TYPE","ACCESS_NE_IP","ACCESS_NE_PORT","ACCESS_NE_ID","P_CSCF_IP","P_CSCF_PORT","P_CSCF_ID","S_CSCF_IP","S_CSCF_PORT","S_CSCF_ID","I_CSCF_IP","I_CSCF_PORT","I_CSCF_ID","AS_IP","AS_PORT","AS_ID","HSS_IP","HSS_Port","HSS_ID","CALL_SIDE","IMPI_TEL_URI","IMPI_SIP_URI","IMPU_TEL_URI","IMPU_SIP_URI","IMPU_TYPE","FORMAT_IMPU","FORMAT_IMPU_TYPE","IMEI","TERM_UNTRUST_IP_ADDR","TERM_UNTRUST_PORT","TERM_TRUST_IP_ADDR","TERM_TRUST_PORT","DEVICE_TYPE","ACCESS_TYPE","ACCESS_INFO","VISIT_DOMAIN","HOME_DOMAIN","RESPONSE_CODE","FINISH_WARNING","FINISH_REASON","FIRFAILTIME","FIRST_FAIL_NE_TYPE","FIRST_FAIL_NE_IP","FIRFAILPROT","FIRFAILMSG","FIRFAILCAUSE","CAUSELST","PROTOCOLS","Layer1ID","Layer2ID","Layer3ID","Layer4ID","Layer5ID","Layer6ID")
val tdr_ims_miscellaneous_sip = spark.read.option("delimiter","|").csv(landing_tdr_ims_miscellaneous_sip).toDF(columnas: _*)

// COMMAND ----------

val df = tdr_ims_miscellaneous_sip.
withColumn("year", date_format(from_unixtime(col("STARTTIME")), "yyyy")).
withColumn("month", date_format(from_unixtime(col("STARTTIME")), "MM")).
withColumn("day", date_format(from_unixtime(col("STARTTIME")), "dd")).
withColumn("hour", date_format(from_unixtime(col("STARTTIME")), "HH")).
select(col("STARTTIME").cast("int"),col("MILLISEC"),col("ENDTIME").cast("int"),col("SERVICE_TYPE"),col("SERVICE_STATUS"),col("ACCESS_NE_TYPE"),col("ACCESS_NE_IP"),col("ACCESS_NE_PORT"),col("ACCESS_NE_ID"),col("P_CSCF_IP"),col("P_CSCF_PORT"),col("P_CSCF_ID"),col("S_CSCF_IP"),col("S_CSCF_PORT"),col("S_CSCF_ID"),col("I_CSCF_IP"),col("I_CSCF_PORT"),col("I_CSCF_ID"),col("AS_IP"),col("AS_PORT"),col("AS_ID"),col("HSS_IP"),col("HSS_Port"),col("HSS_ID"),col("CALL_SIDE"),col("IMPI_TEL_URI"),col("IMPI_SIP_URI"),col("IMPU_TEL_URI"),col("IMPU_SIP_URI"),col("IMPU_TYPE"),col("FORMAT_IMPU"),col("FORMAT_IMPU_TYPE"),col("IMEI"),col("TERM_UNTRUST_IP_ADDR"),col("TERM_UNTRUST_PORT"),col("TERM_TRUST_IP_ADDR"),col("TERM_TRUST_PORT"),col("DEVICE_TYPE"),col("ACCESS_TYPE"),col("ACCESS_INFO"),col("VISIT_DOMAIN"),col("HOME_DOMAIN"),col("RESPONSE_CODE"),col("FINISH_WARNING"),col("FINISH_REASON"),col("FIRFAILTIME"),col("FIRST_FAIL_NE_TYPE"),col("FIRST_FAIL_NE_IP"),col("FIRFAILPROT"),col("FIRFAILMSG"),col("FIRFAILCAUSE"),col("CAUSELST"),col("PROTOCOLS"),col("Layer1ID"),col("Layer2ID"),col("Layer3ID"),col("Layer4ID"),col("Layer5ID"),col("Layer6ID"),col("year"),col("month"),col("day"),col("hour"))


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
