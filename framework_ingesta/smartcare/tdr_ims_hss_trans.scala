// Databricks notebook source
/*dbutils.widgets.text("moment","2024/11/05/15/")
dbutils.widgets.text("path_adls","abfss://bigdatadev@stbigdatadev02.dfs.core.windows.net")
dbutils.widgets.text("ruta_origen","/data/trafico/trafico_detalle/smartcare/landing_traf_cert/tdr_ims_hss_trans/")
dbutils.widgets.text("path_salida","/data/trafico/trafico_detalle/smartcare/landing_traf_cert/tdr_ims_hss_trans/")
dbutils.widgets.text("catalogo","bidesarrollo")
dbutils.widgets.text("tabla_salida","trafico_cert.smartcare_tdr_ims_hss_trans")*/

// COMMAND ----------

val path_adls = dbutils.widgets.get("path_adls")
val catalogo = dbutils.widgets.get("catalogo")
val landing_tdr_ims_hss_trans = path_adls + dbutils.widgets.get("ruta_origen")
val tabla_salida = catalogo +"."+ dbutils.widgets.get("tabla_salida")
val path_salida = path_adls + dbutils.widgets.get("path_salida")

// COMMAND ----------

val columnas = Seq("SID","REFID","ProbeID","STARTTIME","MILLISEC","ENDTIME","TRANS_TYPE","TRANS_SUCCED_FLAG","INTERFACE","SOURCE_NE_TYPE","SOURCE_NE_IP","SOURCE_NE_PORT","SOURCR_NE_ID","DEST_NE_TYPE","DEST_NE_IP","DEST_NE_PORT","DEST_NE_ID","IMPI_TEL_URI","IMPI_SIP_URI","IMPU_TEL_URI","IMPU_SIP_URI","IMPU_TYPE","Orig_Host","Orig_Realm","Dest_Host","Dest_Realm","Server_Name","NAI","VISITED_NETWORK_ID","RESULT_CODE","EXPERIMENTAL_RESULT","OCSISERVICEKEY","OCSISCPNO","OCSICAPVER","DCSISERVICEKEY","DCSISCPNO","DCSICAPVER","VTCSISERVICEKEY","VTCSISCPNO","VTCSICAPVER","GENERALODB","HPLMNODB","FIRFAILTIME","RESERVED1","RESERVED2","RESERVED3","RESERVED4","RESERVED5","RESERVED6","RESERVED7","RESERVED8","CTX_GROUP_NO","CTX_SUBGROUP_NO","CTX_PRIVATE_NUMBER","TADS_FLAG","TADS_RAT","TADS_IMSVOICE_FLAG","C_MSISDN","STN_SR","UE_SRVCC_CAPABILITY","REQ_TYPE","SERVICE_TYPE","CSRN","USER_CATEGORY","CS_LOC_INFO","IMEI","SRV_ATTR_ID","FAIL_CATEGORY","RULE_CODE","SUPPLEMENTARY_SERVICE_TYPE")
val tdr_ims_hss_trans = spark.read.option("delimiter","|").csv(landing_tdr_ims_hss_trans).toDF(columnas: _*)

// COMMAND ----------

val df = tdr_ims_hss_trans.
withColumn("year", date_format(from_unixtime(col("STARTTIME")), "yyyy")).
withColumn("month", date_format(from_unixtime(col("STARTTIME")), "MM")).
withColumn("day", date_format(from_unixtime(col("STARTTIME")), "dd")).
withColumn("hour", date_format(from_unixtime(col("STARTTIME")), "HH")).
select(col("SID"),col("REFID"),col("ProbeID"),col("STARTTIME").cast("int"),col("MILLISEC"),col("ENDTIME").cast("int"),col("TRANS_TYPE"),col("TRANS_SUCCED_FLAG"),col("INTERFACE"),col("SOURCE_NE_TYPE"),col("SOURCE_NE_IP"),col("SOURCE_NE_PORT"),col("SOURCR_NE_ID"),col("DEST_NE_TYPE"),col("DEST_NE_IP"),col("DEST_NE_PORT"),col("DEST_NE_ID"),col("IMPI_TEL_URI"),col("IMPI_SIP_URI"),col("IMPU_TEL_URI"),col("IMPU_SIP_URI"),col("IMPU_TYPE"),col("Orig_Host"),col("Orig_Realm"),col("Dest_Host"),col("Dest_Realm"),col("Server_Name"),col("NAI"),col("VISITED_NETWORK_ID"),col("RESULT_CODE"),col("EXPERIMENTAL_RESULT"),col("OCSISERVICEKEY"),col("OCSISCPNO"),col("OCSICAPVER"),col("DCSISERVICEKEY"),col("DCSISCPNO"),col("DCSICAPVER"),col("VTCSISERVICEKEY"),col("VTCSISCPNO"),col("VTCSICAPVER"),col("GENERALODB"),col("HPLMNODB"),col("FIRFAILTIME"),col("RESERVED1"),col("RESERVED2"),col("RESERVED3"),col("RESERVED4"),col("RESERVED5"),col("RESERVED6"),col("RESERVED7"),col("RESERVED8"),col("CTX_GROUP_NO"),col("CTX_SUBGROUP_NO"),col("CTX_PRIVATE_NUMBER"),col("TADS_FLAG"),col("TADS_RAT"),col("TADS_IMSVOICE_FLAG"),col("C_MSISDN"),col("STN_SR"),col("UE_SRVCC_CAPABILITY"),col("REQ_TYPE"),col("SERVICE_TYPE"),col("CSRN"),col("USER_CATEGORY"),col("CS_LOC_INFO"),col("IMEI"),col("SRV_ATTR_ID"),col("FAIL_CATEGORY"),col("RULE_CODE"),col("SUPPLEMENTARY_SERVICE_TYPE"),col("year"),col("month"),col("day"),col("hour"))


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
