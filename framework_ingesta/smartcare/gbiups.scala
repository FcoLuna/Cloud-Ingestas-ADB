// Databricks notebook source
/*dbutils.widgets.text("moment","2024/11/05/15/0/")
dbutils.widgets.text("path_adls","abfss://bigdatadev@stbigdatadev02.dfs.core.windows.net")
dbutils.widgets.text("ruta_origen","/data/trafico/trafico_detalle/smartcare/landing_traf_cert/gbiups/")
dbutils.widgets.text("path_salida","/data/trafico/trafico_detalle/smartcare/landing_traf_cert/gbiups/")
dbutils.widgets.text("catalogo","bidesarrollo")
dbutils.widgets.text("tabla_salida","trafico_cert.smartcare_gbiups")
*/

// COMMAND ----------

val path_adls = dbutils.widgets.get("path_adls")
val catalogo = dbutils.widgets.get("catalogo")
val landing_gbiups = path_adls + dbutils.widgets.get("ruta_origen")
val tabla_salida = catalogo +"."+ dbutils.widgets.get("tabla_salida")
val path_salida = path_adls + dbutils.widgets.get("path_salida")

// COMMAND ----------

val columnas = Seq("SID","PROTOCOL_ID","PROCEDURE_ID","PROBEID","ENCRYPT_VERSION","INTERFACEID","IMSI","MSISDN","IMEI","MS_IP","PTMSI","ROAMING_TYPE","HOMEMCC","HOMEMNC","HOMEPROID","HOMEAREAID","RAT","MCC","MNC","LAI","RAI_TAI","SAI_CGI_ECGI","APN","REFID","PROC_TYPE","PROC_STARTTIME","PROC_STARTTIME_MSEC","PROC_ENDTIME","PROC_ENDTIME_MSEC","PROC_SUCCED_FLAG","PROC_FIRFAIL_PROT","PROC_FIRFAIL_TRANS","PROC_FIRFAIL_CAUSE_TYPE","PROC_FIRFAIL_CAUSE","SGSN_ID","RAN_NE_ID","SGSN_SIG_IP","RAN_NE_SIG_IP","SGSN_SIGIP_LST","GGSN_SIGIP_LST","NETWORK_REQ_PDP_TYPE","REQ_PDPACT_TIME_SEC","REQ_PDPACT_TIME_MSEC","REQ_PDPACT_SUCCED_FLAG","REQ_PDPACT_TIMEOUT_FLAG","PAGING_REQ_TIME_SEC","PAGING_REQ_TIME_MSEC","PAGING_RSP_TIME_SEC","PAGING_RSP_TIME_MSEC","PAGING_PS_SUCCED_FLAG","PAGING_PS_RESELECT_FLAG","PAGING_PS_TIMEOUT_FLAG","MM_TRAN_TYPE","MM_TRANS_SUB_TYPE","MM_TRANS_DIRECTION","MM_TRANS_REQ_TIME_SEC","MM_TRANS_REQ_TIME_MSEC","MM_TRANS_RSP_TIME_SEC","MM_TRANS_RSP_TIME_MSEC","MM_TRANS_SUCCED_FLAG","MM_TRANS_RESELECT_FLAG","MM_TRANS_CAUSE","POWEROFF_FLAG","RAU_WITH_PDP_FLAG","RAU_CATEGORY","OLD_MCC","OLD_MNC","OLD_LAI","OLD_RAI_TAI","OLD_SAI_CGI_ECGI","IDENTITY_REQ_TIME","IDENTITY_REQ_TIME_MSEC","IDENTITY_RSP_TIME","IDENTITY_RSP_TIME_MSEC","IDENTITY_CAUSE","IDENTITY_SUCCED_FLAG","IDENTITY_RESELECT_FLAG","AUTH_CIPHER_REQ_TIME_SEC","AUTH_CIPHER_REQ_TIME_MSEC","AUTH_CIPHER_RSP_TIME_SEC","AUTH_CIPHER_RSP_TIME_MSEC","AUTH_CIPHER_SUCCEED_FLAG","AUTH_CIPHER_RESELECT_FLAG","AUTH_CIPHER_CAUSE","SECURITY_MODE_TIME","SECURITY_MODE_TIME_MSEC","SECURITY_MODE_CMPT_TIME","SECURITY_MODE_CMPT_TIME_MSEC","SECURITY_MODE_SUCCED_FLAG","SECURITY_MODE_RESELECT_FLAG","SECURITY_MODE_CAUSE","PDP_TRANS_TYPE","SECONDARY_PDP_FLAG","PDP_REQ_TIME","PDP_REQ_TIME_MSEC","PDP_RSP_TIME","PDP_RSP_TIME_MSEC","PDP_SUCCEED_FLAG","PDP_FAIL_CAUSE","PDPADDRESS_NULL_FLAG","PDP_TRANS_DIRECTION","ASSN_TIME","ASSN_TIME_MSEC","ASSN_CMPT_TIME","ASSN_CMPT_TIME_MSEC","ASSN_SUCCED_FLAG","ASSN_CAUSE","RADIO_STATUS_TIME","RADIO_STATUS_TIME_MS","RADIO_STATUS_CAUSE","LLC_DISCARD_TIME","LLC_DISCARD_TIME_MS","LLC_FRAME_NUM","LLC_FRAME_OCT","DISCARD_FRAME_NUM","DISCARD_FRAME_OCT","RESERVED1","RESERVED2","RESERVED3","RESERVED4","HO_REFER_NUMBER","LAYER1ID","LAYER2ID","LAYER3ID","LAYER4ID","LAYER5ID","LAYER6ID","RAB_TYPE","MBR_DL_NEG","GBR_DL_NEG","MBR_UL_NEG","GBR_UL_NEG","SV","IMEI_CIPHERTEXT","PREPAID_FLAG","PTMSI_SIGNATURE","NRI","RANAP_TRANS_TYPE","RANAP_TRANS_REQ_TIME_SEC","RANAP_TRANS_REQ_TIME_MSEC","RANAP_TRANS_RSP_TIME_SEC","RANAP_TRANS_RSP_TIME_MSEC","RANAP_TRANS_SUCCED_FLAG","RANAP_TRANS_CAUSE","SESSIONKEY","PDP_TYPE","OLD_PTMSI_TYPE","USER_CATEGORY","OLD_RAT","c1")


// COMMAND ----------

// Leer el archivo CSV y separar los valores por "|"
val df = spark.read.text(landing_gbiups)

// Limpiar los corchetes [] y las comillas dobles "
val cleanedData = df.
withColumn("value", regexp_replace(col("value"), "[\\[\\]\"]", "")).  // Eliminar corchetes [] y comillas "
withColumn("value", split(col("value"), "\\|"))  // Separar los valores usando el delimitador '|'

// Verificar cuántos elementos tiene cada fila (esto nos da el número de columnas)
val numColumns = cleanedData.select(size(col("value"))).first().getInt(0)

// Crear una lista de columnas con el método getItem() para acceder a cada elemento del array
val columns = columnas.zipWithIndex.map { case (colName, index) =>
  col("value").getItem(index).as(colName)
}

// Seleccionar las columnas y crear el DataFrame final
val gbiups = cleanedData.select(columns: _*)

// COMMAND ----------

val df = gbiups.
withColumn("year", date_format(from_unixtime(col("PROC_STARTTIME")), "yyyy")).
withColumn("month", date_format(from_unixtime(col("PROC_STARTTIME")), "MM")).
withColumn("day", date_format(from_unixtime(col("PROC_STARTTIME")), "dd")).
withColumn("hour", date_format(from_unixtime(col("PROC_STARTTIME")), "HH")).
select(
  col("SID"),col("PROTOCOL_ID"),col("PROCEDURE_ID"),col("PROBEID"),col("ENCRYPT_VERSION"),col("INTERFACEID").cast("short"),col("IMSI"),col("MSISDN"),col("IMEI"),col("MS_IP"),col("PTMSI"),col("ROAMING_TYPE").cast("short"),col("HOMEMCC"),col("HOMEMNC"),col("HOMEPROID"),col("HOMEAREAID"),col("RAT").cast("bigint"),col("MCC"),col("MNC"),col("LAI"),col("RAI_TAI"),col("SAI_CGI_ECGI"),col("APN"),col("REFID"),col("PROC_TYPE").cast("bigint"),col("PROC_STARTTIME").cast("bigint"),col("PROC_STARTTIME_MSEC"),col("PROC_ENDTIME").cast("bigint"),col("PROC_ENDTIME_MSEC"),col("PROC_SUCCED_FLAG").cast("short"),col("PROC_FIRFAIL_PROT").cast("bigint"),col("PROC_FIRFAIL_TRANS").cast("bigint"),col("PROC_FIRFAIL_CAUSE_TYPE").cast("bigint"),col("PROC_FIRFAIL_CAUSE").cast("bigint"),col("SGSN_ID"),col("RAN_NE_ID"),col("SGSN_SIG_IP"),col("RAN_NE_SIG_IP"),col("SGSN_SIGIP_LST"),col("GGSN_SIGIP_LST"),col("NETWORK_REQ_PDP_TYPE"),col("REQ_PDPACT_TIME_SEC"),col("REQ_PDPACT_TIME_MSEC"),col("REQ_PDPACT_SUCCED_FLAG"),col("REQ_PDPACT_TIMEOUT_FLAG"),col("PAGING_REQ_TIME_SEC"),col("PAGING_REQ_TIME_MSEC"),col("PAGING_RSP_TIME_SEC"),col("PAGING_RSP_TIME_MSEC"),col("PAGING_PS_SUCCED_FLAG"),col("PAGING_PS_RESELECT_FLAG"),col("PAGING_PS_TIMEOUT_FLAG"),col("MM_TRAN_TYPE"),col("MM_TRANS_SUB_TYPE"),col("MM_TRANS_DIRECTION"),col("MM_TRANS_REQ_TIME_SEC"),col("MM_TRANS_REQ_TIME_MSEC"),col("MM_TRANS_RSP_TIME_SEC"),col("MM_TRANS_RSP_TIME_MSEC"),col("MM_TRANS_SUCCED_FLAG"),col("MM_TRANS_RESELECT_FLAG"),col("MM_TRANS_CAUSE"),col("POWEROFF_FLAG"),col("RAU_WITH_PDP_FLAG"),col("RAU_CATEGORY"),col("OLD_MCC"),col("OLD_MNC"),col("OLD_LAI"),col("OLD_RAI_TAI"),col("OLD_SAI_CGI_ECGI"),col("IDENTITY_REQ_TIME"),col("IDENTITY_REQ_TIME_MSEC"),col("IDENTITY_RSP_TIME"),col("IDENTITY_RSP_TIME_MSEC"),col("IDENTITY_CAUSE"),col("IDENTITY_SUCCED_FLAG"),col("IDENTITY_RESELECT_FLAG"),col("AUTH_CIPHER_REQ_TIME_SEC"),col("AUTH_CIPHER_REQ_TIME_MSEC"),col("AUTH_CIPHER_RSP_TIME_SEC"),col("AUTH_CIPHER_RSP_TIME_MSEC"),col("AUTH_CIPHER_SUCCEED_FLAG"),col("AUTH_CIPHER_RESELECT_FLAG"),col("AUTH_CIPHER_CAUSE"),col("SECURITY_MODE_TIME"),col("SECURITY_MODE_TIME_MSEC"),col("SECURITY_MODE_CMPT_TIME"),col("SECURITY_MODE_CMPT_TIME_MSEC"),col("SECURITY_MODE_SUCCED_FLAG"),col("SECURITY_MODE_RESELECT_FLAG"),col("SECURITY_MODE_CAUSE"),col("PDP_TRANS_TYPE"),col("SECONDARY_PDP_FLAG"),col("PDP_REQ_TIME"),col("PDP_REQ_TIME_MSEC"),col("PDP_RSP_TIME"),col("PDP_RSP_TIME_MSEC"),col("PDP_SUCCEED_FLAG"),col("PDP_FAIL_CAUSE"),col("PDPADDRESS_NULL_FLAG"),col("PDP_TRANS_DIRECTION"),col("ASSN_TIME"),col("ASSN_TIME_MSEC"),col("ASSN_CMPT_TIME"),col("ASSN_CMPT_TIME_MSEC"),col("ASSN_SUCCED_FLAG"),col("ASSN_CAUSE"),col("RADIO_STATUS_TIME"),col("RADIO_STATUS_TIME_MS"),col("RADIO_STATUS_CAUSE"),col("LLC_DISCARD_TIME"),col("LLC_DISCARD_TIME_MS"),col("LLC_FRAME_NUM"),col("LLC_FRAME_OCT"),col("DISCARD_FRAME_NUM"),col("DISCARD_FRAME_OCT"),col("RESERVED1"),col("RESERVED2"),col("RESERVED3"),col("RESERVED4"),col("HO_REFER_NUMBER"),col("LAYER1ID"),col("LAYER2ID"),col("LAYER3ID"),col("LAYER4ID"),col("LAYER5ID"),col("LAYER6ID"),col("RAB_TYPE"),col("MBR_DL_NEG"),col("GBR_DL_NEG"),col("MBR_UL_NEG"),col("GBR_UL_NEG"),col("SV"),col("IMEI_CIPHERTEXT"),col("PREPAID_FLAG"),col("PTMSI_SIGNATURE"),col("NRI"),col("RANAP_TRANS_TYPE"),col("RANAP_TRANS_REQ_TIME_SEC"),col("RANAP_TRANS_REQ_TIME_MSEC"),col("RANAP_TRANS_RSP_TIME_SEC"),col("RANAP_TRANS_RSP_TIME_MSEC"),col("RANAP_TRANS_SUCCED_FLAG"),col("RANAP_TRANS_CAUSE"),col("SESSIONKEY"),col("PDP_TYPE"),col("OLD_PTMSI_TYPE"),col("USER_CATEGORY"),col("OLD_RAT"),col("c1").as("_corrupt_record"),col("year"),col("month"),col("day"),col("hour"))

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
