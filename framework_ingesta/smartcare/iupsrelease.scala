// Databricks notebook source
/*dbutils.widgets.text("moment","2024/11/05/15/0/")
dbutils.widgets.text("path_adls","abfss://bigdatadev@stbigdatadev02.dfs.core.windows.net")
dbutils.widgets.text("ruta_origen","/data/trafico/trafico_detalle/smartcare/landing_traf_cert/iupsrelease/")
dbutils.widgets.text("path_salida","/data/trafico/trafico_detalle/smartcare/landing_traf_cert/iupsrelease/")
dbutils.widgets.text("catalogo","bidesarrollo")
dbutils.widgets.text("tabla_salida","trafico_cert.smartcare_iupsrelease")*/

// COMMAND ----------

val path_adls = dbutils.widgets.get("path_adls")
val catalogo = dbutils.widgets.get("catalogo")
val landing_iupsrelease = path_adls + dbutils.widgets.get("ruta_origen")
val tabla_salida = catalogo +"."+ dbutils.widgets.get("tabla_salida")
val path_salida = path_adls + dbutils.widgets.get("path_salida")

// COMMAND ----------

val columnas = Seq("SID","PROTOCOL_ID","PROCEDURE_ID","PROBEID","ENCRYPT_VERSION","INTERFACEID","IMSI","MSISDN","IMEI","MS_IP","P_TMSI","ROAMING_TYPE","ROAM_DIRECTION","HOMEMCC","HOMEMNC","HOMEPROID","HOMEAREAID","RAT","MCC","MNC","LAI","RAI_TAI","SAI_CGI_ECGI","APN","REFID","PROC_TYPE","SGSN_ID","RAN_NE_ID","RLS_REQ_TYPE","RLS_REQ_TIME_SEC","RLS_REQ_TIME_MSEC","TRANS_TYPE","TRANS_REQ_TIME_SEC","TRANS_REQ_TIME_MSEC","TRANS_RSP_TIME_SEC","TRANS_RSP_TIME_MSEC","TRANS_SUCCED_FLAG","TRANS_RESELECT_FLAG","TRANS_FIRFAILMSG","TRANS_CAUSE","TRANS_SIG_DIRECTION","RESERVED1","RESERVED2","RESERVED3","RESERVED4","LAYER1ID","LAYER2ID","LAYER3ID","LAYER4ID","LAYER5ID","LAYER6ID","SV","IMEI_CIPHERTEXT","PREPAID_FLAG","USER_CATEGORY","_corrupt_record")
val iupsrelease = spark.read.option("delimiter","|").csv(landing_iupsrelease).toDF(columnas: _*)

// COMMAND ----------

val df = iupsrelease.
withColumn("year", date_format(from_unixtime(col("TRANS_REQ_TIME_SEC")), "yyyy")).
withColumn("month", date_format(from_unixtime(col("TRANS_REQ_TIME_SEC")), "MM")).
withColumn("day", date_format(from_unixtime(col("TRANS_REQ_TIME_SEC")), "dd")).
withColumn("hour", date_format(from_unixtime(col("TRANS_REQ_TIME_SEC")), "HH")).
select(col("SID").cast("bigint"),col("PROTOCOL_ID").cast("int"),col("PROCEDURE_ID").cast("bigint"),col("PROBEID").cast("bigint"),col("ENCRYPT_VERSION").cast("int"),col("INTERFACEID").cast("short"),col("IMSI"),col("MSISDN"),col("IMEI"),col("MS_IP"),col("P_TMSI"),col("ROAMING_TYPE").cast("short"),col("ROAM_DIRECTION").cast("short"),col("HOMEMCC"),col("HOMEMNC"),col("HOMEPROID").cast("bigint"),col("HOMEAREAID").cast("bigint"),col("RAT").cast("int"),col("MCC"),col("MNC"),col("LAI"),col("RAI_TAI"),col("SAI_CGI_ECGI"),col("APN"),col("REFID").cast("bigint"),col("PROC_TYPE").cast("int"),col("SGSN_ID").cast("bigint"),col("RAN_NE_ID").cast("bigint"),col("RLS_REQ_TYPE").cast("bigint"),col("RLS_REQ_TIME_SEC").cast("int"),col("RLS_REQ_TIME_MSEC").cast("int"),col("TRANS_TYPE").cast("int"),col("TRANS_REQ_TIME_SEC").cast("int"),col("TRANS_REQ_TIME_MSEC").cast("int"),col("TRANS_RSP_TIME_SEC").cast("int"),col("TRANS_RSP_TIME_MSEC").cast("int"),col("TRANS_SUCCED_FLAG").cast("short"),col("TRANS_RESELECT_FLAG").cast("short"),col("TRANS_FIRFAILMSG").cast("int"),col("TRANS_CAUSE").cast("int"),col("TRANS_SIG_DIRECTION").cast("short"),col("RESERVED1"),col("RESERVED2"),col("RESERVED3"),col("RESERVED4"),col("LAYER1ID").cast("int"),col("LAYER2ID").cast("int"),col("LAYER3ID").cast("int"),col("LAYER4ID").cast("int"),col("LAYER5ID").cast("int"),col("LAYER6ID").cast("int"),col("SV"),col("IMEI_CIPHERTEXT"),col("PREPAID_FLAG").cast("short"),col("USER_CATEGORY").cast("bigint"),col("_corrupt_record"),col("year"),col("month"),col("day"),col("hour"))


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
