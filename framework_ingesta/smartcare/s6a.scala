// Databricks notebook source
/*dbutils.widgets.text("moment","2024/11/05/15/0/")
dbutils.widgets.text("path_adls","abfss://bigdatadev@stbigdatadev02.dfs.core.windows.net")
dbutils.widgets.text("ruta_origen","/data/trafico/trafico_detalle/smartcare/landing_traf_cert/s6a/")
dbutils.widgets.text("path_salida","/data/trafico/trafico_detalle/smartcare/landing_traf_cert/s6a/")
dbutils.widgets.text("catalogo","bidesarrollo")
dbutils.widgets.text("tabla_salida","trafico_cert.smartcare_s6a")*/

// COMMAND ----------

val path_adls = dbutils.widgets.get("path_adls")
val catalogo = dbutils.widgets.get("catalogo")
val landing_s6a = path_adls + dbutils.widgets.get("ruta_origen")
val tabla_salida = catalogo +"."+ dbutils.widgets.get("tabla_salida")
val path_salida = path_adls + dbutils.widgets.get("path_salida")

// COMMAND ----------

val columnas = Seq("IMSI","MSISDN","IMEI","HSS_SIG_IP","MME_SIG_IP","TRANS_TYPE","TRANS_REQ_TIME_SEC","TRANS_REQ_TIME_MSEC","TRANS_RSP_TIME_SEC","TRANS_RSP_TIME_MSEC","TRANS_SUCCED_FLAG","TRANS_CAUSE_TYPE","TRANS_CAUSE","MCC","MNC","HOMEMCC","HOMEMNC","_corrupt_record")
val s6a = spark.read.option("delimiter","|").csv(landing_s6a).toDF(columnas: _*)

// COMMAND ----------

val df = s6a.
withColumn("year", date_format(from_unixtime(col("TRANS_REQ_TIME_SEC")), "yyyy")).
withColumn("month", date_format(from_unixtime(col("TRANS_REQ_TIME_SEC")), "MM")).
withColumn("day", date_format(from_unixtime(col("TRANS_REQ_TIME_SEC")), "dd")).
withColumn("hour", date_format(from_unixtime(col("TRANS_REQ_TIME_SEC")), "HH")).
select(col("IMSI"),col("MSISDN"),col("IMEI"),col("HSS_SIG_IP"),col("MME_SIG_IP"),col("TRANS_TYPE").cast("int"),col("TRANS_REQ_TIME_SEC").cast("int"),col("TRANS_REQ_TIME_MSEC").cast("int"),col("TRANS_RSP_TIME_SEC").cast("int"),col("TRANS_RSP_TIME_MSEC").cast("int"),col("TRANS_SUCCED_FLAG").cast("short"),col("TRANS_CAUSE_TYPE").cast("int"),col("TRANS_CAUSE").cast("int"),col("MCC"),col("MNC"),col("HOMEMCC"),col("HOMEMNC"),col("_corrupt_record"),col("year"),col("month"),col("day"),col("hour"))

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
