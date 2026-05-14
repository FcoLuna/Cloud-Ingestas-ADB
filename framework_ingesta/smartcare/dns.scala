// Databricks notebook source
/*
dbutils.widgets.text("path_adls","abfss://bigdatadev@stbigdatadev02.dfs.core.windows.net")
dbutils.widgets.text("ruta_origen","/data/trafico/trafico_detalle/smartcare/landing_/dns/")
dbutils.widgets.text("path_salida","/data/trafico/trafico_detalle/smartcare/dns/raw/")
dbutils.widgets.text("catalogo","bidesarrollo")
dbutils.widgets.text("tabla_salida","trafico_cert.smartcare_dns") 
*/

// COMMAND ----------

val path_adls = dbutils.widgets.get("path_adls")
val catalogo = dbutils.widgets.get("catalogo")
val landing_dns = path_adls + dbutils.widgets.get("ruta_origen")
val tabla_salida = catalogo +"."+ dbutils.widgets.get("tabla_salida")
val path_salida = path_adls + dbutils.widgets.get("path_salida")

// COMMAND ----------

val columnas = Seq("SID","INTERFACEID","BEGIN_TIME","BEGIN_TIME_MSEL","END_TIME","END_TIME_MSEL","PROT_CATEGORY","PROT_TYPE","MSISDN","IMSI","IMEI","ROAM_DIRECTION","MCC","MNC","RAT","LAC","RAC","SAC","CI","MS_IP","SERVER_IP","MS_PORT","SERVER_PORT","APN","SGSN_USER_IP","GGSN_USER_IP","L4_TYPE","L4_UL_THROUGHPUT","L4_DW_THROUGHPUT","L4_UL_GOODPUT","L4_DW_GOODPUT","L4_UL_PACKETS","L4_DW_PACKETS","TCP_CONN_STATES","TCP_RTT","TCP_UL_OUTOFSEQU","TCP_DW_OUTOFSEQU","TCP_UL_RETRANS","TCP_DW_RETRANS","TCP_WIN_SIZE","DNS_TRANS_NUM","DNS_SUCCEED_NUM","DNS_OK_DELAY_TIME","DNS_SOURCE_TYPE","DNS_DOMAIN","DNS_DOMAIN_IP","FST_TRANS_FLAG","ERROR_CODE_FST","TAC","ECI","TCP_RTT_STEP1","TCP_UL_RETRANS_WITHPL","TCP_DW_RETRANS_WITHPL","TCP_UL_PACKAGES_WITHPL","TCP_DW_PACKAGES_WITHPL","RAN_NE_USER_IP","HOMEMCC","HOMEMNC","PREPAID_FLAG","TETHERING_FLAG","CHARGING_CHARACTERISTICS","SV","SUB_PROT_TYPE","APP_ID","TRAFFIC_CLASS_NEG","THP_NEG","QCI_NEG","TOTAL_TCP_RTT","TOTAL_TCP_RTT_STEP1","_corrupt_record")
val dns = spark.read.option("delimiter","|").csv(landing_dns).toDF(columnas: _*)

// COMMAND ----------

val df = dns.
withColumn("year", date_format(from_unixtime(col("BEGIN_TIME")), "yyyy")).
withColumn("month", date_format(from_unixtime(col("BEGIN_TIME")), "MM")).
withColumn("day", date_format(from_unixtime(col("BEGIN_TIME")), "dd")).
withColumn("hour", date_format(from_unixtime(col("BEGIN_TIME")), "HH")).
select(col("SID"),col("INTERFACEID").cast("short"),col("BEGIN_TIME").cast("int"),col("BEGIN_TIME_MSEL"),col("END_TIME").cast("int"),col("END_TIME_MSEL"),col("PROT_CATEGORY").cast("int"),col("PROT_TYPE").cast("int"),col("MSISDN"),col("IMSI"),col("IMEI"),col("ROAM_DIRECTION"),col("MCC"),col("MNC"),col("RAT").cast("short"),col("LAC"),col("RAC"),col("SAC"),col("CI"),col("MS_IP"),col("SERVER_IP"),col("MS_PORT"),col("SERVER_PORT").cast("int"),col("APN"),col("SGSN_USER_IP"),col("GGSN_USER_IP"),col("L4_TYPE"),col("L4_UL_THROUGHPUT").cast("bigint"),col("L4_DW_THROUGHPUT").cast("bigint"),col("L4_UL_GOODPUT"),col("L4_DW_GOODPUT"),col("L4_UL_PACKETS"),col("L4_DW_PACKETS"),col("TCP_CONN_STATES"),col("TCP_RTT").cast("bigint"),col("TCP_UL_OUTOFSEQU"),col("TCP_DW_OUTOFSEQU"),col("TCP_UL_RETRANS"),col("TCP_DW_RETRANS"),col("TCP_WIN_SIZE"),col("DNS_TRANS_NUM"),col("DNS_SUCCEED_NUM"),col("DNS_OK_DELAY_TIME"),col("DNS_SOURCE_TYPE"),col("DNS_DOMAIN"),col("DNS_DOMAIN_IP"),col("FST_TRANS_FLAG"),col("ERROR_CODE_FST"),col("TAC"),col("ECI"),col("TCP_RTT_STEP1").cast("bigint"),col("TCP_UL_RETRANS_WITHPL"),col("TCP_DW_RETRANS_WITHPL"),col("TCP_UL_PACKAGES_WITHPL"),col("TCP_DW_PACKAGES_WITHPL"),col("RAN_NE_USER_IP"),col("HOMEMCC"),col("HOMEMNC"),col("PREPAID_FLAG"),col("TETHERING_FLAG").cast("short"),col("CHARGING_CHARACTERISTICS"),col("SV"),col("SUB_PROT_TYPE").cast("int"),col("APP_ID").cast("int"),col("TRAFFIC_CLASS_NEG"),col("THP_NEG"),col("QCI_NEG"),col("TOTAL_TCP_RTT"),col("TOTAL_TCP_RTT_STEP1"),col("_corrupt_record"),col("year"),col("month"),col("day"),col("hour"))


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
