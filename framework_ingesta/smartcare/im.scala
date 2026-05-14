// Databricks notebook source
/*dbutils.widgets.text("moment","2024/11/05/15/0/")
dbutils.widgets.text("path_adls","abfss://bigdatadev@stbigdatadev02.dfs.core.windows.net")
dbutils.widgets.text("ruta_origen","/data/trafico/trafico_detalle/smartcare/landing_traf_cert/im/")
dbutils.widgets.text("path_salida","/data/trafico/trafico_detalle/smartcare/landing_traf_cert/im/")
dbutils.widgets.text("catalogo","bidesarrollo")
dbutils.widgets.text("tabla_salida","trafico_cert.smartcare_im")
*/

// COMMAND ----------

val path_adls = dbutils.widgets.get("path_adls")
val catalogo = dbutils.widgets.get("catalogo")
val landing_im = path_adls + dbutils.widgets.get("ruta_origen")
val tabla_salida = catalogo +"."+ dbutils.widgets.get("tabla_salida")
val path_salida = path_adls + dbutils.widgets.get("path_salida")

// COMMAND ----------

val columnas = Seq("BEGIN_TIME","BEGIN_TIME_MSEL","END_TIME","END_TIME_MSEL","PROT_CATEGORY","PROT_TYPE","L7_CARRIER_PROT","MSISDN","IMSI","IMEI","ROAM_DIRECTION","MS_IP","SERVER_IP","MS_PORT","SERVER_PORT","APN","SGSN_SIG_IP","GGSN_SIG_IP","SGSN_USER_IP","GGSN_USER_IP","MCC","MNC","RAT","LAC","RAC","SAC","CI","L4_TYPE","L4_UL_THROUGHPUT","L4_DW_THROUGHPUT","L4_UL_GOODPUT","L4_DW_GOODPUT","L4_UL_PACKETS","L4_DW_PACKETS","TCP_CONN_STATES","TAC","ECI","TCP_RTT_STEP1","TCP_UL_RETRANS_WITHPL","TCP_DW_RETRANS_WITHPL","TCP_UL_PACKAGES_WITHPL","TCP_DW_PACKAGES_WITHPL","RAN_NE_USER_IP","TCP_RTT","TCP_UL_OUTOFSEQU","TCP_DW_OUTOFSEQU","TCP_WIN_SIZE","TCP_MSS","INTERFACEID","HOMEMCC","HOMEMNC","HOST","USER_AGENT","FST_URI","TETHERING_FLAG","MS_WIN_STAT_TOTAL_NUM","MS_WIN_STAT_SMALL_NUM","MS_ACK_TO_1STGET_DELAY","SERVER_ACK_TO_1STDATA_DELAY","AVG_UL_RTT","AVG_DW_RTT","UL_RTT_LONG_NUM","DW_RTT_LONG_NUM","UL_RTT_STAT_NUM","DW_RTT_STAT_NUM","USER_PROBE_UL_LOST_PKT","SERVER_PROBE_UL_LOST_PKT","SERVER_PROBE_DW_LOST_PKT","USER_PROBE_DW_LOST_PKT","L7_UL_GOODPUT_FULL_MSS","DATATRANS_UL_DURATION","L7_DW_GOODPUT_FULL_MSS","DATATRANS_DW_DURATION","UL_SMALLPACKET_NUM","DW_SMALLPACKET_NUM","DL_SERIOUS_OUT_OF_ORDER_NUM","DL_SLIGHT_OUT_OF_ORDER_NUM","DL_FLIGHT_TOTAL_SIZE","DL_FLIGHT_TOTAL_NUM","DL_MAX_FLIGHT_SIZE","UL_SERIOUS_OUT_OF_ORDER_NUM","UL_SLIGHT_OUT_OF_ORDER_NUM","UL_FLIGHT_TOTAL_SIZE","UL_FLIGHT_TOTAL_NUM","UL_MAX_FLIGHT_SIZE","USER_DL_SLIGHT_OUT_OF_ORDER_PACKETS","SERVER_UL_SLIGHT_OUT_OF_ORDER_PACKETS","DL_CONTINUOUS_RETRANSMISSION_DELAY","USER_HUNGRY_DELAY","SERVER_HUNGRY_DELAY","SV","SUB_PROT_TYPE","APP_ID","AVG_UL_RTT_MICRO_SEC","AVG_DW_RTT_MICRO_SEC","TRAFFIC_CLASS_NEG","THP_NEG","QCI_NEG","INTERACTIVE_DELAY","INTERACTIVE_NUM","TOTAL_TCP_RTT","TOTAL_TCP_RTT_STEP1","BTS_NODEB_ENODEB_ID","TCP_UL_MAX_MSS","TCP_DL_MAX_MSS","MSACK_2_FIN_DELAY","GET_2_FIRST_DL_DATA_DELAY","LASTACK_2_CLIENTHELLO","SSL_HANDSHAKE_FINISH_2_FIRST_PAYLOAD","FIRST_SSL_UL_CONTINUOUS_TRANS_DELAY","FIRST_SSL_DL_CONTINUOUS_TRANS_DELAY","SERVICE_VALID_FLAG","ENCRYPTED_MODEL_FLAG","COMBINED_FLOW_NUM","ACTIVE_TRANS_NUM","SERVICE_CUTOFF_FLAG","AVG_UL_INTERVAL","AVG_DL_INTERVAL","AVG_UL_JITTER","AVG_DL_JITTER","_corrupt_record")
val im = spark.read.option("delimiter","|").csv(landing_im).toDF(columnas: _*)

// COMMAND ----------

val df = im.
withColumn("year", date_format(from_unixtime(col("BEGIN_TIME")), "yyyy")).
withColumn("month", date_format(from_unixtime(col("BEGIN_TIME")), "MM")).
withColumn("day", date_format(from_unixtime(col("BEGIN_TIME")), "dd")).
withColumn("hour", date_format(from_unixtime(col("BEGIN_TIME")), "HH")).
select(col("BEGIN_TIME").cast("int"),col("BEGIN_TIME_MSEL"),col("END_TIME").cast("int"),col("END_TIME_MSEL"),col("PROT_CATEGORY").cast("int"),col("PROT_TYPE").cast("int"),col("L7_CARRIER_PROT"),col("MSISDN"),col("IMSI"),col("IMEI"),col("ROAM_DIRECTION"),col("MS_IP"),col("SERVER_IP"),col("MS_PORT"),col("SERVER_PORT").cast("int"),col("APN"),col("SGSN_SIG_IP"),col("GGSN_SIG_IP"),col("SGSN_USER_IP"),col("GGSN_USER_IP"),col("MCC"),col("MNC"),col("RAT").cast("short"),col("LAC"),col("RAC"),col("SAC"),col("CI"),col("L4_TYPE"),col("L4_UL_THROUGHPUT").cast("bigint"),col("L4_DW_THROUGHPUT").cast("bigint"),col("L4_UL_GOODPUT"),col("L4_DW_GOODPUT"),col("L4_UL_PACKETS"),col("L4_DW_PACKETS"),col("TCP_CONN_STATES"),col("TAC"),col("ECI"),col("TCP_RTT_STEP1").cast("bigint"),col("TCP_UL_RETRANS_WITHPL"),col("TCP_DW_RETRANS_WITHPL"),col("TCP_UL_PACKAGES_WITHPL"),col("TCP_DW_PACKAGES_WITHPL"),col("RAN_NE_USER_IP"),col("TCP_RTT").cast("bigint"),col("TCP_UL_OUTOFSEQU"),col("TCP_DW_OUTOFSEQU"),col("TCP_WIN_SIZE"),col("TCP_MSS"),col("INTERFACEID").cast("short"),col("HOMEMCC"),col("HOMEMNC"),col("HOST"),col("USER_AGENT"),col("FST_URI"),col("TETHERING_FLAG").cast("short"),col("MS_WIN_STAT_TOTAL_NUM"),col("MS_WIN_STAT_SMALL_NUM"),col("MS_ACK_TO_1STGET_DELAY"),col("SERVER_ACK_TO_1STDATA_DELAY"),col("AVG_UL_RTT"),col("AVG_DW_RTT"),col("UL_RTT_LONG_NUM"),col("DW_RTT_LONG_NUM"),col("UL_RTT_STAT_NUM"),col("DW_RTT_STAT_NUM"),col("USER_PROBE_UL_LOST_PKT"),col("SERVER_PROBE_UL_LOST_PKT"),col("SERVER_PROBE_DW_LOST_PKT"),col("USER_PROBE_DW_LOST_PKT"),col("L7_UL_GOODPUT_FULL_MSS"),col("DATATRANS_UL_DURATION"),col("L7_DW_GOODPUT_FULL_MSS"),col("DATATRANS_DW_DURATION"),col("UL_SMALLPACKET_NUM"),col("DW_SMALLPACKET_NUM"),col("DL_SERIOUS_OUT_OF_ORDER_NUM"),col("DL_SLIGHT_OUT_OF_ORDER_NUM"),col("DL_FLIGHT_TOTAL_SIZE"),col("DL_FLIGHT_TOTAL_NUM"),col("DL_MAX_FLIGHT_SIZE"),col("UL_SERIOUS_OUT_OF_ORDER_NUM"),col("UL_SLIGHT_OUT_OF_ORDER_NUM"),col("UL_FLIGHT_TOTAL_SIZE"),col("UL_FLIGHT_TOTAL_NUM"),col("UL_MAX_FLIGHT_SIZE"),col("USER_DL_SLIGHT_OUT_OF_ORDER_PACKETS"),col("SERVER_UL_SLIGHT_OUT_OF_ORDER_PACKETS"),col("DL_CONTINUOUS_RETRANSMISSION_DELAY"),col("USER_HUNGRY_DELAY"),col("SERVER_HUNGRY_DELAY"),col("SV"),col("SUB_PROT_TYPE").cast("int"),col("APP_ID").cast("int"),col("AVG_UL_RTT_MICRO_SEC"),col("AVG_DW_RTT_MICRO_SEC"),col("TRAFFIC_CLASS_NEG"),col("THP_NEG"),col("QCI_NEG"),col("INTERACTIVE_DELAY"),col("INTERACTIVE_NUM"),col("TOTAL_TCP_RTT"),col("TOTAL_TCP_RTT_STEP1"),col("BTS_NODEB_ENODEB_ID"),col("TCP_UL_MAX_MSS"),col("TCP_DL_MAX_MSS"),col("MSACK_2_FIN_DELAY"),col("GET_2_FIRST_DL_DATA_DELAY"),col("LASTACK_2_CLIENTHELLO"),col("SSL_HANDSHAKE_FINISH_2_FIRST_PAYLOAD"),col("FIRST_SSL_UL_CONTINUOUS_TRANS_DELAY"),col("FIRST_SSL_DL_CONTINUOUS_TRANS_DELAY"),col("SERVICE_VALID_FLAG"),col("ENCRYPTED_MODEL_FLAG"),col("COMBINED_FLOW_NUM"),col("ACTIVE_TRANS_NUM"),col("SERVICE_CUTOFF_FLAG"),col("AVG_UL_INTERVAL"),col("AVG_DL_INTERVAL"),col("AVG_UL_JITTER"),col("AVG_DL_JITTER"),col("_corrupt_record"),col("year"),col("month"),col("day"),col("hour"))


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
