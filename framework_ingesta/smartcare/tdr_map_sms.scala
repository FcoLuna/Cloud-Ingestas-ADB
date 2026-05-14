// Databricks notebook source
dbutils.widgets.text("moment","2024/11/05/15/0/")
dbutils.widgets.text("path_adls","abfss://bigdatadev@stbigdatadev02.dfs.core.windows.net")
dbutils.widgets.text("ruta_origen","/data/trafico/trafico_detalle/smartcare/landing_traf_cert/tdr_map_sms/")
dbutils.widgets.text("path_salida","/data/trafico/trafico_detalle/smartcare/landing_traf_cert/tdr_map_sms/")
dbutils.widgets.text("catalogo","bidesarrollo")
dbutils.widgets.text("tabla_salida","trafico_cert.smartcare_tdr_map_sms") //raw_trafico.aiumoc (cuando pase a prod)

// COMMAND ----------

val moment = dbutils.widgets.get("moment")
val path_adls = dbutils.widgets.get("path_adls")
val catalogo = dbutils.widgets.get("catalogo")
val landing_tdr_map_sms = path_adls + dbutils.widgets.get("ruta_origen") + moment
val tabla_salida = catalogo +"."+ dbutils.widgets.get("tabla_salida")
val path_salida = path_adls + dbutils.widgets.get("path_salida")

// COMMAND ----------



// COMMAND ----------

val tdr_map_sms = spark.read.option("delimiter","|").csv(landing_tdr_map_sms)
display(tdr_map_sms)

// COMMAND ----------

val columnas = Seq("TDRID","REFID","STARTTIME","MILLISEC","SRVSTAT","CDRSTAT","NI","OPC","OSSN","OGT","DPC","DSSN","DGT","LINKSETID","SRV_TYPE","IMSI","TMSI","MSISDN","MSCNO","SMSCNO","PMSISDN","SMS_LEN","END_TIME","RELTYPE","CAUSE","ENCRYPT_VERSION","RESERVED1","RESERVED2","RESERVED3","RESERVED4","RESERVED5","RESERVED6","RESERVED7","RESERVED8","FILELOCATION","OFFSET_DSI","PROBEID","GROUPID","ROAM_DIRECTION","HOMEMCC","HOMEMNC","VISITCC","VISITNDC","SIG_COLLECTION_TYPE","LNKOPC","LNKDPC","LNKSRCIP","LNKSRCPORT","LNKDESTIP","LNKDESTPORT","VASSERVICETYPE","SEGREF","SEGTOTALNUM","SEGSEQ","TPMTI","PD","OPPTYPE","FIRFAILTIME","PREPAID_FLAG","LAYER1ID","LAYER2ID","LAYER3ID","LAYER4ID","LAYER5ID","LAYER6ID","SMSC_TIMESTAMP","RSP_LNK1_TYPE","RSP_LNK1_SIG_TYPE","RSP_LNK1_OPC","RSP_LNK1_DPC","RSP_LNK1_SLC","RSP_LNK1_SRCIP","RSP_LNK1_SRCPORT","RSP_LNK1_DESTIP","RSP_LNK1_DESTPORT","RSP_LNK2_TYPE","RSP_LNK2_SIG_TYPE","RSP_LNK2_OPC","RSP_LNK2_DPC","RSP_LNK2_SLC","RSP_LNK2_SRCIP","RSP_LNK2_SRCPORT","RSP_LNK2_DESTIP","RSP_LNK2_DESTPORT","RSP_LNK3_TYPE","RSP_LNK3_SIG_TYPE","RSP_LNK3_OPC","RSP_LNK3_DPC","RSP_LNK3_SLC","RSP_LNK3_SRCIP","RSP_LNK3_SRCPORT","RSP_LNK3_DESTIP","RSP_LNK3_DESTPORT","RSP_LNK4_TYPE","RSP_LNK4_SIG_TYPE","RSP_LNK4_OPC","RSP_LNK4_DPC","RSP_LNK4_SLC","RSP_LNK4_SRCIP","RSP_LNK4_SRCPORT","RSP_LNK4_DESTIP","RSP_LNK4_DESTPORT","USER_CATEGORY","BSS_IND","IMEI","CUSTOMER_TYPE","SRV_ATTR_ID","FAIL_CATEGORY","RULE_CODE","SV","CALC_TIME","c1","c2")
val tdr_map_sms = spark.read.option("delimiter","|").csv(landing_tdr_map_sms).toDF(columnas: _*)

// COMMAND ----------

tdr_map_sms.show(3,false)

// COMMAND ----------

val df = tdr_map_sms.
withColumn("year", date_format(from_unixtime(col("STARTTIME")), "yyyy")).
withColumn("month", date_format(from_unixtime(col("STARTTIME")), "MM")).
withColumn("day", date_format(from_unixtime(col("STARTTIME")), "dd")).
withColumn("hour", date_format(from_unixtime(col("STARTTIME")), "HH")).
select(col("TDRID"),col("REFID"),col("STARTTIME").cast("int"),col("MILLISEC"),col("SRVSTAT"),col("CDRSTAT"),col("NI"),col("OPC"),col("OSSN"),col("OGT"),col("DPC"),col("DSSN"),col("DGT"),col("LINKSETID"),col("SRV_TYPE"),col("IMSI"),col("TMSI"),col("MSISDN"),col("MSCNO"),col("SMSCNO"),col("PMSISDN"),col("SMS_LEN"),col("END_TIME").cast("int"),col("RELTYPE"),col("CAUSE"),col("ENCRYPT_VERSION"),col("RESERVED1"),col("RESERVED2"),col("RESERVED3"),col("RESERVED4"),col("RESERVED5"),col("RESERVED6"),col("RESERVED7"),col("RESERVED8"),col("FILELOCATION"),col("OFFSET_DSI"),col("PROBEID"),col("GROUPID"),col("ROAM_DIRECTION"),col("HOMEMCC"),col("HOMEMNC"),col("VISITCC"),col("VISITNDC"),col("SIG_COLLECTION_TYPE"),col("LNKOPC"),col("LNKDPC"),col("LNKSRCIP"),col("LNKSRCPORT"),col("LNKDESTIP"),col("LNKDESTPORT"),col("VASSERVICETYPE"),col("SEGREF"),col("SEGTOTALNUM"),col("SEGSEQ"),col("TPMTI"),col("PD"),col("OPPTYPE"),col("FIRFAILTIME"),col("PREPAID_FLAG"),col("LAYER1ID"),col("LAYER2ID"),col("LAYER3ID"),col("LAYER4ID"),col("LAYER5ID"),col("LAYER6ID"),col("SMSC_TIMESTAMP"),col("RSP_LNK1_TYPE"),col("RSP_LNK1_SIG_TYPE"),col("RSP_LNK1_OPC"),col("RSP_LNK1_DPC"),col("RSP_LNK1_SLC"),col("RSP_LNK1_SRCIP"),col("RSP_LNK1_SRCPORT"),col("RSP_LNK1_DESTIP"),col("RSP_LNK1_DESTPORT"),col("RSP_LNK2_TYPE"),col("RSP_LNK2_SIG_TYPE"),col("RSP_LNK2_OPC"),col("RSP_LNK2_DPC"),col("RSP_LNK2_SLC"),col("RSP_LNK2_SRCIP"),col("RSP_LNK2_SRCPORT"),col("RSP_LNK2_DESTIP"),col("RSP_LNK2_DESTPORT"),col("RSP_LNK3_TYPE"),col("RSP_LNK3_SIG_TYPE"),col("RSP_LNK3_OPC"),col("RSP_LNK3_DPC"),col("RSP_LNK3_SLC"),col("RSP_LNK3_SRCIP"),col("RSP_LNK3_SRCPORT"),col("RSP_LNK3_DESTIP"),col("RSP_LNK3_DESTPORT"),col("RSP_LNK4_TYPE"),col("RSP_LNK4_SIG_TYPE"),col("RSP_LNK4_OPC"),col("RSP_LNK4_DPC"),col("RSP_LNK4_SLC"),col("RSP_LNK4_SRCIP"),col("RSP_LNK4_SRCPORT"),col("RSP_LNK4_DESTIP"),col("RSP_LNK4_DESTPORT"),col("USER_CATEGORY"),col("BSS_IND"),col("IMEI"),col("CUSTOMER_TYPE"),col("SRV_ATTR_ID"),col("FAIL_CATEGORY"),col("RULE_CODE"),col("SV"),col("CALC_TIME"),col("c1"),col("c2"),col("year"),col("month"),col("day"),col("hour"))


// COMMAND ----------

df.printSchema

// COMMAND ----------

display(df)

// COMMAND ----------

println(path_salida)
println(tabla_salida)

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

// COMMAND ----------

// MAGIC %sql
// MAGIC select * from bidesarrollo.trafico_cert.smartcare_tdr_map_sms
