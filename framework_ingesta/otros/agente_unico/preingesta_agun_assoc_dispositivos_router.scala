// Databricks notebook source
import org.apache.spark.sql.{DataFrame, Row, Column}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

// COMMAND ----------

val delimitador = dbutils.widgets.get("delimitador")
val encabezado = dbutils.widgets.get("encabezado")
val path_stage = dbutils.widgets.get("path_stage")
val dateFormatFile = dbutils.widgets.get("dateFormatFile")
val timestampFormat = dbutils.widgets.get("timestampFormat")
val filename = dbutils.widgets.get("filename")

// COMMAND ----------

// Columns for concatenation
val columnsForConcat = Array(
  col("fecha"),
  lit(" "),
  col("hora"),
  lit(":00")
)

// Columns for selection
val columnsForSelect = Array(
  col("new_timest_fh").alias("timest_fh").cast(TimestampType),
  col("ap_de").alias("ap_de").cast(StringType),
  col("sta_de").alias("sta_de").cast(StringType),
  col("v_iface_de").alias("v_iface_de").cast(StringType),
  col("cnt_nr").alias("cnt_nr").cast(StringType),
  col("rssi_dbm_nr").alias("rssi_dbm_nr").cast(StringType),
  col("rssi_dbm_max_nr").alias("rssi_dbm_max_nr").cast(StringType),
  col("rssi_dbm_min_nr").alias("rssi_dbm_min_nr").cast(StringType),
  col("rssi_dbm_med_nr").alias("rssi_dbm_med_nr").cast(StringType),
  col("tx_phy_rate_nr").alias("tx_phy_rate_nr").cast(StringType),
  col("tx_phy_rate_max_nr").alias("tx_phy_rate_max_nr").cast(StringType),
  col("tx_phy_rate_min_nr").alias("tx_phy_rate_min_nr").cast(StringType),
  col("tx_phy_rate_med_nr").alias("tx_phy_rate_med_nr").cast(StringType),
  col("rx_phy_rate_nr").alias("rx_phy_rate_nr").cast(StringType),
  col("assoc_time_nr").alias("assoc_time_nr").cast(StringType),
  col("tx_bytes_nr").alias("tx_bytes_nr").cast(StringType),
  col("rx_bytes_nr").alias("rx_bytes_nr").cast(StringType),
  col("tx_mcs_nr").alias("tx_mcs_nr").cast(StringType),
  col("rx_mcs_nr").alias("rx_mcs_nr").cast(StringType),
  col("protocol_nr").alias("protocol_nr").cast(StringType),
  col("channel_nr").alias("channel_nr").cast(StringType),
  col("bandwidth_nr").alias("bandwidth_nr").cast(StringType),
  col("cca_int_nr").alias("cca_int_nr").cast(StringType),
  col("cca_int_max_nr").alias("cca_int_max_nr").cast(StringType),
  col("cca_int_min_nr").alias("cca_int_min_nr").cast(StringType),
  col("cca_int_med_nr").alias("cca_int_med_nr").cast(StringType),
  col("cca_idle_nr").alias("cca_idle_nr").cast(StringType),
  col("rx_cnt_crc_nr").alias("rx_cnt_crc_nr").cast(StringType),
  col("vendor_de").alias("vendor_de").cast(StringType),
  col("serial_number_de").alias("serial_number_de").cast(StringType),
  col("user_id").alias("user_id").cast(StringType),
  col("product_class_de").alias("product_class_de").cast(StringType)
  //col("bigdata_close_date").alias("bigdata_close_date").cast(DateType),
  //col("bigdata_ctrl_id").alias("bigdata_ctrl_id").cast(LongType)
)

// Assuming path_landing_pre, bigdata_close_date, and bigdata_ctrl_id are already defined
val df_assoc = spark.read
  .option("inferSchema", "false")
  .option("header", encabezado)
  .option("delimiter", delimitador)
  .csv(path_stage)
  //.withColumn("bigdata_close_date", lit(bigdata_close_date))
  //.withColumn("bigdata_ctrl_id", lit(bigdata_ctrl_id))
  .withColumn("new_timest_fh", regexp_replace(col("timest_fh"), "-", ":"))
  .withColumn("fecha", substring(col("timest_fh"), 1, 10))
  .withColumn("hora", substring(col("new_timest_fh"), 12, 5))
  .withColumn("new_timest_fh", concat(columnsForConcat: _*))
  .select(columnsForSelect: _*)
  .dropDuplicates()

// COMMAND ----------

var rows = "0"

// COMMAND ----------

try {
  if(df_assoc.count() > 0) {
    df_assoc.repartition(1).write
      .mode(SaveMode.Overwrite)
      .option("header", encabezado)
      .option("delimiter", delimitador)
      .csv(path_stage)

    val lista_archivos = dbutils.fs.ls(path_stage)
    for (elemento <- lista_archivos) {
      if (elemento.path.endsWith(".csv")) {
        dbutils.fs.mv(elemento.path, path_stage + "/" + filename)
      } else {
        dbutils.fs.rm(elemento.path)
      }
    }
  }
  rows = df_assoc.count().toString
}
catch {
  case e: Exception => println("Error en archivo: " + e.getMessage)
  rows = "0"
}

// COMMAND ----------

dbutils.notebook.exit(rows)
