// Databricks notebook source
import java.text.SimpleDateFormat
import java.time.LocalDateTime
import java.util.Date
import java.time.format.DateTimeFormatter
import java.util.concurrent.TimeUnit
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.functions._
import spark.implicits._
import org.apache.spark.sql.types.DataType

// COMMAND ----------

// val nombre_catalogo_schema = "desarrollo.stg_vision360"
// val dir_hdfs = "abfss://bigdata@stbigdatadev02.dfs.core.windows.net/data/persona/datos_demograficos/dem_tldm/dem_direccion"
// spark.sql(s"""CREATE SCHEMA IF NOT EXISTS $nombre_catalogo_schema MANAGED LOCATION '$dir_hdfs'""")

// spark.sql(s"""drop TABLE if exists $nomb_proc""")
// spark.sql("drop SCHEMA if exists desarrollo.amdocs_fijo cascade")
// spark.sql("create SCHEMA if not exists desarrollo.odschile_hist")

// spark.sql("drop TABLE if exists desarrollo.stg_prospectos.sigres_equipo_fisico")
// spark.sql("drop TABLE if exists desarrollo.producto_asignado.tbf_prq_suscriptor")
// spark.sql("CREATE EXTERNAL TABLE IF NOT EXISTS desarrollo.stg_prospectos.sigres_equipo_fisico LOCATION 'abfss://bigdata@stbigdatadev02.dfs.core.windows.net/data/gestion_recursos/inventario_red/sigres/sigres_equipo_fisico/raw'")
// spark.sql("drop table desarrollo.stg_prospectos.sigres_equipo_fisico ")
// spark.sql("create external table desarrollo.stg_prospectos.sigres_equipo_fisico location 'abfss://bigdatadev@stbigdatadev02.dfs.core.windows.net/data/gestion_recursos/inventario_red/sigres/sigres_equipo_fisico/raw'")
// spark.sql(s"DROP TABLE IF EXISTS desarrollo.amdocs_fijo.mod_tmp_maxfec_sigres")
// var df = spark.read.parquet("abfss://bigdata@stbigdatadev02.dfs.core.windows.net/modelos/producto_asignado/parque_fijo_suscriptor/temp_test")

// df = df.withColumn("year",lit("2023"))
// df = df.withColumn("month",lit("01"))

// df.repartition(20).write.partitionBy("year", "month").mode("append").format("delta").option("path", "abfss://bigdata@stbigdatadev02.dfs.core.windows.net/modelos/producto_asignado/parque_fijo_suscriptor/conformado").saveAsTable("desarrollo.producto_asignado.tbf_prq_suscriptor")
// display(df)

// COMMAND ----------

// MAGIC %python
// MAGIC # %scala
// MAGIC # var df = spark.read
// MAGIC #   .option("header", "true")
// MAGIC #   .option("quote",";").
// MAGIC #   option("sep",";")
// MAGIC #   .csv("abfss://bigdata@stbigdatadev02.dfs.core.windows.net/modelos/producto/param_main_cmp_service_type/param_main_cmp_service_type.csv")
// MAGIC
// MAGIC # val dfWithoutQuotes = df.columns.foldLeft(df)((accDF, colName) =>
// MAGIC #   accDF.withColumn(colName, regexp_replace(col(colName), "\"", ""))
// MAGIC # )
// MAGIC # // spark.sql("drop table if exists desarrollo.producto.param_main_cmp_service_type")
// MAGIC # dfWithoutQuotes.write
// MAGIC #   .format("parquet")
// MAGIC #   .option("path", "abfss://bigdata@stbigdatadev02.dfs.core.windows.net/modelos/producto/param_main_cmp_service_type/tmp")
// MAGIC #   .saveAsTable("desarrollo.producto.param_main_cmp_service_type")
// MAGIC  

// COMMAND ----------

// MAGIC %python
// MAGIC
// MAGIC file_path = "/dbfs/FileStore/librerias/tqdm-4.66.2.tar.gz"  
// MAGIC
// MAGIC dbutils.fs.cp("file:" + file_path, "dbfs:/tmp/tqdm-4.66.2.tar.gz")  # Copiar el archivo al sistema de archivos distribuido de Databricks
// MAGIC dbutils.library.install("dbfs:/tmp/tqdm-4.66.2.tar.gz")

// COMMAND ----------

// MAGIC %sql 
// MAGIC -- drop table if exists desarrollo.amdocs_fijo.ext_tmp_prd_billing_prm_hist;
// MAGIC -- drop table if exists desarrollo.amdocs_fijo.ext_assigned_product_hist;
// MAGIC -- drop table if exists desarrollo.amdocs_fijo.ext_tmp_assigned_product_v0_hist;
// MAGIC -- drop table if exists desarrollo.amdocs_fijo.ext_tmp_assigned_billing_offer_hist;
// MAGIC -- drop table if exists desarrollo.amdocs_fijo.ext_tmp_assigned_billing_offer_v0_hist;
// MAGIC -- drop table if exists desarrollo.amdocs_fijo.ext_tmp_subscribers_hist;
// MAGIC -- drop table if exists desarrollo.amdocs_fijo.ext_tmp_cust_hist;
// MAGIC -- drop table if exists desarrollo.amdocs_fijo.ext_tmp_contact_hist;
// MAGIC -- drop table if exists desarrollo.amdocs_fijo.ext_tmp_subscriber_contact_rel_hist;
// MAGIC -- drop table if exists desarrollo.amdocs_fijo.ext_tmp_billing_arrangement_hist;
// MAGIC -- drop table if exists desarrollo.amdocs_fijo.ext_tmp_charge_distribution_hist;
// MAGIC -- drop table if exists desarrollo.amdocs_fijo.ext_tmp_prd_financial_account_hist;
// MAGIC -- drop table if exists desarrollo.amdocs_fijo.ext_tmp_prd_product_hist;
// MAGIC -- drop table if exists desarrollo.amdocs_fijo.ext_tmp_prd_subscriber_main_offer_hist;
// MAGIC -- drop table if exists desarrollo.amdocs_fijo.ext_tmp_prd_pay_channel_hist;
// MAGIC -- drop table if exists desarrollo.amdocs_fijo.ext_tmp_prd_ar_debit_hist;
// MAGIC -- drop table if exists desarrollo.amdocs_fijo.ext_tmp_prd_collection_account_hist;
// MAGIC -- drop table if exists desarrollo.amdocs_fijo.ext_tmp_prd_payment_arrangement_inst_hist;
// MAGIC -- drop table if exists desarrollo.amdocs_fijo.ext_tmp_prd_recurring_charge_request_hist;
// MAGIC -- drop table if exists desarrollo.raw_gestion_recursos.dmps_entrecalles;
// MAGIC --drop table if exists desarrollo.interacciones.tbf_intencion_baja;
