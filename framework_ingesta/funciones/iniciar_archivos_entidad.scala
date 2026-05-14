// Databricks notebook source

var dir_adls = dbutils.widgets.get("dir_adls")
var query_hql = dbutils.widgets.get("query_hql")
var json_schema = dbutils.widgets.get("json_schema")

var nombre_entidad = dir_adls.split("/").last;

// COMMAND ----------

// var dir_adls = "abfss://bigdatadev@stbigdatadev02.dfs.core.windows.net/modelos/ods/service_req_case_hist"
// var nombre_entidad = dir_adls.split("/").last;
// var query_hql = """
// create external table if not exists NOMBREPROCUNITYCATALOGDATABRICKS
// (
// case_key bigint,
// case_desc string,
// service_request_status_key bigint,
// service_request_type_key bigint,
// service_request_severity_key bigint,
// service_request_priority_key bigint,
// queue_key bigint,
// service_req_status_ind string,
// first_call_resolution_ind string,
// cure_code string,
// case_open_time timestamp,
// case_close_time timestamp,
// last_modifed_date timestamp,
// total_phone_time_spent_on_case bigint,
// total_research_time_spent_on_c bigint,
// previous_closing_date timestamp,
// actual_billable_expenses bigint,
// actual_non_billable_expenses bigint,
// case_id_number string,
// case_source_id bigint,
// close_case_source_id bigint,
// subscriber_key bigint,
// address_key bigint,
// contact_key bigint,
// close_case_agent bigint,
// agent_originated_key bigint,
// agent_owner_key bigint,
// product_catalog_key bigint,
// customer_key bigint,
// source_timestamp timestamp,
// etl_crd_dt timestamp,
// etl_upd_dt timestamp,
// dml_ind string,
// batch_id bigint,
// eff_from_date timestamp,
// eff_to_date timestamp,
// service_request_type_level_2_k bigint,
// service_request_type_level_3_k bigint,
// current_ind string,
// case_resler_sales_channel_key bigint,
// report_cust_sales_channel_key bigint,
// activity_log_source_id bigint,
// participant_source_id bigint,
// bigdata_close_date date,
// bigdata_ctrl_id bigint
// )
// location 'LOCATIONMANAGEDCONTAINERHQL'
// """

// var json_schema = """
// {
//   "type": "struct",
//   "fields": [
//     {
//       "name": "case_key",
//       "type": "long",
//       "nullable": true,
//       "metadata": {}
//     },
//     {
//       "name": "case_desc",
//       "type": "string",
//       "nullable": true,
//       "metadata": {}
//     },
//     {
//       "name": "service_request_status_key",
//       "type": "long",
//       "nullable": true,
//       "metadata": {}
//     },
//     {
//       "name": "service_request_type_key",
//       "type": "long",
//       "nullable": true,
//       "metadata": {}
//     },
//     {
//       "name": "service_request_severity_key",
//       "type": "long",
//       "nullable": true,
//       "metadata": {}
//     },
//     {
//       "name": "service_request_priority_key",
//       "type": "long",
//       "nullable": true,
//       "metadata": {}
//     },
//     {
//       "name": "queue_key",
//       "type": "long",
//       "nullable": true,
//       "metadata": {}
//     },
//     {
//       "name": "service_req_status_ind",
//       "type": "string",
//       "nullable": true,
//       "metadata": {}
//     },
//     {
//       "name": "first_call_resolution_ind",
//       "type": "string",
//       "nullable": true,
//       "metadata": {}
//     },
//     {
//       "name": "cure_code",
//       "type": "string",
//       "nullable": true,
//       "metadata": {}
//     },
//     {
//       "name": "case_open_time",
//       "type": "timestamp",
//       "nullable": true,
//       "metadata": {}
//     },
//     {
//       "name": "case_close_time",
//       "type": "timestamp",
//       "nullable": true,
//       "metadata": {}
//     },
//     {
//       "name": "last_modifed_date",
//       "type": "timestamp",
//       "nullable": true,
//       "metadata": {}
//     },
//     {
//       "name": "total_phone_time_spent_on_case",
//       "type": "long",
//       "nullable": true,
//       "metadata": {}
//     },
//     {
//       "name": "total_research_time_spent_on_c",
//       "type": "long",
//       "nullable": true,
//       "metadata": {}
//     },
//     {
//       "name": "previous_closing_date",
//       "type": "timestamp",
//       "nullable": true,
//       "metadata": {}
//     },
//     {
//       "name": "actual_billable_expenses",
//       "type": "long",
//       "nullable": true,
//       "metadata": {}
//     },
//     {
//       "name": "actual_non_billable_expenses",
//       "type": "long",
//       "nullable": true,
//       "metadata": {}
//     },
//     {
//       "name": "case_id_number",
//       "type": "string",
//       "nullable": true,
//       "metadata": {}
//     },
//     {
//       "name": "case_source_id",
//       "type": "long",
//       "nullable": true,
//       "metadata": {}
//     },
//     {
//       "name": "close_case_source_id",
//       "type": "long",
//       "nullable": true,
//       "metadata": {}
//     },
//     {
//       "name": "subscriber_key",
//       "type": "long",
//       "nullable": true,
//       "metadata": {}
//     },
//     {
//       "name": "address_key",
//       "type": "long",
//       "nullable": true,
//       "metadata": {}
//     },
//     {
//       "name": "contact_key",
//       "type": "long",
//       "nullable": true,
//       "metadata": {}
//     },
//     {
//       "name": "close_case_agent",
//       "type": "long",
//       "nullable": true,
//       "metadata": {}
//     },
//     {
//       "name": "agent_originated_key",
//       "type": "long",
//       "nullable": true,
//       "metadata": {}
//     },
//     {
//       "name": "agent_owner_key",
//       "type": "long",
//       "nullable": true,
//       "metadata": {}
//     },
//     {
//       "name": "product_catalog_key",
//       "type": "long",
//       "nullable": true,
//       "metadata": {}
//     },
//     {
//       "name": "customer_key",
//       "type": "long",
//       "nullable": true,
//       "metadata": {}
//     },
//     {
//       "name": "source_timestamp",
//       "type": "timestamp",
//       "nullable": true,
//       "metadata": {}
//     },
//     {
//       "name": "etl_crd_dt",
//       "type": "timestamp",
//       "nullable": true,
//       "metadata": {}
//     },
//     {
//       "name": "etl_upd_dt",
//       "type": "timestamp",
//       "nullable": true,
//       "metadata": {}
//     },
//     {
//       "name": "dml_ind",
//       "type": "string",
//       "nullable": true,
//       "metadata": {}
//     },
//     {
//       "name": "batch_id",
//       "type": "long",
//       "nullable": true,
//       "metadata": {}
//     },
//     {
//       "name": "eff_from_date",
//       "type": "timestamp",
//       "nullable": true,
//       "metadata": {}
//     },
//     {
//       "name": "eff_to_date",
//       "type": "timestamp",
//       "nullable": true,
//       "metadata": {}
//     },
//     {
//       "name": "service_request_type_level_2_k",
//       "type": "long",
//       "nullable": true,
//       "metadata": {}
//     },
//     {
//       "name": "service_request_type_level_3_k",
//       "type": "long",
//       "nullable": true,
//       "metadata": {}
//     },
//     {
//       "name": "current_ind",
//       "type": "string",
//       "nullable": true,
//       "metadata": {}
//     },
//     {
//       "name": "case_resler_sales_channel_key",
//       "type": "long",
//       "nullable": true,
//       "metadata": {}
//     },
//     {
//       "name": "report_cust_sales_channel_key",
//       "type": "long",
//       "nullable": true,
//       "metadata": {}
//     },
//     {
//       "name": "activity_log_source_id",
//       "type": "long",
//       "nullable": true,
//       "metadata": {}
//     },
//     {
//       "name": "participant_source_id",
//       "type": "long",
//       "nullable": true,
//       "metadata": {}
//     }
//   ]
// }
// """

// COMMAND ----------

// package com.tchile.bigdata.etl

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
import org.apache.spark.sql.types.DataType



// COMMAND ----------

// MAGIC %run ./funciones_genericas
// MAGIC

// COMMAND ----------

// MAGIC %md
// MAGIC ### CREACION DE CARPETAS

// COMMAND ----------

//var dir_adls = "abfss://contenedor-test@stbigdatadev02.dfs.core.windows.net/data/pruebas/pruebacarpetas1/pruebacarpetas2/pruebacarpetas3/entidad"
var cont = 0
var lista_carpetas = dir_adls.split("/")
var actual_path = ""
for (carpeta <- lista_carpetas) {
    if(cont > 0){
      actual_path = actual_path + "/" + carpeta
    }else{
      actual_path = actual_path + "" + carpeta
    }
    if(cont > 3){
      println("carpeta: " + carpeta)
      println("actual_path: " + actual_path)
      var existe = exists_file(actual_path)
      println("existe: " + existe)
      if(!existe){
        mkdir(actual_path)
      }
    }
    cont = cont+1
}
println("actual_path: " + actual_path)

// COMMAND ----------

mkdir(dir_adls+"/processing")
mkdir(dir_adls+"/processing_error")
mkdir(dir_adls+"/stage")
mkdir(dir_adls+"/stage_error")
mkdir(dir_adls+"/landing")
mkdir(dir_adls+"/raw")

// Directorios añadidos para procesos ODS full-inc
mkdir(dir_adls+"/mensual")
mkdir(dir_adls+"/diario")

// COMMAND ----------

// MAGIC %md
// MAGIC ### CREACION DE HQL

// COMMAND ----------

makeTxtFile(dir_adls + "/"+ nombre_entidad +".hql", query_hql)

// COMMAND ----------

// MAGIC %md
// MAGIC ### CREACION DE JSON

// COMMAND ----------

makeTxtFile(dir_adls + "/"+ nombre_entidad +".json", json_schema)


// COMMAND ----------

// MAGIC %md
// MAGIC ### CREACION DE ARCHIVO MAX TIMESTAMP

// COMMAND ----------

var fecha_aux = "2000-01-01T01:01:01.001Z"
makeTxtFile(dir_adls + "/last_ingest_time.txt", "timestamp;\n"+fecha_aux+";")

// COMMAND ----------

// var path_df = "abfss://contenedor-test@stbigdatadev02.dfs.core.windows.net/data/finanzas/facturacion/kfactor/ingreso3/stage"
// val df = spark.read.format("parquet").load(path_df)
// display(df)
