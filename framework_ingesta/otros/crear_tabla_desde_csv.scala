// Databricks notebook source
val filepath = "abfss://bigdata@stbigdatadev02.dfs.core.windows.net".concat(dbutils.widgets.get("filepath"))
//filepath -> la ruta del csv en el datalake, por ejemplo: 
//"abfss://bigdata@stbigdatadev02.dfs.core.windows.net/modelos/interacciones/tbf_intencion_baja/stagging/tmp_subscribers_rollouts.csv/"

val table_name = dbutils.widgets.get("table_name") //"tmp_subscribers_rollouts"
val catalogo = dbutils.widgets.get("catalogo") //"desarrollo"
val data_base = dbutils.widgets.get("data_base") //"amdocs_fijo"
val delimiter = dbutils.widgets.get("delimiter") //","
val header_settings = dbutils.widgets.get("header_true_or_false") //"true"

val outputpath = "abfss://bigdata@stbigdatadev02.dfs.core.windows.net".concat(dbutils.widgets.get("outputpath"))
//outputpath -> la ruta de output como tabla delta, por ejemplo:
//"abfss://bigdata@stbigdatadev02.dfs.core.windows.net/modelos/interacciones/tbf_intencion_baja/stagging/tmp_subscribers_rollouts/"

// COMMAND ----------

val dropQuery = s"DROP TABLE IF EXISTS $catalogo.$data_base.$table_name"

// COMMAND ----------

// MAGIC %sql
// MAGIC $dropQuery

// COMMAND ----------

val df = spark.read
            .option("header", header_settings)
            .option("sep", delimiter)
            .csv(filepath)

display(df)

// COMMAND ----------

    dfRenamed.write.mode("overwrite")
            .format("delta")
            .option("path", outputpath)
            .saveAsTable(
                        catalogo.concat(".").concat(data_base).concat(".").concat(table_name)
                        )
