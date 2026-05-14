// Databricks notebook source
dbutils.widgets.text("ruta_adls", "/data/interacciones/canales/atenciones_suc/tch_encuestas_checkin_envios")
dbutils.widgets.text("nombre_tabla", "raw_interacciones.tch_encuestas_checkin_envios")
dbutils.widgets.text("catalogo", "bi_ingestas")
dbutils.widgets.text("contenedor", "abfss://ingestas@stbigdatadev02.dfs.core.windows.net")

// COMMAND ----------

//ruta encuesta
val ruta_adls = dbutils.widgets.get("ruta_adls")
val tabla_envios = dbutils.widgets.get("nombre_tabla")
val catalogo = dbutils.widgets.get("catalogo")
val contenedor = dbutils.widgets.get("contenedor")

// COMMAND ----------

val path = contenedor + ruta_adls + "/raw"
val path_tmp = contenedor + ruta_adls + "/tmp"
val df = spark.sql(s"select * from $catalogo.$tabla_envios")//sparkh.read.parquet(path)
df.createOrReplaceTempView("data")
val year = spark.sql(s"""select max(year(a.bigdata_close_date)) from data a""").collectAsList.toString.replace("[", "").replace("]", "")

//val path_inicial = ruta_adls + "/raw/year=" + year

//val files = dbutils.fs.ls(s"$contenedor$path_inicial")
//val fileNames: Array[String] = files.map(_.name).toArray

//val nameParquet = fileNames.mkString("")
val pathResp = contenedor + ruta_adls+"/raw_respaldo" // /year=" + year
val df_resp = spark.sql(s"select * from $catalogo.$tabla_envios where year = $year")

//df_resp.repartition(1).write.option("header", "true").mode("overwrite").option("partitionOverwriteMode", "dynamic").partitionBy("year").format("delta").save(pathResp)
df_resp.repartition(1).write.option("header", "true").option("mergeSchema", "true").mode("overwrite").option("partitionOverwriteMode", "dynamic").partitionBy("year", "month", "day").format("delta").save(pathResp)

//val file_parquet_resp = dbutils.fs.ls(pathResp).filter(_.name.startsWith("part")).head.name
//dbutils.fs.mv(pathResp + "/"+file_parquet_resp, pathResp + "/" + nameParquet)

// COMMAND ----------

val df_datos = spark.sql(
        s"""SELECT a.id_turno, a.id_encuesta, a.nom_encuesta, a.id_encuesta_envio,
          |a.fecha_hora_creacion, a.fecha_hora_envio, a.bigdata_close_date,
          |a.bigdata_ctrl_id, a.year, a.month, a.day
          |FROM $catalogo.$tabla_envios a""".stripMargin)
df_datos.createOrReplaceTempView("duplicados")

val df_raw_max_ctrlid = spark.sql(
        """SELECT a.id_encuesta_envio, max(a.bigdata_ctrl_id) as bigdata_ctrl_id
          |FROM duplicados a GROUP BY a.id_encuesta_envio""".stripMargin)
df_raw_max_ctrlid.createOrReplaceTempView("id_unicos")

val df_cruce_final = spark.sql(
  """SELECT DISTINCT a.id_turno, a.id_encuesta, a.nom_encuesta,
    |a.id_encuesta_envio, a.fecha_hora_creacion, a.fecha_hora_envio,
    |a.bigdata_close_date, a.bigdata_ctrl_id, a.year, a.month, a.day
    |FROM duplicados a
    |INNER JOIN id_unicos b
    |ON b.id_encuesta_envio = a.id_encuesta_envio
    |AND a.bigdata_ctrl_id = b.bigdata_ctrl_id""".stripMargin)
//df_cruce_final.createOrReplaceTempView("cruce_final")

df_cruce_final.repartition(1).write.option("header", "true").mode("overwrite").format("delta").save(path_tmp)

val df_final = spark.read.format("delta").load(path_tmp)
//df_final.repartition(1).write.option("header", "true").mode("overwrite").option("partitionOverwriteMode", "dynamic").partitionBy("year").format("delta").save(path)

df_final.repartition(1).write.option("header", "true").mode("overwrite").option("partitionOverwriteMode", "dynamic").partitionBy("year","month","day").format("delta").save(path)

//val file_parquet_final = dbutils.fs.ls(path_inicial).filter(_.name.startsWith("part")).head.name
//dbutils.fs.mv(path_inicial + "/" + file_parquet_final, path_inicial + "/" + nameParquet)

dbutils.fs.rm(path_tmp, true)

spark.sql(s"FSCK REPAIR TABLE $catalogo.$tabla_envios")
