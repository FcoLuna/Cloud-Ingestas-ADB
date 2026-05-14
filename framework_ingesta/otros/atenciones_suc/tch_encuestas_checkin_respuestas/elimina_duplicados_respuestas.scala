// Databricks notebook source
dbutils.widgets.text("ruta_adls", "/data/interacciones/canales/atenciones_suc/tch_encuestas_checkin_respuestas")
dbutils.widgets.text("nombre_tabla", "raw_interacciones.tch_encuestas_checkin_respuestas")
dbutils.widgets.text("catalogo", "bi_ingestas")
dbutils.widgets.text("contenedor", "abfss://ingestas@stbigdatadev02.dfs.core.windows.net")

// COMMAND ----------

//ruta encuesta
val ruta_adls = dbutils.widgets.get("ruta_adls")
val tabla_respuestas = dbutils.widgets.get("nombre_tabla")
val catalogo = dbutils.widgets.get("catalogo")
val contenedor = dbutils.widgets.get("contenedor")

// COMMAND ----------

val df = spark.sql(s"select * from $catalogo.$tabla_respuestas")
df.createOrReplaceTempView("data")
val year = spark.sql(s"""SELECT MAX(year(bigdata_close_date)) FROM data""").collectAsList.toString.replace("[", "").replace("]", "")

val path = s"$contenedor$ruta_adls/raw"
val path_resp = s"$contenedor$ruta_adls/raw_respaldo/"
val path_tmp = s"$contenedor$ruta_adls/tmp"

val df_resp = spark.sql(s"select * from $catalogo.$tabla_respuestas where year = $year")
df_resp.repartition(1).write
  .option("header", "true")
  .mode("overwrite")
  .option("mergeSchema", "true")
  .option("partitionOverwriteMode", "dynamic")
  .partitionBy("year", "month", "day")
  .format("delta")
  .save(path_resp)

val df_datos = spark.sql(
  s"""SELECT a.id_turno, a.id_pregunta, a.valor_respuesta, a.verbatim_respuesta, 
    a.fecha_hora_respuesta, a.bigdata_close_date, a.bigdata_ctrl_id, a.year, a.month, a.day
    FROM $catalogo.$tabla_respuestas a"""
)
df_datos.createOrReplaceTempView("duplicados")

val df_raw_max_ctrlid = spark.sql(
  s"""SELECT a.id_turno, a.id_pregunta, a.fecha_hora_respuesta, 
    MAX(a.bigdata_ctrl_id) AS bigdata_ctrl_id 
    FROM $catalogo.$tabla_respuestas a
    GROUP BY a.id_turno, a.id_pregunta, a.fecha_hora_respuesta"""
)
df_raw_max_ctrlid.createOrReplaceTempView("id_unicos")

val df_cruce_final = spark.sql(
  s"""SELECT DISTINCT b.id_turno, b.id_pregunta, a.valor_respuesta, 
    a.verbatim_respuesta, b.fecha_hora_respuesta, a.bigdata_close_date, 
    b.bigdata_ctrl_id, a.year, a.month, a.day
    FROM duplicados a
    INNER JOIN id_unicos b ON b.id_turno = a.id_turno 
    AND a.fecha_hora_respuesta = b.fecha_hora_respuesta 
    AND a.id_pregunta = b.id_pregunta 
    AND a.bigdata_ctrl_id = b.bigdata_ctrl_id"""
  
    
)
df_cruce_final.createOrReplaceTempView("cruce_final")

df_cruce_final.repartition(1).write
  .option("header", "true")
  .mode("overwrite")
  .format("delta")
  .save(path_tmp)

val df_final = spark.read.format("delta").load(path_tmp)
df_final.repartition(1).write
  .option("header", "true")
  .mode("overwrite")
  .option("partitionOverwriteMode", "dynamic")
  .partitionBy("year", "month", "day")
  .format("delta")
  .save(path)

dbutils.fs.rm(path_tmp, true)

spark.sql(s"FSCK REPAIR TABLE $catalogo.$tabla_respuestas")
