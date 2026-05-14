// Databricks notebook source
import java.text.SimpleDateFormat
import java.util.{Date, TimeZone}
import java.time.Instant


dbutils.widgets.text("adls_container","abfss://ingestas@stbigdatadev02.dfs.core.windows.net")
val adls_container = dbutils.widgets.get("adls_container")

dbutils.widgets.text("dir_adls_rel","/data/trafico/trafico_red/radius")
val dir_adls_rel = dbutils.widgets.get("dir_adls_rel")

dbutils.widgets.text("max_date","2024-10-01T02:50:11Z")
val max_date = dbutils.widgets.get("max_date")
//val max_date = "2025-01-03T02:54:01.000Z"

if (max_date.matches("\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}Z")) {

  //format_timestamp.setTimeZone(TimeZone.getTimeZone("Chile/Continental"))

  val format_timestamp = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'")
  format_timestamp.setTimeZone(TimeZone.getTimeZone("UTC"))


  val max_date_format = format_timestamp.parse(max_date)

  val max_date_format_1 = Date.from(max_date_format.toInstant.plusSeconds(1))

  // Crear un nuevo formato con milisegundos
  val format_with_millis = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")
  format_with_millis.setTimeZone(TimeZone.getTimeZone("UTC"))
  
  // Formatear la fecha con milisegundos
  val max_date_format_1_with_millis = format_with_millis.format(max_date_format_1)

  println("segundos mili: "+max_date_format_1_with_millis)
  dbutils.fs.put(adls_container+dir_adls_rel+"/last_ingest_time.txt","timestamp;\n"+max_date_format_1_with_millis+";", overwrite = true)
}
else if (max_date.matches("\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}\\.\\d{3}Z")) {
  val format_timestamp = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")
  format_timestamp.setTimeZone(TimeZone.getTimeZone("UTC"))

  val max_date_format = format_timestamp.parse(max_date)
  val max_date_format_1 = Date.from(max_date_format.toInstant.plusSeconds(1))

  // Crear un nuevo formato con milisegundos
  val format_with_millis = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")
  format_with_millis.setTimeZone(TimeZone.getTimeZone("UTC"))
  
  // Formatear la fecha con milisegundos
  val max_date_format_1_with_millis = format_with_millis.format(max_date_format_1)

  println("mili: "+max_date_format_1_with_millis)
  dbutils.fs.put(adls_container+dir_adls_rel+"/last_ingest_time.txt","timestamp;\n"+max_date_format_1_with_millis+";", overwrite = true)
}
//makeTxtFile(adls_container + "/last_ingest_time.txt", "timestamp;\n"+string_fecha_modificacion_actual_plus_1+";")
//dbutils.fs.ls("abfss://bigdatadev@stbigdatadev02.dfs.core.windows.net/data/trafico/trafico_red/radius/procesado")
