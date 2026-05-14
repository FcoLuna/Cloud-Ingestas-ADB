// Databricks notebook source
// MAGIC %md
// MAGIC ### Actualizacion de archivo last_ingest_time_mensual.txt

// COMMAND ----------

dbutils.widgets.text("original_file_date","2025-03-06T00:00:28.01Z")
dbutils.widgets.text("dir_adls","abfss://ingestas@stbigdatadev02.dfs.core.windows.net/modelos/producto_asignado/parque_fijo_suscriptor_producto_cg")

// COMMAND ----------

import java.text.SimpleDateFormat
import java.time.LocalDateTime
import java.util.Date
import java.time.format.DateTimeFormatter

// COMMAND ----------

// MAGIC %md
// MAGIC ### Variables

// COMMAND ----------

//PARAMETROS QUE SE PASAN DESDE ADF
val original_file_date = dbutils.widgets.get("original_file_date")
val dir_adls           = dbutils.widgets.get("dir_adls")

// Define los 3 tipos de formateo de fecha 1) 20240423 11:33:23  2) 2024-04-23T08:30:45.123Z 3) 2024-04-23T08:30:45Z
var format_actual = new java.text.SimpleDateFormat("yyyyMMdd HHmmss")
var format_max = new java.text.SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'") 
var format_timestamp = new java.text.SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'") 

// COMMAND ----------

// MAGIC %run /Workspace/Repos/ingestas/Cloud-Ingestas-ADB/framework_ingesta/funciones/funciones_genericas

// COMMAND ----------

try {
  // A la fecha original del archivo en el origen, lo formatea segun las variables definidas anteriormente
  // Define la misma fecha en otro formato (format_max) para el mismo archivo de origen 
  println("original_file_date: " + original_file_date)
  val datetime_fecha_archivo_procesando= format_timestamp.parse(original_file_date) 
  val string_fecha_archivo_procesando = format_max.format(datetime_fecha_archivo_procesando) 

  // Hay un archivo de TAG que se llama "last_ingest_time" que le ayuda a determinar la fecha de los datos del archivo de la ultima vez que se proceso 
  // Si no existe el archivo, lo crea con la fecha del archivo a procesar y lo utilizará en la siguiente ejecución para comparar
  if(!(exists_file(dir_adls + "/last_ingest_time_mensual.txt"))){ 
      println("no existe archivo con fecha maxima  [CREANDO ARCHIVO]");
      println("crear archivo")
      makeTxtFile(dir_adls + "/last_ingest_time_mensual.txt", "timestamp;\n"+string_fecha_archivo_procesando+";")
  }else{ 
    // si existe el archivo, lo lee y compara las fechas
    // fecha del archivo almacenado de la ultima ejecución de proceso
    val fecha_ultima_ejecucion = readTxtFile(dir_adls + "/last_ingest_time_mensual.txt").split("\n")(1).dropRight(1) 

    // fecha del archivo a procesar que esta en la carpeta landing
    val datetime_fecha_ultima_ejecucion = format_max.parse(fecha_ultima_ejecucion) // fecha maxima en formato string
    var result: Int = 0;
    result = datetime_fecha_archivo_procesando.compareTo(datetime_fecha_ultima_ejecucion);
      
    // si el valor es mayor a 0, significa que el archivo de last_ingest_time_mensual 
    if(result >= 0) {
      println("fecha del archivo a procesar es mayor que la fecha de ultima ejecución [ACTUALIZAR FECHA DEL ARCHIVO]");
      delete(dir_adls + "/last_ingest_time_mensual.txt")
      makeTxtFile(dir_adls + "/last_ingest_time_mensual.txt", "timestamp;\n"+string_fecha_archivo_procesando+";")
    }else{
      println("fecha del archivo de proceso menor o igual que la fecha de la ultima ejecución [NO HACER NADA]");
    }
  } 
} catch {
    case e: Exception =>
      val status_ejecucion = 1
      val desc_status_ejecucion = "[ERROR] " + e
      println("[ERROR] " + e)
}
