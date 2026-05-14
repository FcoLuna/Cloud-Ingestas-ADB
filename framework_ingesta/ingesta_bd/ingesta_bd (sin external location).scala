// Databricks notebook source
// MAGIC %md
// MAGIC
// MAGIC ##Librerias

// COMMAND ----------

// DBTITLE 0,Librerías
import java.util.Calendar
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.DataType
import java.text.SimpleDateFormat
import java.util.Date
import org.apache.spark.sql.functions.split
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.DateType
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import io.delta.tables._
import org.apache.spark.sql.Row
import java.time.format.DateTimeFormatter
import java.time.LocalDateTime

// COMMAND ----------

// MAGIC %md
// MAGIC
// MAGIC ##Seteo de variables

// COMMAND ----------

//DIRECTORIO HDFS DESDE DONDE SE EJECUTARA LA LECTURA Y CARGA DE ARCHIVOS.
val dir_adls = dbutils.widgets.get("dir_adls")
//INDICA SI LA DATA ES DE TIPO RAW,NORAW,CONFORMADO.
val dataType = dbutils.widgets.get("dataType")
//INDICA DESDE DONDE VIENE LA INFORMACION A CARGAR (ATIS,SAP,ETC)
val plataforma = dbutils.widgets.get("plataforma")
//INDICA A QUE SUBDIRECTORIO CORRESPONDIENTE A LA TABLA A CARGAR
/* val subdirectorio = dbutils.widgets.get("subdirectorio") */
//INDICA EL TIPO DE CARGA A EJECUTAR INCREMENTAL,FULL,etc (i,d)
val loadType = dbutils.widgets.get("loadType")
//INDICA LA PERIODICIDAD DE LA INGESTA, diaria, semanal, mensual (d,s,m).
val periodicity = dbutils.widgets.get("periodicity")
//COLUMNA POR LA CUAL LA DATA SERA PARTICIONADA EN HDFS, VACIO POR DEFECTO
val partition_date_column = Option(dbutils.widgets.get("partition_date_column")).filterNot(_.isEmpty).getOrElse("")
//INDICA EL TIMESTAMP DEL MOMENTO EN QUE NIFI INICIO EL FLUJO.
val starttime_nifi = dbutils.widgets.get("starttime_nifi")
//INDICA EL TIMESTAMP DEL MOMENTO EN QUE NIFI TERMINO EL FLUJO.
val endtime_nifi = dbutils.widgets.get("endtime_nifi")
//INDICA EL TOTAL DE TIEMPO EN SEGUNDOS QUE DEMORO LA EJECUCION DE NIFI.
val format_actual = new SimpleDateFormat("yyyyMMdd HHmmss")
//CALCULO TOTAL NIFI
val totaltime_nifi = ((format_actual.parse(endtime_nifi).getTime() - format_actual.parse(starttime_nifi).getTime())/1000).toString()
//N/A
val original_file_size = dbutils.widgets.get("original_file_size")
//N/A
val original_file_date = dbutils.widgets.get("original_file_date")
//NOMBRE DE LA TABLA Y ESQUEMA DE LA BD CON FORMATO ESQUEMA.TABLA
val data_source_name = dbutils.widgets.get("data_source_name")
//FORMATO EN QUE VIENEN LAS COLUMNAS TIPO TIMESTAMP DESDE LA BD. yyyy-MM-dd'T'HH:mm:ss[.SSS][XXX] POR DEFECTO
val timestampFormat = Option(dbutils.widgets.get("timestampFormat")).filterNot(_.isEmpty).getOrElse("yyyy-MM-dd HH:mm:ss[.SSS][XXX]")
//EN CASO DE TENER DATA ENCOMILLADA SE DEFINEN COMILLAS DOBLES, SIMPLES, ETC. COMILLAS DOBLES POR DEFECTO
val quote = Option(dbutils.widgets.get("quote")).filterNot(_.isEmpty).getOrElse("\"")
//RECIBE NOMBRE DEL PROCESO POR PARAMETRO
val nomb_proc = dbutils.widgets.get("nomb_proc")
//MERGE STRING
val merge_string = dbutils.widgets.get("merge_string")
//PARÁMETRO SCHEMA
val schema = dbutils.widgets.get("schema")
// PIPELINE RUN ID
val pipelineRunId = dbutils.widgets.get("pipelineRunId")
// Catalogo, esquema y tabla de control
val catalog_control = dbutils.widgets.get("catalog_control")



var nombre_catalogo = nomb_proc.split("\\.")(0) //NOMBRE DEL CATALOGO DE UNITY CATALOG
val nombre_archivo = dir_adls.split("/").last //NOMBRE DE LOS ARCHIVOS HQL Y JSON
val spark = SparkSession.builder.getOrCreate()
val current = new Date().getTime
val formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
val formatter2 = new SimpleDateFormat("yyyy-MM-dd")
val starttime_spark = formatter.format(current)
val insert_date = formatter2.format(current)
val process_name = "spark_" + nomb_proc
val data_source_type = "table"
val end = new Date().getTime
val endtime_spark = formatter.format(end)
val totaltime_spark = (end - current).toFloat / 1000
val totaltime_process = totaltime_spark + totaltime_nifi.toInt

// COMMAND ----------

  var columns = Seq.empty[String]
  var process_type = "normal"
  var numPartitions = 0
  var final_file_size = 0L
  var original_row_count = 0L
  var final_row_count = 0L
  var dif_row_count = 0
  var final_number_of_files = 0L
  var final_name = ""
  var end_file_name = ""
  var bigdata_ctrl_id = ""
  var insert_data_ctrl_date = ""
  var current_ins = 0L
  var formatter_ins: SimpleDateFormat = null
  var current_id = 0L
  var formatter_id: SimpleDateFormat = null
  var partition_name = ""
  var counter = 1
  var filename = ""
  var bigdata_close_date = ""
  var partition_value = ""
  var list = ""
  var controlData = Seq.empty[(String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String)]

// COMMAND ----------

// MAGIC %run /Shared/framework_ingesta/funciones/funciones_genericas

// COMMAND ----------

// MAGIC %md
// MAGIC ##Directorios de trabajo para el proceso

// COMMAND ----------

//Cambio
val path_landing   = dir_adls + "/landing/"
val path_stage     = dir_adls + "/stage/"
val path_log       = dir_adls + "/log/"
val path_diario    = dir_adls + "/diario/"
val path_mensual   = dir_adls + "/mensual/"
val path_data      = dir_adls + "/"+dataType+"/"
val path_tmp       = dir_adls + "/tmp"
var path_json = dir_adls + "/" + nombre_archivo + ".json"
var path_hql2 = dir_adls + "/" + nombre_archivo + ".hql"

// COMMAND ----------

// MAGIC %md
// MAGIC # Lee archivo parquet y lo pasa a landing en formato delta

// COMMAND ----------

//Se define el schema para el dataframe
var df = defineSchemaJson(path_json, path_stage)

// COMMAND ----------

// MAGIC %md
// MAGIC ###Crea tabla externa Delta

// COMMAND ----------

var subida_datos =  openFile(dir_adls + "/" + nombre_archivo + ".hql")

// //SE OBTIENE EL NOMBRE DE LA COLUMNA O COLUMNAS DE PARTICION DESDE EL HQL. ej: (partition_date,year,month,day)
// partition_name = subida_datos.substring(subida_datos.toLowerCase().indexOf("partitioned by") + 15, subida_datos.toLowerCase().indexOf("partitioned by") + 15 + subida_datos.substring(subida_datos.toLowerCase().indexOf("partitioned by") + 15).indexOf(")")).replace("(", "").replace("string", "").trim()

// REPLACE LOCATION MANAGED
subida_datos = subida_datos.replace("LOCATIONMANAGEDCONTAINERHQL", dir_adls + "/" + dataType)
subida_datos = subida_datos.replace("NOMBREPROCUNITYCATALOGDATABRICKS", nomb_proc)

println("[INFO] subida_datos " + subida_datos)

//CREA TABLA DEL CONTENIDO DEL ARCHIVO HQL
// spark.sql(subida_datos)

var dataframe1, df1, df_sal1, df_sal2 = spark.sql(subida_datos)

// COMMAND ----------

// MAGIC %md
// MAGIC ###Landing

// COMMAND ----------

// SE GENERA EL TIMESTAMP
current_id = new Date().getTime

//SE CREA UN FORMATO DE TIMESTAMP
formatter_id = new SimpleDateFormat("yyyyMMddHHmmss")

//SE CONCATENA EL TIMESTAMP MAS EL CONTADOR GENERANDO UN ID UNICO PARA LA TABLA DE CONTROL
bigdata_ctrl_id = formatter_id.format(current_id) + "%03d".format(counter)

val format = new SimpleDateFormat("yyyy-MM-dd")
bigdata_close_date = format.format(Calendar.getInstance().getTime())

// SE AGREGA LA COLUMNA BIGDATA_CLOSE_DATE Y BIGDATA_CTRL_ID AL DATAFRAME
df = df.withColumn("bigdata_close_date",lit(bigdata_close_date) cast "date").withColumn("bigdata_ctrl_id", lit(bigdata_ctrl_id) cast "long")

//SE CUENTA LA CANTIDAD DE REGISTROS

val original_row_count = df.count

if (subida_datos.toLowerCase().contains("partitioned by")) {

  //Si la carga es incremental...
  if (loadType == "incremental"){
    println("Si es incremental, Hacer Merge")
    mergeData(path_landing, merge_string, df, true, columns)

  }else{
    println("Si es full, hacer overwrite")
    df
    .write
    .format("delta")
    .mode("overwrite")
    .partitionBy(columns: _*)
    .option("partitionOverwriteMode", "dynamic")
    .option("mergeSchema", "true")
    .save(path_landing)
  }

}else{

    //Si la carga es incremental...
  if (loadType == "incremental"){
    println("No contiene la columna partitioned by...")
    println("Si es incremental, Hacer Merge")
    mergeData(path_landing, merge_string, df, false, columns)

  }else{
    println("No contiene la columna partitioned by...")
    println("Si es full, hacer overwrite")
    df
    .write
    .format("delta")
    .mode("overwrite")
    .option("mergeSchema", "true")
    .save(path_landing)
  }
}

//se crea archivo Delta en Landing
// SE CALCULA LA CANTIDAD DE ARCHIVOS A ESCRIBIR EN HD
var numPartitions = numPartitionsCalc(path_landing)

// display(df)

// COMMAND ----------

// MAGIC %md
// MAGIC ###Raw, NoRaw, Conformado

// COMMAND ----------

//se eliminan valores nulos y duplicados
println("SE ELIMINAN DUPLICADOS y NULOS")
df = df.dropDuplicates
df = df.na.drop("all")

val (columns, select_line) = listPartitions(subida_datos)

// SE REGISTRA EL DATAFRAME COMO TABLA TEMPORAL
df.createOrReplaceTempView("ing_schema")

// SE APLICA LA QUERY CREADA Y SE GUARDA EL RESULTADO EN UN DATAFRAME
val df_sal1 = spark.sql(select_line)

if (subida_datos.toLowerCase().contains("partitioned by")) {

  //Si la carga es incremental...
  if (loadType == "incremental"){
    println("Si es incremental, Hacer Merge")
    mergeData(path_data, merge_string, df_sal1, true, columns)


  }else{
    println("Si es full, hacer overwrite")
    df_sal1
    .write
    .format("delta")
    .mode("overwrite")
    .partitionBy(columns: _*)
    .option("partitionOverwriteMode", "dynamic")
    .option("mergeSchema", "true")
    .save(path_data)
  }

}else{
  
  //Si la carga es incremental...
  if (loadType == "incremental"){
    println("Si es incremental, Hacer Merge")
    mergeData(path_data, merge_string, df_sal1, false, columns)

  }else{
    println("Si es full, hacer overwrite")
    df_sal1
    .write
    .format("delta")
    .mode("overwrite")
    .option("mergeSchema", "true")
    .save(path_data)
  }

}
 
//optimiza archivos delta
val deltaTable = DeltaTable.forPath(spark, path_data)
deltaTable.optimize().executeCompaction()

// COMMAND ----------

// MAGIC %md
// MAGIC #Dataframe Control Ingesta

// COMMAND ----------

controlData = controlData :+ (bigdata_ctrl_id, process_name, data_source_type, data_source_name, original_file_date, starttime_nifi, endtime_nifi, totaltime_nifi, starttime_spark, process_type, original_file_size, final_file_size.toString, original_row_count.toString, final_row_count.toString, dif_row_count.toString, final_number_of_files.toString, end_file_name, insert_data_ctrl_date, path_data, pipelineRunId)

//SE CREA UN DATAFRAME CON TODOS LOS REGISTROS PARA LA TABLA DE CONTROL
var control_dataframe = controlData.toDF("big_data_ctrl_id", "process_name", "data_source_type", "data_source_name", "original_file_date", "starttime_nifi", "endtime_nifi", "totaltime_nifi", "starttime_spark", "process_type", "original_file_size", "final_file_size", "original_row_count", "final_row_count", "dif_row_count", "final_number_of_files", "end_file_name", "insert_data_ctrl_date", "hdfs_path", "pipelineRunId")

// SE OBTIENE EL TIMESTAMP
val end = new Date().getTime

//SE FORMATEA EL TIMESTAMP 
val endtime_spark = formatter.format(end)

//SE CALCULA LA DURACION DEL PROCESO SPARK
val totaltime_spark = (end - current).toFloat / 1000

//SE CALCULA LA DURACION DEL PROCESO SPARK + NIFI
val totaltime_process = totaltime_spark + totaltime_nifi.toInt

//SE AGREGAN LAS COLUMNAS CON LA DURACION Y SE FORMATEAN LOS TIMESTAMP AL DATAFRAME DE CONTROL.
control_dataframe = control_dataframe.
withColumn("endtime_spark", lit(endtime_spark)).
withColumn("totaltime_spark", lit(totaltime_spark)).
withColumn("totaltime_process", lit(totaltime_process)).
withColumn("original_file_date", from_unixtime(unix_timestamp($"original_file_date", "yyyyMMdd HHmmss"), "yyyy-MM-dd HH:mm:ss")).
withColumn("starttime_nifi", from_unixtime(unix_timestamp($"starttime_nifi", "yyyyMMdd HHmmss"), "yyyy-MM-dd HH:mm:ss")).
withColumn("endtime_nifi", from_unixtime(unix_timestamp($"endtime_nifi", "yyyyMMdd HHmmss"), "yyyy-MM-dd HH:mm:ss")).
select("big_data_ctrl_id", "process_name", "data_source_type", "data_source_name", "original_file_date", "starttime_nifi", "endtime_nifi", "totaltime_nifi", "starttime_spark", "endtime_spark", "totaltime_spark", "totaltime_process", "insert_data_ctrl_date", "process_type", "original_file_size", "final_file_size", "original_row_count", "final_row_count", "dif_row_count", "final_number_of_files", "end_file_name","hdfs_path", "pipelineRunId")


// SE ESCRIBEN LOS REGISTROS DE CONTROL EN CATALOGO
control_dataframe.write.mode(SaveMode.Append).saveAsTable(catalog_control)
// display(control_dataframe)

// COMMAND ----------

// MAGIC %md
// MAGIC #Limpieza Carpetas

// COMMAND ----------

//se eliminan archivos procesados y que hayan quedado en las fases de landing
//limpia datos en landing y stage
delete_files(path_landing)
delete_files(path_stage)
