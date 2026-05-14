// Databricks notebook source
// MAGIC %md
// MAGIC
// MAGIC ##Librerias

// COMMAND ----------

spark.conf.set("spark.sql.session.timeZone", "America/Santiago")

// COMMAND ----------

// DBTITLE 0,Librerías

import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions.expr
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.functions.split
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import io.delta.tables._
import org.apache.spark.sql.Row

import java.util.{Calendar, TimeZone, Date}
import org.apache.spark.sql.types.DateType
import java.time.format.DateTimeFormatter
import java.time.LocalDateTime
import java.text.SimpleDateFormat


// COMMAND ----------

// MAGIC %run /Workspace/Repos/ingestas/Cloud-Ingestas-ADB/framework_ingesta/funciones/funciones_genericas

// COMMAND ----------

var status_ejecucion = 0
var desc_status_ejecucion = "[OK]"
var status_error: Exception = null

// COMMAND ----------

// MAGIC %md
// MAGIC
// MAGIC ##Seteo de variables

// COMMAND ----------

// PARAMETROS
var dir_adls, delimeter, dataType, plataforma, loadType, periodicity, partition_date_column, auxiliar_partition_value, formato_partition_column, formato_partition_column_out, starttime_nifi, endtime_nifi, original_file_size, original_file_date, data_source_name, timestampFormat, quote, nomb_proc, merge_string, schema, pipelineRunId, catalog_control, escape = ""
var totaltime_nifi = "0"
val data_source_type = "table"
var nombre_catalogo, nombre_archivo, process_name = ""
// PATHS DIRECTORIOS PARAMETROS 
var path_landing, path_stage, path_log, path_diario, path_mensual, path_data, path_tmp, path_json, path_hql2 = ""

// VARIABLES
var string_fecha_archivo_procesando, fecha_ultima_ejecucion = ""
var datetime_fecha_ultima_ejecucion: java.util.Date = null
var datetime_fecha_archivo_procesando: java.util.Date = null
var starttime_spark = ""

var columns = Seq.empty[String]
var process_type = "normal"
var numPartitions, dif_row_count = 0
val final_number_of_files = "eliminar variable"
val final_name = "eliminar variable"
val end_file_name = "eliminar variable"
var bigdata_ctrl_id, insert_data_ctrl_date, partition_name, filename, bigdata_close_date, list = ""
var final_file_size, original_row_count, final_row_count = 0L
var end_time_date, current_ins, current_id = 0L
var counter = 1
var controlData = Seq.empty[(String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String)]

var format_actual: SimpleDateFormat = null

var create_table_query_hql: String = ""
var df_sal1: DataFrame  = null
var df: DataFrame = null

// COMMAND ----------

// TIEMPO INICIO EJECUCION
val spark = SparkSession.builder.getOrCreate()
val start_time_date = new Date().getTime
val formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
val formatter2 = new SimpleDateFormat("yyyy-MM-dd")
val format = new SimpleDateFormat("yyyy-MM-dd")
val formatter_ins = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
val formatter_id = new SimpleDateFormat("yyyyMMddHHmmss") 
var format_timestamp = new java.text.SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'")
var format_max = new java.text.SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'") 

formatter.setTimeZone(TimeZone.getTimeZone("Chile/Continental"))
formatter2.setTimeZone(TimeZone.getTimeZone("Chile/Continental"))
format.setTimeZone(TimeZone.getTimeZone("Chile/Continental"))
formatter_ins.setTimeZone(TimeZone.getTimeZone("Chile/Continental"))
formatter_id.setTimeZone(TimeZone.getTimeZone("Chile/Continental"))
format.setTimeZone(TimeZone.getTimeZone("Chile/Continental"))


val starttime_spark = formatter.format(start_time_date)
val insert_date = formatter2.format(start_time_date)


// SE GENERA EL TIMESTAMP
current_id = new Date().getTime

//SE CONCATENA EL TIMESTAMP MAS EL CONTADOR GENERANDO UN ID UNICO PARA LA TABLA DE CONTROL
bigdata_ctrl_id = formatter_id.format(current_id) + "%03d".format(counter)
//OBTIENE FECHA EJECUCION
val fecha = format.format(current_id)

// COMMAND ----------

if(status_ejecucion == 0){
    try {
        
      //DIRECTORIO HDFS DESDE DONDE SE EJECUTARA LA LECTURA Y CARGA DE ARCHIVOS.
      dir_adls = dbutils.widgets.get("dir_adls")
      //INDICA SI LA DATA ES DE TIPO RAW,NORAW,CONFORMADO.
      dataType = dbutils.widgets.get("dataType")
      //INDICA DESDE DONDE VIENE LA INFORMACION A CARGAR (ATIS,SAP,ETC)
      plataforma = dbutils.widgets.get("plataforma")
      //INDICA EL TIPO DE CARGA A EJECUTAR INCREMENTAL,FULL,etc (i,d)
      loadType = dbutils.widgets.get("loadType")
      //INDICA LA PERIODICIDAD DE LA INGESTA, diaria, semanal, mensual (d,s,m).
      periodicity = dbutils.widgets.get("periodicity")
      //COLUMNA POR LA CUAL LA DATA SERA PARTICIONADA EN HDFS, VACIO POR DEFECTO
      partition_date_column = Option(dbutils.widgets.get("partition_date_column")).filterNot(_.isEmpty).getOrElse("")
      //EN CASO QUE LA TABLA NO TENGA COLUMNA DE PARTICION, SE PERMITE INGRESAR UNA FECHA EXTERNA QUE REEMPLAZE LA COLUMNA (ej FECHA DE CARGA SACADA DESD UNA TABLA DE LOG)
      auxiliar_partition_value =  Option(dbutils.widgets.get("auxiliar_partition_value")).filterNot(_.isEmpty).getOrElse("")
      //FORMATO DE FECHA EN LA QUE LA COLUMNA DE PARTICION VIENE (Ej: yyyyMMdd,yyyy-MM-dd,ETC. VACIA POR DEFECTO)
      formato_partition_column = Option(dbutils.widgets.get("formato_partition_column")).filterNot(_.isEmpty).getOrElse("")
      //FORMATO DE FECHA EN LA QUE LA COLUMNA DE PARTICION QUEDARA EN EL ARCHIVO DE SALIDA (Ej: yyyyMMdd,yyyy-MM-dd,ETC. yyyy-MM-dd POR DEFECTO)
      formato_partition_column_out = Option(dbutils.widgets.get("formato_partition_column_out")).filterNot(_.isEmpty).getOrElse("yyyy-MM-dd")
      //INDICA EL TIMESTAMP DEL MOMENTO EN QUE NIFI INICIO EL FLUJO.
      starttime_nifi = dbutils.widgets.get("starttime_nifi")
      //INDICA EL TIMESTAMP DEL MOMENTO EN QUE NIFI TERMINO EL FLUJO.
      endtime_nifi = dbutils.widgets.get("endtime_nifi")
      //INDICA EL TOTAL DE TIEMPO EN SEGUNDOS QUE DEMORO LA EJECUCION DE NIFI.
      format_actual = new SimpleDateFormat("yyyyMMdd HHmmss")
      //CALCULO TOTAL NIFI
      totaltime_nifi = ((format_actual.parse(endtime_nifi).getTime() - format_actual.parse(starttime_nifi).getTime())/1000).toString()
      //N/A
      //original_file_size = dbutils.widgets.get("original_file_size")
      //N/A
      original_file_date = dbutils.widgets.get("original_file_date")
      //NOMBRE DE LA TABLA Y ESQUEMA DE LA BD CON FORMATO ESQUEMA.TABLA
      data_source_name = dbutils.widgets.get("data_source_name")
      //FORMATO EN QUE VIENEN LAS COLUMNAS TIPO TIMESTAMP DESDE LA BD. yyyy-MM-dd HH:mm:ss.SSSSSSS POR DEFECTO
      timestampFormat = Option(dbutils.widgets.get("timestampFormat")).filterNot(_.isEmpty).getOrElse("yyyy-MM-dd HH:mm:ss.SSSSSSS")
      //EN CASO DE TENER DATA ENCOMILLADA SE DEFINEN COMILLAS DOBLES, SIMPLES, ETC. COMILLAS DOBLES POR DEFECTO
      quote = Option(dbutils.widgets.get("quote")).filterNot(_.isEmpty).getOrElse("\"")
      //RECIBE NOMBRE DEL PROCESO POR PARAMETRO
      nomb_proc = dbutils.widgets.get("nomb_proc")
      //MERGE STRING
      merge_string = dbutils.widgets.get("merge_string")
      //PARÁMETRO SCHEMA
      schema = dbutils.widgets.get("schema")
      // PIPELINE RUN ID
      pipelineRunId = dbutils.widgets.get("pipelineRunId")
      // Catalogo, esquema y tabla de control
      catalog_control = dbutils.widgets.get("catalog_control")
      // delimitador para lectura csv 
      delimeter = dbutils.widgets.get("delimeter")
      // caracter de escape para csv
      escape = dbutils.widgets.get("escape")

      nombre_catalogo = nomb_proc.split("\\.")(0) //NOMBRE DEL CATALOGO DE UNITY CATALOG
      nombre_archivo = dir_adls.split("/").last //NOMBRE DE LOS ARCHIVOS HQL Y JSON
      process_name = "spark_" + nomb_proc

    } catch {
        case e: Exception =>
          status_ejecucion = 1
          desc_status_ejecucion = "[ERROR] " + e
          status_error = e
          println("[ERROR] " + e)
    } 
}

// COMMAND ----------

println(s"dir_adls: $dir_adls")
println(s"dataType: $dataType")
println(s"plataforma: $plataforma")
println(s"loadType: $loadType")
println(s"periodicity: $periodicity")
println(s"partition_date_column: $partition_date_column")
println(s"auxiliar_partition_value: $auxiliar_partition_value")
println(s"formato_partition_column: $formato_partition_column")
println(s"formato_partition_column_out: $formato_partition_column_out")
println(s"starttime_nifi: $starttime_nifi")
println(s"endtime_nifi: $endtime_nifi")
println(s"format_actual: $format_actual")
println(s"totaltime_nifi: $totaltime_nifi")
println(s"original_file_size: $original_file_size")
println(s"original_file_date: $original_file_date")
println(s"data_source_name: $data_source_name")
println(s"timestampFormat: $timestampFormat")
println(s"quote: $quote")
println(s"nomb_proc: $nomb_proc")
println(s"merge_string: $merge_string")
println(s"schema: $schema")
println(s"pipelineRunId: $pipelineRunId")
println(s"catalog_control: $catalog_control")

// COMMAND ----------

if (loadType != "full" && loadType != "incremental") {
  throw new IllegalArgumentException("loadType debe ser full o incremental")
}

// COMMAND ----------

// MAGIC %run ./funciones_genericas_csv

// COMMAND ----------

// MAGIC %md
// MAGIC ##Directorios de trabajo para el proceso

// COMMAND ----------

if(status_ejecucion == 0){
  try {
    // A la fecha original del archivo en el origen, lo formatea segun las variables definidas anteriormente
    // Define la misma fecha en otro formato (format_max) para el mismo archivo de origen 
    println("original_file_date: " + original_file_date)
    datetime_fecha_archivo_procesando= format_timestamp.parse(original_file_date) 
    string_fecha_archivo_procesando = format_max.format(datetime_fecha_archivo_procesando) 

    // Hay un archivo de TAG que se llama "last_ingest_time" que le ayuda a determinar la fecha de los datos del archivo de la ultima vez que se proceso 
    // Si no existe el archivo, lo crea con la fecha del archivo a procesar y lo utilizará en la siguiente ejecución para comparar
    if(!(exists_file(dir_adls + "/last_ingest_time.txt"))){ 
        println("no existe archivo con fecha maxima  [CREANDO ARCHIVO]");
        println("crear archivo")
        makeTxtFile(dir_adls + "/last_ingest_time.txt", "timestamp;\n"+string_fecha_archivo_procesando+";")
    }else{ 
      // si existe el archivo, lo lee y compara las fechas
      // fecha del archivo almacenado de la ultima ejecución de proceso
      fecha_ultima_ejecucion = readTxtFile(dir_adls + "/last_ingest_time.txt").split("\n")(1).dropRight(1) 

      // fecha del archivo a procesar que esta en la carpeta landing
      datetime_fecha_ultima_ejecucion = format_max.parse(fecha_ultima_ejecucion) // fecha maxima en formato string
      var result: Int = 0;
      result = datetime_fecha_archivo_procesando.compareTo(datetime_fecha_ultima_ejecucion);
      
      // si el valor es mayor a 0, significa que el archivo de last_ingest_time 
      if(result >= 0) {
        println("fecha del archivo a procesar es mayor que la fecha de ultima ejecución [ACTUALIZAR FECHA DEL ARCHIVO]");
        delete(dir_adls + "/last_ingest_time.txt")
        makeTxtFile(dir_adls + "/last_ingest_time.txt", "timestamp;\n"+string_fecha_archivo_procesando+";")

      }else{
        println("fecha del archivo de proceso menor o igual que la fecha de la ultima ejecución [NO HACER NADA]");
      }
    } 
  } catch {
      case e: Exception =>
        status_ejecucion = 1
        desc_status_ejecucion = "[ERROR] " + e
        println("[ERROR] " + e)
    }
} else {
  println("Skipped - Ejecución = " + status_ejecucion)
}

// COMMAND ----------

if(status_ejecucion == 0){
    try {
      path_landing   = dir_adls + "/landing/"
      path_stage     = dir_adls + "/stage/"
      path_log       = dir_adls + "/log/"
      path_diario    = dir_adls + "/diario/"
      path_mensual   = dir_adls + "/mensual/"
      path_data      = dir_adls + "/"+dataType+"/"
      path_tmp       = dir_adls + "/tmp"
      path_json = dir_adls + "/" + nombre_archivo + ".json"
      path_hql2 = dir_adls + "/" + nombre_archivo + ".hql"
    } catch {
        case e: Exception =>
          status_ejecucion = 1
          desc_status_ejecucion = "[ERROR] " + e
          status_error = e
          println("[ERROR] " + e)
    } 
} else {
  println("Skipped")
}

// COMMAND ----------

println(s"path_landing: $path_landing")   
println(s"path_stage: $path_stage")     
println(s"path_log: $path_log")       
println(s"path_diario: $path_diario")    
println(s"path_mensual: $path_mensual")   
println(s"path_data: $path_data")      
println(s"path_tmp: $path_tmp")       
println(s"path_json: $path_json") 
println(s"path_hql2: $path_hql2") 

// COMMAND ----------

if(status_ejecucion == 0){
    try {
      original_file_size = get_folder_size(path_stage)
    } catch {
        case e: Exception =>
          status_ejecucion = 1
          desc_status_ejecucion = "[ERROR] " + e
          status_error = e
          println("[ERROR] " + e)
    } 
} else {
  println("Skipped")
}

// COMMAND ----------

// MAGIC %md
// MAGIC # Lee archivo parquet y lo pasa a landing en formato delta

// COMMAND ----------

if(status_ejecucion == 0){
    try {
      //Se define el schema para el dataframe
      println("path_json : " + path_json)
      println("path_stage : " + path_stage)
      println("delimeter : " + delimeter)
      println("quote : " + quote)
      println("timestampFormat : " + timestampFormat)   
      println("escape : " + escape)                           
      df = defineSchemaJson(path_json, path_stage, delimeter, quote, timestampFormat, escape) 
    } catch {
        case e: Exception =>
          status_ejecucion = 1
          desc_status_ejecucion = "[ERROR] " + e
          status_error = e
          println("[ERROR] " + e)
    } 
}  else {
  println("Skipped")
}

display(df)

// COMMAND ----------

// MAGIC %md
// MAGIC ###Crea tabla externa Delta

// COMMAND ----------

create_table_query_hql =  openFile(dir_adls + "/" + nombre_archivo + ".hql")

// REPLACE LOCATION MANAGED
create_table_query_hql = create_table_query_hql.replace("LOCATIONMANAGEDCONTAINERHQL", dir_adls + "/" + dataType)
create_table_query_hql = create_table_query_hql.replace("NOMBREPROCUNITYCATALOGDATABRICKS", nomb_proc)

println(create_table_query_hql)

if(status_ejecucion == 0){
  if(!spark.catalog.tableExists(nomb_proc)){
     try {
      println("[INFO] create_table_query_hql " + create_table_query_hql)

      //CREA TABLA DEL CONTENIDO DEL ARCHIVO HQL
      df_sal1 = spark.sql(create_table_query_hql)

    } catch {
        case e: Exception =>
          status_ejecucion = 1
          desc_status_ejecucion = "[ERROR] " + e
          status_error = e
          println("[ERROR] " + e)
    } 
  }else{
    println("Tabla ya existe, no se crea.")
  }
}  else {
  println("Skipped")
}

// COMMAND ----------

// MAGIC %md
// MAGIC ###Landing

// COMMAND ----------

if(status_ejecucion == 0){
    try {
      // SE AGREGA LA COLUMNA BIGDATA_CLOSE_DATE Y BIGDATA_CTRL_ID AL DATAFRAME
      df = df.withColumn("bigdata_close_date",lit(null)).withColumn("bigdata_ctrl_id", lit(bigdata_ctrl_id) cast "long")

      //se eliminan valores nulos y duplicados
      println("SE ELIMINAN DUPLICADOS y NULOS")
      df = df.dropDuplicates
      df = df.na.drop("all")

      //SE CUENTA LA CANTIDAD DE REGISTROS
      println("Contando cantidad de registros")
      original_row_count = df.count


      // SI TIENE PARTICIONES
      if (create_table_query_hql.toLowerCase().contains("partitioned by")) {

        // CARGA INCREMENTAL
        if (loadType == "incremental"){
          println("Si es incremental, Hacer Merge")
          mergeData(path_landing, merge_string, df, true, columns)

        // CARGA FULL
        }else{
          println("Si es full, hacer overwrite")
          df
          .write
          .format("delta")
          .mode("overwrite")
          .partitionBy(columns: _*)
          .option("partitionOverwriteMode", "dynamic")
          // .option("x  ", "true")
          .save(path_landing)
        }

      // SI NO TIENE PARTICIONES
      }else{

        // CARGA INCREMENTAL
        if (loadType == "incremental"){
          println("No contiene la columna partitioned by...")
          println("Si es incremental, Hacer Merge")
          mergeData(path_landing, merge_string, df, false, columns)

        // CARGA FULL
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
      numPartitions = numPartitionsCalc(path_landing)


    } catch {
        case e: Exception =>
          status_ejecucion = 1
          desc_status_ejecucion = "[ERROR] " + e
          status_error = e
          status_error = e
          println("[ERROR] " + e)
    } 
} else {
  println("Skipped")
}


// COMMAND ----------

// MAGIC %md
// MAGIC ###Raw, NoRaw, Conformado

// COMMAND ----------


if(status_ejecucion == 0){
    try {
              
      // EVALUA SI LA COLUMNA DE PARTICION ESTA VACIA, GENERANDO UNA COLUMNA DE PARTICION NUEVA CON LA FECHA DEL DIA Y AGREGANDOLA AL DATAFRAME. 
      // EN CASO QUE LA COLUMNA DE PARTICION Y LA COLUMNA AUXILIAR ESTAN VACIAS, SE OBTIENE EL TIMESTAMP Y SE FORMATEA A YYYY-MM-DD UTILIZANDO ESTA FECHA COMO COLUMNA DE PARTICION   
      if (partition_date_column == "" && auxiliar_partition_value != "") {
          df = df.withColumn("bigdata_close_date", lit(auxiliar_partition_value) cast "date")
          partition_date_column = "bigdata_close_date"
          formato_partition_column = "yyyy-MM-dd"
      } else if (partition_date_column == "" && auxiliar_partition_value == "") {
          val format = new SimpleDateFormat("yyyy-MM-dd")
          bigdata_close_date = format.format(Calendar.getInstance().getTime())
          df = df.withColumn("bigdata_close_date", lit(bigdata_close_date) cast "date")
          partition_date_column = "bigdata_close_date"
          formato_partition_column = "yyyy-MM-dd"
        }


      if (create_table_query_hql.toLowerCase().contains("partitioned by")) {
          //SE CREA UNA COLUMNA DE PARTICION TEMPORAL, TOMANDO LA COLUMNA DE PARTICION Y CAMBIANDO EL FORMATO DE FECHA DE ENTRADA AL DE SALIDA

          df = df.withColumn("bigdata_close_date", from_unixtime(unix_timestamp(col(partition_date_column), formato_partition_column), formato_partition_column_out) cast "date")
            
          //SE OBTIENE EL NOMBRE DE LA COLUMNA O COLUMNAS DE PARTICION DESDE EL HQL. ej: (partition_date,year,month,day)
          val find_text_inicial = create_table_query_hql.toLowerCase().indexOf("partitioned by") + 15
          val find_text_final = find_text_inicial + create_table_query_hql.substring(find_text_inicial).indexOf(")")
          val partition_name = create_table_query_hql.substring(find_text_inicial, find_text_final).replace("(", "").replace("string", "").trim()

          //SE SEPARA LA COLUMNA DE PARTICION POR "," EN CASO QUE SEA MAS DE UNA COLUMNA
          var splits = partition_name.split(",").size

          // SE RECORRE LA CANTIDAD DE COLUMNAS DE PARTICION,
          // SE MODIFICA EL SELECT AGREGANDO EL NOMBRE DE LA COLUMNA Y EL VALOR DE LA FECHA,
          // SE GENERA LA RUTA HDFS CON LOS NOMBRES DE LAS COLUMNAS DE PARTICION Y LOS VALORES DE LA FECHA
          // SE AGREGA EL NOMBRE DE LA COLUMNA A UNA LISTA
          println(splits)
          display(df)

          println("[INFO] prueba 3")
          for (x <- 0 to splits - 1 by 1) {
            columns = columns :+ partition_name.split(",")(x).trim()
          }

          println(columns)
          //SE GENERA UN SELECT DE LAS COLUMNAS DEL DATAFRAME RECORRIENDO SUS COLUMNAS Y SUMANDO LAS COLUMNAS DE PARTICION MAS SUS VALORES
          val selectExprs = df.columns.map(col) ++ (0 until columns.size  map (i => $"tmp".getItem(i).as(columns(i))))
          
          //SE APLICA EL SELECT CREADO AL DATAFRAME
          df_sal1 = df.withColumn("tmp", split($"bigdata_close_date", "-")).select(selectExprs:_*)
          display(df_sal1)
      }


    } catch {
        case e: Exception =>
          status_ejecucion = 1
          desc_status_ejecucion = "[ERROR] " + e
          status_error = e
          println("[ERROR] " + e)
    } 
} else {
  println("Skipped")
}

// COMMAND ----------

if(status_ejecucion == 0){
    try {
        if (create_table_query_hql.toLowerCase().contains("partitioned by")) {

          //Si la carga es incremental...
          if (loadType == "incremental"){
            println("Si es incremental, Hacer Merge")
            mergeData(path_data, merge_string, df_sal1, true, columns)


          }else if (loadType == "full"){
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
          else{
            println("LoadType no valido")
          }

        }else{
          
          //Si la carga es incremental...
          if (loadType == "incremental"){
            println("Si es incremental, Hacer Merge")
            mergeData(path_data, merge_string, df, false, columns)

          }else if (loadType == "full"){
            println("Si es full, hacer overwrite")
            df
            .write
            .format("delta")
            .mode("overwrite")
            .option("mergeSchema", "true")
            .save(path_data)
          }
          else{
            println("LoadType no valido")
          }

        }

        // SE OBTIENE EL TAMAÑO FINAL DE LOS ARCHIVOS ESCRITOS
        final_file_size = sizeFile(path_data)

        // SE CUENTA LA CANTIDAD DE REGISTROS DEL ARCHIVO FINAL
        final_row_count = spark.read.format("delta").load(path_data).count 

        // SE VALIDA SI LA CANTIDAD DE FILAS ORIGINALES VS LA CANTIDAD DE FILAS ESCRITAS ES DISTINTA
        if (original_row_count != final_row_count) dif_row_count = 1 else dif_row_count = 0

        //optimiza archivos delta
        val deltaTable = DeltaTable.forPath(spark, path_data)
        deltaTable.optimize().executeCompaction()

    } catch {
        case e: Exception =>
          status_ejecucion = 1
          desc_status_ejecucion = "[ERROR] " + e
          status_error = e
          println("[ERROR] " + e)
    } 
} else {
  println("Skipped")
}

// COMMAND ----------

// MAGIC %md
// MAGIC #Limpieza Carpetas

// COMMAND ----------

if(status_ejecucion == 0){
    try {
        //se eliminan archivos procesados y que hayan quedado en las fases de landing
        //limpia datos en landing y stage
        delete_files(path_landing)
        delete_files(path_stage)

    } catch {
        case e: Exception =>
          status_ejecucion = 1
          desc_status_ejecucion = "[ERROR] " + e
          status_error = e
          println("[ERROR] " + e)
    } 
} else {
  println("Skipped")
}

// COMMAND ----------

// MAGIC %md
// MAGIC #Dataframe Control Ingesta

// COMMAND ----------

// SE CALCULA EL TIMESTAMP
end_time_date = new Date().getTime
insert_data_ctrl_date = formatter2.format(end_time_date)
val endtime_spark = formatter.format(end_time_date)
val totaltime_spark = (end_time_date - start_time_date).toFloat / 1000
val totaltime_process = totaltime_spark + totaltime_nifi.toInt

// COMMAND ----------

println("bigdata_ctrl_id:  "+bigdata_ctrl_id)
println("process_name:  "+process_name)
println("data_source_type:  "+data_source_type)
println("data_source_name:  "+data_source_name)
println("original_file_date:  "+original_file_date)
println("starttime_nifi:  "+starttime_nifi)
println("endtime_nifi:  "+endtime_nifi)
println("totaltime_nifi:  "+totaltime_nifi)
println("starttime_spark:  "+starttime_spark)
println("process_type:  "+process_type)
println("original_file_size:  "+original_file_size)
println("final_file_size:  "+final_file_size.toString)
println("original_row_count:  "+original_row_count.toString)
println("final_row_count:  "+final_row_count.toString)
println("dif_row_count:  "+dif_row_count.toString)
println("final_number_of_files:  "+final_number_of_files)
println("end_file_name:  "+end_file_name)
println("insert_data_ctrl_date:  "+insert_data_ctrl_date)
println("path_data:  "+path_data)
println("pipelineRunId:  "+pipelineRunId)

// COMMAND ----------

// SE CALCULA EL TIMESTAMP
/* current_ins = new Date().getTime
insert_data_ctrl_date = formatter2.format(current_ins)
val endtime_spark = formatter.format(current_ins)
val totaltime_spark = (current_ins - current).toFloat / 1000
val totaltime_process = totaltime_spark + totaltime_nifi.toInt */
var status_ejecucion_str = ""
if(status_ejecucion == 0){
  status_ejecucion_str = "OK"
}else{
  status_ejecucion_str = "ERROR"
}

controlData = controlData :+ (status_ejecucion_str, desc_status_ejecucion, bigdata_ctrl_id, process_name, data_source_type, data_source_name, original_file_date, starttime_nifi, endtime_nifi, totaltime_nifi, starttime_spark, process_type, original_file_size, final_file_size.toString, original_row_count.toString, final_row_count.toString, dif_row_count.toString, final_number_of_files, end_file_name, insert_data_ctrl_date, path_data, pipelineRunId)


//SE CREA UN DATAFRAME CON TODOS LOS REGISTROS PARA LA TABLA DE CONTROL
var control_dataframe = controlData.toDF("status", "desc_status", "big_data_ctrl_id", "process_name", "data_source_type", "data_source_name", "original_file_date", "starttime_nifi", "endtime_nifi", "totaltime_nifi", "starttime_spark", "process_type", "original_file_size", "final_file_size", "original_row_count", "final_row_count", "dif_row_count", "final_number_of_files", "end_file_name", "insert_data_ctrl_date", "hdfs_path", "pipelineRunId")


//SE AGREGAN LAS COLUMNAS CON LA DURACION Y SE FORMATEAN LOS TIMESTAMP AL DATAFRAME DE CONTROL
control_dataframe = control_dataframe.
withColumn("endtime_spark", lit(endtime_spark)).
withColumn("totaltime_spark", lit(totaltime_spark)).
withColumn("totaltime_process", lit(totaltime_process)).
withColumn("original_file_date", from_unixtime(unix_timestamp($"original_file_date", "yyyyMMdd HHmmss"), "yyyy-MM-dd HH:mm:ss")).
withColumn("starttime_nifi", from_unixtime(unix_timestamp($"starttime_nifi", "yyyyMMdd HHmmss"), "yyyy-MM-dd HH:mm:ss")).
withColumn("endtime_nifi", from_unixtime(unix_timestamp($"endtime_nifi", "yyyyMMdd HHmmss"), "yyyy-MM-dd HH:mm:ss")).
select("status", "desc_status", "big_data_ctrl_id", "process_name", "data_source_type", "data_source_name", "original_file_date", "starttime_nifi", "endtime_nifi", "totaltime_nifi", "starttime_spark", "endtime_spark", "totaltime_spark", "totaltime_process", "insert_data_ctrl_date", "process_type", "original_file_size", "final_file_size", "original_row_count", "final_row_count", "dif_row_count", "final_number_of_files", "end_file_name","hdfs_path", "pipelineRunId")


// COMMAND ----------

// SE ESCRIBEN LOS REGISTROS DE CONTROL EN CATALOGO
control_dataframe.write.mode(SaveMode.Append).option("mergeSchema", "true").saveAsTable(catalog_control) // 

// COMMAND ----------

if(status_ejecucion == 0){
  dbutils.notebook.exit("OK")
}else{
  throw status_error
}
