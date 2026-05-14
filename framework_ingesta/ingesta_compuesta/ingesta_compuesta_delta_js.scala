// Databricks notebook source
// MAGIC %md
// MAGIC
// MAGIC ##Librerias

// COMMAND ----------

spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")

// COMMAND ----------

// Importa librerías para ejecución de metodos

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

// MAGIC %md
// MAGIC
// MAGIC ##Llama Notebook de funciones genericas

// COMMAND ----------

// MAGIC %run /Workspace/Repos/ingestas/Cloud-Ingestas-ADB/framework_ingesta/funciones/funciones_genericas

// COMMAND ----------

// MAGIC %md
// MAGIC
// MAGIC ##Instancia Variables

// COMMAND ----------

// cmd 8
var delimitador, encabezado, dir_adls, pipelineRunId, catalog_control, formato_entrada, dataType, plataforma, categoria, loadType, periodicity, endtime_nifi, original_file_size, formato_salida, dateFormatFile, timestampFormat, dir_adls_2, filename, partition_date_column, quote = ""

//add by JS
var split_index_datefile = -1



// cmd 9
var string_fecha_archivo_procesando, fecha_ultima_ejecucion = ""
var datetime_fecha_ultima_ejecucion: java.util.Date = null
var original_file_date = ""
var datetime_fecha_archivo_procesando: java.util.Date = null
var starttime_spark = ""

// cmd 10
var path_processing, path_landing, path_processing_error, path_log, path_data, path_tmp, path_tmp_transform = ""

// Define los 3 tipos de formateo de fecha 1) 20240423 11:33:23  2) 2024-04-23T08:30:45.123Z 3) 2024-04-23T08:30:45Z
var format_actual = new java.text.SimpleDateFormat("yyyyMMdd HHmmss")
var format_max = new java.text.SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'") 
var format_timestamp = new java.text.SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'") 

val current = new Date().getTime
val formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
val formatter2 = new SimpleDateFormat("yyyy-MM-dd")

var starttime_nifi_dateFormat: Date = null
var endtime_nifi_dateFormat: Date = null
var starttime_nifi = ""
var totaltime_nifi = "" 

// cmd 12
var nombre_archivo, subida_datos, query, query2 = ""
var data_source_type = "file"

//SE DEFINE UN ESQUEMA NULL PARA LAS TRANSFORMACIONES
var schema_data_trns = null.asInstanceOf[StructType]
var schema_data = null.asInstanceOf[StructType]

// cmd 13 
var salida, bigdata_close_date, partition_value, process_name, nomb_proc, nomb_schema = ""
  var df1, df2, df3, df_join, df4, df5, df_sal2, df_sal3, df_sal4 = null.asInstanceOf[DataFrame]
  var controlData = Seq.empty[(String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String)]

// estado ejecucion - cuando cambie a 0 se dirigira a la tabla de control a cerrar el proceso
var status = 0
var status_ejecucion = 0
var desc_status_ejecucion = "[OK]"


// COMMAND ----------

// MAGIC %md
// MAGIC
// MAGIC ##Variables desde Data Factory

// COMMAND ----------

// // Variables Pruebas

// delimitador = ";"
// encabezado = "True"
// dir_adls = "abfss://bigdatadev@stbigdatadev02.dfs.core.windows.net/data/gestion_recursos/logistica/sap/stock_series_convergente"
// pipelineRunId = "643b21d2-0d1e-494b-a77f-0ff5dc703eef"
// catalog_control = "desarrollo.control.control_ingestas"
// formato_entrada = "yyyyMMdd"
// dataType = "raw"
// plataforma = "sap"
// categoria = "stock_series"
// loadType = "i"
// periodicity = "d"
// starttime_nifi = "20240708 100953"
// endtime_nifi = "20240708 101130"
// original_file_size = "112048"
// original_file_date = "2024-07-04T18:38:24Z"
// formato_salida = "yyyy-MM-dd"
// dateFormatFile = "d/MM/yyyy"
// timestampFormat = "yyyy-MM-dd HH|mm|ss"
// dir_adls_2 = "abfss://bigdatadev@stbigdatadev02.dfs.core.windows.net/data/gestion_recursos/logistica/sap/stock_series_convergente/landing/2024/07"
// filename = "0404_Stk_Ser_20240703.txt.gz"
// partition_date_column = ""
// quote = "'"
// nomb_proc = "bidesarrollo.raw_gestion_recursos.stock_series_convergente"
// nomb_schema = nomb_proc.split('.')(0)+"."+nomb_proc.split('.')(1)        

// COMMAND ----------

//Recibe las variables desde Data Factory para utilizarlas durante el proceso

try {
  delimitador = dbutils.widgets.get("delimitador")
  encabezado = dbutils.widgets.get("encabezado").toLowerCase()
  dir_adls = dbutils.widgets.get("dir_adls")
  pipelineRunId = dbutils.widgets.get("pipelineRunId")
  catalog_control = dbutils.widgets.get("catalog_control")
  formato_entrada = dbutils.widgets.get("formato_entrada")
  dataType = dbutils.widgets.get("dataType")
  plataforma = dbutils.widgets.get("plataforma")
  categoria = dbutils.widgets.get("categoria")
  loadType = dbutils.widgets.get("loadType")
  periodicity = dbutils.widgets.get("periodicity")
  starttime_nifi = dbutils.widgets.get("starttime_nifi")
  endtime_nifi = dbutils.widgets.get("endtime_nifi")
  original_file_size = dbutils.widgets.get("original_file_size")
  original_file_date = dbutils.widgets.get("original_file_date")
  formato_salida = Option(dbutils.widgets.get("formato_salida")).filterNot(_.isEmpty).getOrElse("yyyy-MM-dd")
  dateFormatFile = Option(dbutils.widgets.get("dateFormatFile")).filterNot(_.isEmpty).getOrElse("yyyy-MM-dd")
  timestampFormat = Option(dbutils.widgets.get("timestampFormat")).filterNot(_.isEmpty).getOrElse("yyyy-MM-dd'T'HH:mm:ss[.SSS][XXX]").replace("|",":")
  dir_adls_2 = dbutils.widgets.get("dir_adls_2")
  filename = dbutils.widgets.get("filename")
  partition_date_column = dbutils.widgets.get("partition_date_column")
  quote = scala.util.Try(dbutils.widgets.get("quote")).getOrElse("\"").replace("none","")

  nomb_proc = dbutils.widgets.get("nomb_proc")
  nomb_schema = nomb_proc.split('.')(0)+"."+nomb_proc.split('.')(1)

} catch {
    case e: Exception =>
      status_ejecucion = 0
      desc_status_ejecucion = "[ERROR] " + e
      println("[ERROR] " + e)
} 

// COMMAND ----------

//add by JS////////////////////////////
// en caso de no traer split_index_datefile, se mantiene en 1.
try {
    split_index_datefile = dbutils.widgets.get("split_index_datefile").toInt
  } catch {
    case e: Exception =>
      split_index_datefile = -1
  }
//////////////////////////////////////

// COMMAND ----------

println(delimitador)
println(encabezado)
println(dir_adls)
println(pipelineRunId)
println(catalog_control)
println(formato_entrada)
println(dataType)
println(plataforma)
println(categoria)
println(loadType)
println(periodicity)
println(starttime_nifi)
println(endtime_nifi)
println(original_file_size)
println(original_file_date)
println(formato_salida)
println(dateFormatFile)
println(timestampFormat)
println(dir_adls_2)
println(filename)
println(partition_date_column)
println(quote)
println(nomb_proc)
println(nomb_schema)

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
    path_processing = dir_adls + "/processing/"
    path_landing = dir_adls_2
    path_processing_error = dir_adls + "/processing_error/"
    path_log = dir_adls + "/log/"
    path_data = dir_adls + "/" + dataType + "/"
    path_tmp = dir_adls + "/tmp/"
    path_tmp_transform = dir_adls + "/tmp_transform/"

    println("[INFO] path_processing " + path_processing)
    println("[INFO] path_log " + path_log)
    println("[INFO] path_landing " + path_landing)
    println("[INFO] path_processing_error " + path_processing_error)
    println("[INFO] delimitador " + delimitador)
    println("[INFO] encabezado " + encabezado)
    println("[INFO] formato_entrada " + formato_entrada)
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
    println("current " + current)
    starttime_spark = formatter.format(current)
    println("starttime " + starttime_spark)
    var insert_date = formatter2.format(current)
    println("insert_date " + insert_date)
    println("[INFO] Operaciones_ETL_Batch")

    //INDICA EL TOTAL DE TIEMPO EN SEGUNDOS QUE DEMORO LA EJECUCION DE NIFI.
    starttime_nifi_dateFormat = format_actual.parse(starttime_nifi)
    endtime_nifi_dateFormat = format_actual.parse(endtime_nifi)
    totaltime_nifi = ((endtime_nifi_dateFormat.getTime() - starttime_nifi_dateFormat.getTime())/1000).toString()

    println("starttime_nifi "+starttime_nifi)
    println("endtime_nifi "+endtime_nifi)
    println("totaltime_nifi "+totaltime_nifi)

  } catch{
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
    // DESDE EL DIRECTORIO HDFS SE OBTIENE EL ULTIMO ELEMENTO HACIENDO SPLIT POR "/" CON EL FIN DE OBTENER EL NOMBRE DE LA ENTIDAD
    // ej: /data/interacciones/ordenes/oap/detalle_numeros_portados EL VALOR OBTENIDO SERIA detalle_numeros_portados
    nombre_archivo = dir_adls.split("/").last

    // SE LEE EL ARCHIVO HQL EL CUAL CONTIENE LAS INSTRUCCIONES ddl DE CREATE TABLE PARA HIVE.
    // (ESTE DEBE TENER EL MISMO NOMBRE QUE LA VARIABLE OBTENIDA EN EL PASO ANTERIOR EJ: detalle_numeros_portados.hql
    subida_datos = openFile(dir_adls + "/" + nombre_archivo + ".hql") 

    // REPLACE LOCATION MANAGED
    subida_datos = subida_datos.replace("LOCATIONMANAGEDCONTAINERHQL", path_data)
    subida_datos = subida_datos.replace("NOMBREPROCUNITYCATALOGDATABRICKS", nomb_proc)

    println("[INFO] subida_datos " + subida_datos)

    // SE LEE EL ARCHIVO JSON EL CUAL CONTIENE LAS ESTRUCTURA Y ESQUEMA CON SUS TIPOS DE DATOS .
    // (ESTE DEBE TENER EL MISMO NOMBRE QUE LA VARIABLE OBTENIDA EN EL PASO ANTERIOR EJ: detalle_numeros_portados.json
    schema_data = DataType.fromJson(openFile(dir_adls + "/" + nombre_archivo + ".json")).asInstanceOf[StructType]
    println("[INFO] schema_data " + schema_data)

  
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
    /*
    DESDE EL HQL LEIDO SE REALIZA UN SUBSTRING PARA OBTENER EL NOMBRE DEL PROCESO DESDE LA PRIMERA LINEA DEL ARCHIVO
    ej: CREATE EXTERNAL TABLE IF NOT EXISTS capa_semantica.detalle_numeros_portados tendria el valor de capa_semantica.detalle_numeros_portados
    */

    nomb_proc = subida_datos.substring(0, subida_datos.indexOf('\n')).substring(subida_datos.toLowerCase().indexOf("create external table if not exists ") + 36).trim()
    println("[INFO] nombre proceso " + nomb_proc)


    //SE CONCATENA SPARK AL NOMBRE DEL PROCESO
    process_name = "spark_" + nomb_proc
    println("[INFO] spark + nombre proceso " + process_name)


    // SE DEFINE QUE EL TIPO DE FUENTE SERA UN ARCHIVO
    println("[INFO] path data " + path_data)


    //SE DEFINEN DATAFRAMES VACIOS Y SE EJECUTA EL HQL EN HIVE CREANDO LA TABLA EXTERNA
    println("[INFO] Creación de tabla si no existe")
    df1 = spark.sql(subida_datos)
    df2 = df1
    df3 = df1
    df_join = df1
    df4 = df1
    df5 = df1
    df_sal2 = df1
    df_sal3 = df1
    df_sal4 = df1

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
    // SE REALIZA UN SUBSTRING AL HQL Y SE SEPARA LINEA POR LINEA OBTENIENDO CAMPO POR CAMPO , ESTO SE GUARDA EN UN ARRAY.
    val arreglo_string = subida_datos.substring(subida_datos.indexOf("(") + 2, subida_datos.indexOf(")\n")).split("\n")
    var variable = ""
    var select_line = "select "
    var select_line2 = ""
    var path_data2 = ""
    var partition_format = ""
    
    /* SE RECORRE LA LISTA CON LOS CAMPOS ,A LA VARIABLE SELECT_LINE SE LE CONCATENAN TODOS LOS CAMPOS FORMANDO UNA QUERY*/
    for (campo <- arreglo_string if campo.trim() != "") {
      variable = campo.trim().substring(0, campo.trim().indexOf(" "))
      println("[INFO] " + variable)
      select_line = select_line.concat(variable + ",")
    }

    var partition_name = ""
    
    //SE DEFINE CONTADOR
    var counter = 1
    
    // SE OBTIENE UN STATUS GLOBAL DE LA RUTA HDFS
    val status = listFiles(dir_adls)
    var columns = Seq.empty[String]
    var process_type = ""
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

    // // //VALIDA SI EXISTE EL ARCHIVO TRANSFORMATION.JSON LA INGESTA REQUIERE PRE PROCESAMIENTO
    if (exists_file(dir_adls + "/transformation/transformation.json")) {
      println("Entro al if")

      /* SE LEE EL ARCHIVO JSON EL CUAL CONTIENE LAS ESTRUCTURA Y ESQUEMA CON SUS TIPOS DE DATOS. */
      schema_data_trns = DataType.fromJson(openFile(dir_adls + "/transformation/transformation.json")).asInstanceOf[StructType]
      
      //CREA UN OBJETO CON TODO LO EXISTENTE EN EL DIRECTORIO TRANSFORMATION
      val statSubquery = listFiles(dir_adls + "/transformation/")
      
      //FILTRA LOS ELEMENTOS QUE SEAN ARCHIVOS Y PREGUNTA POR LOS NOMBRES DE ARCHIVOS QUE COMIENCEN CON SUBQUERY
      val subQueryCount = statSubquery.filter(_.isFile).filter(_.name.startsWith("subquery"))
      
      //CREA UN ARREGLO VACIO DE TAMAÑO N SEGUN CANTIDAD DE ARCHIVOS SUBQUERY EXISTAN
      val querysResult = new Array[String](subQueryCount.size)//(any)

      // SE LEE EL ARCHIVO CSV CON LA OPCION FAILFAST Y SCHEMA. SI EL SCHEMA PROPORCIONADO NO CONCUERDA CON EL ARCHIVO EL PROCESO SE DETIENE POR REGISTROS MALFORMADOS
      df1 = spark.read.option("delimiter", delimitador).
      option("header", encabezado).
      option("mode", "FAILFAST").
      option("quote",quote).
      schema(schema_data_trns).
      csv(dir_adls_2 + "/*")
      
      //GUARDA LAS COLUMNAS DEL DATAFRAME
      val groupCols = df1.columns
      
      // AGRUPA LA DATA CARGADA POR EL NOMBRE DEL ARCHIVO BUSCANDO PARA CADA UNO EL MAXIMO TIMESTAMP
      //CON ESTO SE ASEGURA QUE EN EL REPROCESO SIEMPRE SE TOME EL ARCHIVO REPROCESADO MAS ACTUAL
      val df_ts = df1.groupBy("filename_spark").agg(max("ts").as("ts"))

      //SE HACE UN JOIN PARA DEJAR SOLO LOS DATASET QUE TENGAN EL TIMESTAMP MAS ACTUAL POR ARCHIVO
      df_join=df1.join(df_ts,Seq("filename_spark","ts"))
      println("df_join")
      df_join.show()

      //SE SELECCIONAN LAS COLUMNAS Y SE ELIMINA LA COLUMNA TS
      df_join = df_join.select(groupCols.map(col): _*).drop("ts")
      
      //SE REGISTRA COMO VISTA TEMPORAL
      df_join.createOrReplaceTempView(dir_adls.split("/").last)
      
      //CARGA LA QUERY CON LAS TRANSFORMACIONES
      query = openFile(dir_adls + "/transformation/transformation.hql")
      query2=query

      //RECORRE EL ARREGLO Y APLICA LAS SUBQUERY EXISTENTES ALMACENANDO EL RESULTADO EN UNA VARIABLE
      //CONCATENA A LA QUERY PRINCIPAL LOS VALORES DE LA SUBQUERY
      for (i <- 0 until subQueryCount.size) {
        var subq = openFile(subQueryCount(i).path.toString())
        querysResult(i) = spark.sql(subq).first.getString(0)//get(0)
        query2 = query2.replace("querysResult("+i+")",querysResult(i))
      }
          
      //EJECUTA LA QUERY Y GUARDA EL RESULTADO EN UN DATAFRAME
      df2 = spark.sql(query2)

      //ESCRIBE LA DATA CON LAS TRANSFORMACIONES APLICADAS A UN DIRECTORIO TEMPORAL
      df2.write.
      option("delimiter", delimitador).
      option("header", encabezado).
      option("quote",quote).
      mode(SaveMode.Overwrite).
      csv(path_tmp_transform)
      
      // SE LEE EL ARCHIVO CSV CON LA OPCION FAILFAST Y SCHEMA. SI EL SCHEMA PROPORCIONADO NO CONCUERDA CON EL ARCHIVO EL PROCESO SE DETIENE POR REGISTROS MALFORMADOS
      df3 = spark.read.
      option("delimiter", delimitador).
      option("header", encabezado).
      option("mode", "FAILFAST").
      option("DateFormat", dateFormatFile).
      option("timestampFormat",timestampFormat).
      option("quote",quote).
      schema(schema_data).
      csv(path_tmp_transform + "*")

    } else {
    
      println("Entro al else")

      //EN CASO DE NO NECESITAR TRANSFORMACIONES SE LEE EL ARCHIVO CON EL ESQUEMA PROPORCIONADO
      df1 = spark.read.option("delimiter", delimitador).option("header", encabezado).
      option("mode", "FAILFAST").schema(schema_data).option("DateFormat", dateFormatFile).option("timestampFormat", timestampFormat).csv(path_landing + "/*")
      //PERMISSIVE
      //FAILFAST
        
      val contadorcolumns = df1.count()
      println("contador columns")
      println(contadorcolumns)
      println(df1.printSchema())
      df1.show()

      //GUARDA LAS COLUMNAS DEL DATAFRAME
      val groupCols = df1.columns

      println("llega a guarda las columnas en dataframe")
      // AGRUPA LA DATA CARGADA POR EL NOMBRE DEL ARCHIVO BUSCANDO PARA CADA UNO EL MAXIMO TIMESTAMP
      //CON ESTO SE ASEGURA QUE EN EL REPROCESO SIEMPRE SE TOME EL ARCHIVO REPROCESADO MAS ACTUAL
      val df_ts = df1.groupBy("filename_spark").agg(max("ts").as("ts"))
      println("df_ts")
      println(df_ts)

      //SE HACE UN JOIN PARA DEJAR SOLO LOS DATASET QUE TENGAN EL TIMESTAMP MAS ACTUAL POR ARCHIVO
      df_join=df1.join(df_ts,Seq("filename_spark", "ts"))
      println("df_join")
      println(df_join.count())

      //SE SELECCIONAN LAS COLUMNAS Y SE ELIMINA LA COLUMNA TS
      df3 = df_join.select(groupCols.map(col): _*).drop("ts")
    }

    //SE ELIMINAN DUPLICADOS
    df3 = df3.dropDuplicates
    println(df3.count())
    //SE CUENTA LA CANTIDAD DE REGISTROS
    original_row_count = df3.count
    println(original_row_count)

    // ELIMINA FILAS NULAS
    df4 = df3.na.drop("all")

    // SE GENERA EL TIMESTAMP
    current_id = new Date().getTime
    
    //CREA UN FORMATO DE TIMESTAMP
    formatter_id = new SimpleDateFormat("yyyyMMddHHmmss")
    
    //SE CONCATENA EL TIMESTAMP MAS EL CONTADOR GENERANDO UN ID UNICO PARA LA TABLA DE CONTROL
    bigdata_ctrl_id = formatter_id.format(current_id) + "%03d".format(counter)


    // SE AGREGA LA COLUMNA BIGDATA_CLOSE_DATE Y BIGDATA_CTRL_ID AL DATAFRAME
    df4 = df4.withColumn("bigdata_close_date", lit(null)).withColumn("bigdata_ctrl_id", lit(bigdata_ctrl_id) cast "long")

    /*SI LA COLUMNA DE PARTICION ES VACIA SE UTILIZA EL UDF Y SE GENERA LA COLUMNA BIGDATA_CLOSE_DATE DESDE LA COLUMNA FILENAME DEL DATAFRAME LA CUAL TIENE LOS FILENAMES DE LOS ARCHIVOS 
    EN CASO DE TENER COLUMNA DE PARTICION SE FORMATEA LA COLUMNA DESDE EL FORMATO DE ENTRADA AL FORMATO DE SALIDA ESPECIFICADO*/

    //// mod by JS: Se verifica que el índice de split de fechas sea -1 para ultimo elemento (fecha sin hora) o -2 para penultimo elemento (fecha con hora)
    //val partition_date_column = ""
    if (partition_date_column == "") {
      if (split_index_datefile == -1) {
        df4 = df4.withColumn("bigdata_close_date", date_format(to_date(regexp_replace(element_at(split(col("filename_spark"),"_"), -1), "[^0-9]", ""), formato_entrada), formato_salida) cast "date")
      } else {
        df4 = df4.withColumn("bigdata_close_date", date_format(to_date(regexp_replace(element_at(split(col("filename_spark"),"_"), split_index_datefile), "[^0-9]", ""), formato_entrada), formato_salida) cast "date")
      }
    } else {
      df4 = df4.withColumn("bigdata_close_date", from_unixtime(unix_timestamp(col(partition_date_column), formato_entrada), formato_salida) cast "date")
    }
    ///////////

    //GENERA UN ARREGLO CON LAS DISTINTAS FECHAS EXISTENTES EN EL DATAFRAME
    val dates = df4.select("bigdata_close_date").distinct().as[String].collect()

    //SE OBTIENE EL NOMBRE DE LA COLUMNA O COLUMNAS DE PARTICION DESDE EL HQL. ej: (partition_date,year,month,day)
    partition_name = subida_datos.substring(subida_datos.toLowerCase().indexOf("partitioned by") + 15, subida_datos.toLowerCase().indexOf("partitioned by") + 15 + subida_datos.substring(subida_datos.toLowerCase().indexOf("partitioned by") + 15).indexOf(")")).replace("(", "").replace("string", "").trim()
    println("partition_name " + partition_name)
    
    //SE SEPARA LA COLUMNA DE PARTICION POR "," EN CASO QUE SEA MAS DE UNA COLUMNA
    var splits = partition_name.split(",").size
    println(splits)
    
    //SE CREAN 2 LISTAS STRING, UNA PARA ALMACENAR LA LISTA DE RUTAS HDFS Y OTRA PARA LA LISTA DE COLUMNAS DE PARTICION
    var path_data2_seq = Seq.empty[String]
    println("llegamos a crear dos listas string")
    
    /* SE RECORRE EL ARREGLO CON LAS FECHAS, SE CONSTRUYEN LAS DISTINTAS RUTAS HDFS PARTICIONADAS AGREGANDO EL NOMBRE DE LA COLUMNA Y SU VALOR DONDE SE CARGARA LA DATA.
    SE AGREGA A LA LISTA LOS NOMBRES DE LAS COLUMNAS DE PARTICION
    */
    println("dates ", dates)
    println(dates.length)
    println(splits)

    for (i <- 0 until dates.length) {
      path_data2 = path_data
      println("entro al for")
      println(dates)
      for (x <- 0 to splits - 1 by 1) {
        println("partition_name en el for" + partition_name)

        if (x != splits - 1 && splits > 1) {
          path_data2 = path_data2.concat(partition_name.split(",")(x).trim() + "=" + dates(i).toString.split("-")(x) + "/")
          columns = columns :+ partition_name.split(",")(x).trim()

        } else if (x == splits - 1 && splits > 1) {
          path_data2 = path_data2.concat(partition_name.split(",")(x).trim() + "=" + dates(i).toString.split("-")(x))
          columns = columns :+ partition_name.split(",")(x).trim()
        } else if (x == splits - 1 && splits == 1) {
          path_data2 = path_data2.concat(partition_name.split(",")(x).trim() + "=" + dates(i).toString.split("-")(x))
          columns = columns :+ partition_name.split(",")(x).trim()
        }

      }
      path_data2_seq = path_data2_seq :+ path_data2
      counter = counter + 1
      println(counter)

    }
        
    //SE ELIMINAN COLUMNAS Y RUTAS DE PARTICION DUPLICADAS
    path_data2_seq = path_data2_seq.distinct
    println(path_data2_seq)
    
    columns = columns.distinct
    println("columns distinct")
    println(columns)
    println("pasamos for")
    //SE GENERA UN SELECT DE LAS COLUMNAS DEL DATAFRAME RECORRIENDO SUS COLUMNAS Y SUMANDO LAS COLUMNAS DE PARTICION MAS SUS VALORES
    val selectExprs = df4.columns.map(col) ++ (0 until columns.size map (i => $"tmp".getItem(i).as(columns(i))))

    //SE APLICA EL SELECT CREADO AL DATAFRAME
    df5 = df4.withColumn("tmp", split($"bigdata_close_date", "-")).select(selectExprs: _*).drop("filename_spark")
    println(df5.count)
    println("dfshow")
    df5.show()
    // SE ESCRIBE EL DATAFRAME A UN DIRECTORIO TEMPORAL
    df5.write.format("delta").mode(SaveMode.Overwrite).save(path_tmp)

    // SE CALCULA LA CANTIDAD DE ARCHIVOS A ESCRIBIR EN DATA LAKE.
    numPartitions = numPartitionsCalc(path_tmp)
    
    // SE ELIMINA LO ESCRITO EN LA RUTA TEMPORAL
    delete(path_tmp)
    println(path_tmp)
    println(path_data2_seq)

    //SE ELIMINAN TODOS LOS DIRECTORIOS PARTICIONADOS QUE SE CARGARAN EN CASO QUE EXISTAN
    // deletePartitions(path_data2_seq)
    println("valor df5")
    val valor = df5.count()
    println(valor)
    // SE ESCRIBE EL RESULTADO CON EL NUMERO DE ARCHIVOS CALCULADOS, SE PARTICIONA POR LA O LAS COLUMNAS DE PARTICION.
    df5.repartition(numPartitions).write.partitionBy(columns: _*).format("delta").option("partitionOverwriteMode", "dynamic").mode(SaveMode.Overwrite).save(path_data)

    // SE ELIMINA LO ESCRITO EN LA RUTA TEMPORAL DE TRANSFORMACIONES
    delete(path_tmp_transform)
      
    // SE RECORRE LA LISTA CON LOS DIRECTORIOS
    for (i <- 0 until path_data2_seq.length) {
      
      // SE CALCULA EL TIMESTAMP
      current_ins = new Date().getTime

      // SE CREA UN FORMATO DE FECHA
      formatter_ins = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")

      //SE FORMATEA EL TIMESTAMP Y SE ALMACENA EN LA VARIABLE QUE INDICA EL TIEMPO DE INSERCION DEL ARCHIVO/REGISTRO.
      insert_data_ctrl_date = formatter_ins.format(current_ins)

      // SE OBTIENE EL TAMAÑO FINAL DE LOS ARCHIVOS ESCRITOS
      final_file_size = sizeFile(path_data)

      // SE CUENTA LA CANTIDAD DE REGISTROS DEL ARCHIVO FINAL
      final_row_count = spark.read.format("delta").load(path_data).count

      // SE VALIDA SI LA CANTIDAD DE FILAS ORIGINALES VS LA CANTIDAD DE FILAS ESCRITAS ES DISTINTA
      if (original_row_count != final_row_count) dif_row_count = 1 else dif_row_count = 0

      // SE CUENTA LA CANTIDAD DE ARCHIVOS GENERADOS
      final_number_of_files = countFiles(path_data)

      // SE GUARDA EL NOMBRE FINAL DEL ARCHIVO
      val current = DateTimeFormatter.ofPattern("yyyyMMddHHmmss").format(LocalDateTime.now)
      end_file_name = dataType + "." + plataforma + "." + categoria + "." + loadType + "." + periodicity + "." + current + ".snappy.parquet" 
      //validar si este es el end_file_name
      //println("[INFO] final_name "+end_file_name)

      // SE ACTUALIZA LA TABLA EXTERNA PARA QUE TOME LAS PARTICIONES MODIFICADAS/NUEVAS
      // spark.sql("MSCK REPAIR TABLE " + nomb_proc)
      
      //SE AGREGAN LOS PARAMETOS DE CONTROL A LA LISTA
      controlData = controlData :+ ((status_ejecucion.toString, desc_status_ejecucion, bigdata_ctrl_id, process_name, data_source_type, filename, original_file_date, starttime_nifi, endtime_nifi, totaltime_nifi, starttime_spark, process_type, original_file_size, final_file_size.toString, original_row_count.toString, final_row_count.toString, dif_row_count.toString, final_number_of_files.toString, end_file_name, insert_data_ctrl_date, path_data, pipelineRunId))
    }

   println("[INFO] proceso terminado")

  } catch {
      case e: Exception =>
        {
          status_ejecucion = 1
          desc_status_ejecucion = "[ERROR] " + e
          println("[ERROR] " + e)
        }
  } 
} else {
  println("Skipped - Ejecución = " + status_ejecucion)
}

// COMMAND ----------

//SE CREA UN DATAFRAME CON TODOS LOS REGISTROS PARA LA TABLA DE CONTROL
var control_dataframe = controlData.toDF("status", "desc_status","big_data_ctrl_id", "process_name", "data_source_type", "data_source_name", "original_file_date", "starttime_nifi", "endtime_nifi", "totaltime_nifi", "starttime_spark", "process_type", "original_file_size", "final_file_size", "original_row_count", "final_row_count", "dif_row_count", "final_number_of_files", "end_file_name", "insert_data_ctrl_date", "hdfs_path", "pipelineRunId")

// SE OBTIENE EL TIMESTAMP
var end = new Date().getTime

//SE FORMATEA EL TIMESTAMP 
var endtime_spark = formatter.format(end)

//SE CALCULA LA DURACION DEL PROCESO SPARK
var totaltime_spark = (end - current).toFloat / 1000

//SE CALCULA LA DURACION DEL PROCESO SPARK + NIFI
var totaltime_process = totaltime_spark + totaltime_nifi.toInt


// SE FORMATEAN LOS TIMESTAMP AL DATAFRAME DE CONTROL.
control_dataframe = control_dataframe.
  withColumn("endtime_spark", lit(endtime_spark)).
  withColumn("totaltime_spark", lit(totaltime_spark)).
  withColumn("totaltime_process", lit(totaltime_process)).
  withColumn("original_file_date", from_unixtime(unix_timestamp($"original_file_date", "yyyyMMdd HHmmss"), "yyyy-MM-dd HH:mm:ss")).
  withColumn("starttime_nifi", from_unixtime(unix_timestamp($"starttime_nifi", "yyyyMMdd HHmmss"), "yyyy-MM-dd HH:mm:ss")).
  withColumn("endtime_nifi", from_unixtime(unix_timestamp($"endtime_nifi", "yyyyMMdd HHmmss"), "yyyy-MM-dd HH:mm:ss")).
  select("status", "desc_status","big_data_ctrl_id", "process_name", "data_source_type", "data_source_name", "original_file_date", "starttime_nifi", "endtime_nifi", "totaltime_nifi", "starttime_spark", "endtime_spark", "totaltime_spark", "totaltime_process", "insert_data_ctrl_date", "process_type", "original_file_size", "final_file_size", "original_row_count", "final_row_count", "dif_row_count", "final_number_of_files", "end_file_name", "hdfs_path", "pipelineRunId")

// SE ESCRIBEN LOS REGISTROS DE CONTROL EN HIVE
control_dataframe.write.mode(SaveMode.Append).option("mergeSchema", "true").saveAsTable(catalog_control)

// COMMAND ----------

/* val status_ejecucion = 0
val status_error: Exception = null */
if(status_ejecucion == 0){
  dbutils.notebook.exit("OK")
}else{
  throw new Exception("Error en el Try-Catch del proceso.")
}
