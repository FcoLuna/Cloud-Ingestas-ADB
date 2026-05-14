// Databricks notebook source
// MAGIC %md
// MAGIC ### PARAMETROS NOTEBOOK

// COMMAND ----------

//delimitador por el cual se separaran las columnas del archivo (,;|~,etc)
val delimitador = dbutils.widgets.get("delimitador")
//INDICA SI EL ARCHIVO TRAE O NO CABEZERA (true|false)
val encabezado = dbutils.widgets.get("encabezado").toLowerCase()
//DIRECTORIO HDFS DESDE DONDE SE EJECUTARA LA LECTURA Y CARGA DE ARCHIVOS.
val dir_adls = dbutils.widgets.get("dir_adls")
//FORMATO DE FECHA QUE VIENE EN EL NOMBRE DEL ARCHIVO DE ENTRADA (yyyy-MM-dd,yyyyMMdd,ddMMyyyy,etc)
val formato_entrada = dbutils.widgets.get("formato_entrada")
//INDICA SI LA DATA ES DE TIPO RAW,NORAW,CONFORMADO.
val dataType = dbutils.widgets.get("dataType")
//INDICA DESDE DONDE VIENE LA INFORMACION A CARGAR (ATIS,SAP,ETC)
val plataforma = dbutils.widgets.get("plataforma")
//INDICA A QUE CATEGORIA DE LOS DOMINIOS DE INFORMACION CORRESPONDEN(INTERACCIONES,CAMPAÑAS,GESTION_RECURSOS,ETC)
val categoria = dbutils.widgets.get("categoria")
//INDICA EL TIPO DE CARGA A EJECUTAR INCREMENTAL,FULL,etc (i,d)
val loadType = dbutils.widgets.get("loadType")
//INDICA LA PERIODICIDAD DE LA INGESTA, diaria, semanal, mensual (d,s,m).
val periodicity = dbutils.widgets.get("periodicity")
//INDICA EL TIMESTAMP DEL MOMENTO EN QUE NIFI INICIO EL FLUJO.
val starttime_nifi = dbutils.widgets.get("starttime_nifi")
//INDICA EL TIMESTAMP DEL MOMENTO EN QUE NIFI TERMINO EL FLUJO.
val endtime_nifi = dbutils.widgets.get("endtime_nifi")
//EL TAMAÑO ORIGINAL DEL ARCHIVO EN BYTES EN EL REPOSITORIO DE ORIGEN
val original_file_size = dbutils.widgets.get("original_file_size")
//EL TIMESTAMP ORIGINAL EN LA QUE EL ARCHIVO FUE CREADO/MOVIDO EN EL REPOSITORIO DE ORIGEN.
val original_file_date = dbutils.widgets.get("original_file_date")
//FORMATO QUE TRAEN LOS CAMPOS TIPO FECHA EN EL ARCHIVO. POR DEFECTO yyyy-MM-dd
val dateFormatFile = Option(dbutils.widgets.get("dateFormatFile")).filterNot(_.isEmpty).getOrElse("yyyy-MM-dd")
//FORMATO DE TIMESTAMP QUE TENDRAN LAS COLUMNAS TIPO TIMESTAMP EN EL ARCHIVO (POR DEFECTO yyyy-MM-dd'T'HH:mm:ss[.SSS][XXX])
var timestampFormat = Option(dbutils.widgets.get("timestampFormat")).filterNot(_.isEmpty).getOrElse("yyyy-MM-dd'T'HH:mm:ss[.SSS][XXX]").replace("|", ":")
//FORMATO DE SALIDA PARA EL CAMPO BIGDATA CLOSE DATE. POR DEFECTO yyyy-MM-dd
val formato_salida = Option(dbutils.widgets.get("formato_salida")).filterNot(_.isEmpty).getOrElse("yyyy-MM-dd")
val cant_repartition = Option(dbutils.widgets.get("cant_repartition")).filterNot(_.isEmpty).getOrElse("-1").toInt

val nomb_proc = dbutils.widgets.get("nomb_proc")

// Pipeline Run ID del flujo en Data Factory
val pipelineRunId = dbutils.widgets.get("pipelineRunId")

// Ctalogo, esquema y tabla de control
val catalog_control = dbutils.widgets.get("catalog_control")


// COMMAND ----------

println("original_file_size "+original_file_size)
println("original_file_date "+original_file_date)
println("--")
println("dateFormatFile "+dateFormatFile)
println("timestampFormat "+timestampFormat)
println("formato_salida "+formato_salida)

// COMMAND ----------

// MAGIC %md
// MAGIC ### Importar librerías y definición de variables

// COMMAND ----------

// package com.tchile.bigdata.etl

import java.text.SimpleDateFormat
import java.util.TimeZone
import java.util.Date
import java.time.format.DateTimeFormatter
import java.util.concurrent.TimeUnit
// import org.apache.hadoop.conf.Configuration
// import org.apache.spark.SparkConf
import org.apache.spark.sql.SaveMode
// import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType
// import com.tchile.bigdata.hdfs.ManejoHdfs
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DataType
//librerias HDFS


// import org.apache.commons.io
// import org.apache.commons.io.FileUtils


// COMMAND ----------

// MAGIC %run /Shared/framework_ingesta/funciones/funciones_genericas

// COMMAND ----------

// MAGIC %md
// MAGIC ### MANEJO FECHA ULTIMA MODIFICACION

// COMMAND ----------

val format_actual = new SimpleDateFormat("yyyyMMdd HHmmss")
val format_max = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")
val format_timestamp = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'")  // 2023-11-30T17:08:11Z
val format_2 = new SimpleDateFormat("yyyyMMddHHmmss")
format_actual.setTimeZone(TimeZone.getTimeZone("Chile/Continental"))
format_max.setTimeZone(TimeZone.getTimeZone("Chile/Continental"))
format_timestamp.setTimeZone(TimeZone.getTimeZone("Chile/Continental"))
format_2.setTimeZone(TimeZone.getTimeZone("Chile/Continental"))

println("[INFO] original_file_date " + original_file_date)
val datetime_fecha_modificacion_actual = format_timestamp.parse(original_file_date) // fecha actual en formato date
val string_fecha_modificacion_actual = format_max.format(datetime_fecha_modificacion_actual) // fecha actual en formato string

var datetime_fecha_modificacion_actual_plus_1 = datetime_fecha_modificacion_actual.toInstant().plusMillis( 1 ) 
var datetime_fecha_modificacion_actual_aux = Date.from(datetime_fecha_modificacion_actual_plus_1);
val string_fecha_modificacion_actual_plus_1 = format_max.format(datetime_fecha_modificacion_actual_aux) 

if(!(exists_file(dir_adls + "/last_ingest_time.txt"))){ // si no existe el archivo con la fecha maxima, lo crea con la fecha actual (usado por los siguientes archivos al ser procesados)
    println("no existe archivo con fecha maxima  [CREANDO ARCHIVO]");
    makeTxtFile(dir_adls + "/last_ingest_time.txt", "timestamp;\n"+string_fecha_modificacion_actual_plus_1+";")
}else{ // si existe el archivo, lo lee y compara las fechas

  val fecha_modificacion_max = readTxtFile(dir_adls + "/last_ingest_time.txt").split("\n")(1).dropRight(1)
  val datetime_fecha_modificacion_max = format_max.parse(fecha_modificacion_max) // fecha maxima en formato string
  
  var result: Int = 0;
  result = datetime_fecha_modificacion_actual.compareTo(datetime_fecha_modificacion_max);

  if(result > 0){
    println("[INFO] fecha actual mayor que la fecha del archivo  [ACTUALIZAR FECHA DEL ARCHIVO]");
    delete(dir_adls + "/last_ingest_time.txt")
    makeTxtFile(dir_adls + "/last_ingest_time.txt", "timestamp;\n"+string_fecha_modificacion_actual_plus_1+";")
  }else{
    println("[INFO] fecha actual menor o igual que la fecha del archivo [NO HACER NADA]");
  }
}


// COMMAND ----------

// MAGIC %md
// MAGIC #### Cálculo de Totaltime nifi

// COMMAND ----------

//INDICA EL TOTAL DE TIEMPO EN SEGUNDOS QUE DEMORO LA EJECUCION DE NIFI.
val starttime_nifi_dateFormat = format_actual.parse(starttime_nifi)
val endtime_nifi_dateFormat = format_actual.parse(endtime_nifi)
val totaltime_nifi = ((endtime_nifi_dateFormat.getTime() - starttime_nifi_dateFormat.getTime())/1000).toString()

println("starttime_nifi "+starttime_nifi)
println("endtime_nifi "+endtime_nifi)
println("totaltime_nifi "+totaltime_nifi)

// COMMAND ----------

// MAGIC %md
// MAGIC ### Script ingesta simple

// COMMAND ----------

val current = new Date().getTime
val formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
formatter.setTimeZone(TimeZone.getTimeZone("Chile/Continental"))
val formatter2 = new SimpleDateFormat("yyyy-MM-dd")
formatter2.setTimeZone(TimeZone.getTimeZone("Chile/Continental"))
val starttime_spark = formatter.format(current)
val insert_date = formatter2.format(current)
println("[INFO] Operaciones_ETL_Batch")

// COMMAND ----------

// MAGIC %md
// MAGIC ##### SE DEFINEN LOS DIRECTORIOS DE TRABAJO PARA EL PROCESO

// COMMAND ----------

val path_processing = dir_adls + "/processing/"
val path_landing = dir_adls + "/landing/"
val path_processing_error = dir_adls + "/processing_error/"
val path_log = dir_adls + "/log/"
val path_data = dir_adls + "/" + dataType + "/"
val path_tmp = dir_adls + "/tmp/"
val path_tmp_transform = dir_adls + "/tmp_transform/"

println("[INFO] path_processing " + path_processing)
println("[INFO] path_log " + path_log)
println("[INFO] path_landing " + path_landing)
println("[INFO] path_processing_error " + path_processing_error)
println("[INFO] delimitador " + delimitador)
println("[INFO] encabezado " + encabezado)
println("[INFO] formato_entrada " + formato_entrada)

// COMMAND ----------

// DESDE EL DIRECTORIO HDFS SE OBTIENE EL ULTIMO ELEMENTO HACIENDO SPLIT POR "/" CON EL FIN DE OBTENER EL NOMBRE DE LA ENTIDAD
// ej: /data/interacciones/ordenes/oap/detalle_numeros_portados EL VALOR OBTENIDO SERIA detalle_numeros_portados
val nombre_archivo = dir_adls.split("/").last

// SE LEE EL ARCHIVO HQL EL CUAL CONTIENE LAS INSTRUCCIONES ddl DE CREATE TABLE PARA HIVE.
// (ESTE DEBE TENER EL MISMO NOMBRE QUE LA VARIABLE OBTENIDA EN EL PASO ANTERIOR EJ: detalle_numeros_portados.hql
var subida_datos = openFile(dir_adls + "/" + nombre_archivo + ".hql") 

// REPLACE LOCATION MANAGED
subida_datos = subida_datos.replace("LOCATIONMANAGEDCONTAINERHQL", dir_adls)

println("[INFO] subida_datos " + subida_datos)

// SE LEE EL ARCHIVO JSON EL CUAL CONTIENE LAS ESTRUCTURA Y ESQUEMA CON SUS TIPOS DE DATOS .
// (ESTE DEBE TENER EL MISMO NOMBRE QUE LA VARIABLE OBTENIDA EN EL PASO ANTERIOR EJ: detalle_numeros_portados.json
val schema_data = DataType.fromJson(openFile(dir_adls + "/" + nombre_archivo + ".json")).asInstanceOf[StructType]
println("[INFO] schema_data " + schema_data)

//SE DEFINE UN ESQUEMA NULL PARA LAS TRANSFORMACIONES
var schema_data_trns = null.asInstanceOf[StructType]

//SE GENERAN VARIABLES STRING VACIAS PARA ALMACENAR LAS QUERYS PARA LAS TRANSFORMACIONES
var query = ""
var query2 = ""

//SE CONCATENA SPARK AL NOMBRE DEL PROCESO
val process_name = "spark_" + nomb_proc
// SE DEFINE QUE EL TIPO DE FUENTE SERA UN ARCHIVO
val data_source_type = "file"
val hdfs_path = path_data

// COMMAND ----------

// MAGIC %md
// MAGIC #### CREACIÓN DE TABLA EN HIVE

// COMMAND ----------

var filename = ""
var salida = 1
var bigdata_close_date = ""
var partition_value = ""
println("[INFO] Creación de tabla si no existe")

//SE DEFINEN DATAFRAMES VACIOS Y SE EJECUTA EL HQL EN HIVE CREANDO LA TABLA EXTERNA
var dataframe1, df1, df2, df3, df_sal1, df_sal2 = spark.sql(subida_datos)
var controlData = Seq.empty[(String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String)]

// COMMAND ----------

// MAGIC %md
// MAGIC #### Try Catch

// COMMAND ----------


try {

  // SE ELIMINA EL DIRECTORIO DE LOG EN CASO DE EXISTIR
  delete(path_log)
  println("""[INFO] Mover archivos HDFS""")

  //SE LISTA TODOS LOS ARCHIVOS EXISTENTES EN EL DIRECTORIO PROCESSING
  var list = listFiles(path_processing)

  // SE RECORRE LA LISTA Y SI EXISTEN ARCHIVOS ESTOS SON MOVIDOS A ERROR YA QUE QUEDARON DE UNA EJECUCION ANTERIOR FALLIDA
  if (list != null && list.length > 0) {
    for (i <- list) {
      filename = i.name
      delete(path_processing_error + filename)
      moverArchivoAbfs(path_processing + filename, path_processing_error + filename)
      println("[INFO] FILENAME ERROR to processing error " + filename)
    }
  } else {
    println("[INFO] NO EXISTE ARCHIVO EN PROCESSING")
  }

  // SE REALIZA UN SUBSTRING AL HQL Y SE SEPARA LINEA POR LINEA OBTENIENDO CAMPO POR CAMPO , ESTO SE GUARDA EN UN ARRAY.
  val arreglo_string = subida_datos.substring(subida_datos.indexOf("(") + 2, subida_datos.indexOf(")\n")).split("\n")
  var variable = ""
  var select_line = "select "
  var select_line2 = ""
  var path_data2 = ""
  var partition_format = ""
  var partition_name = ""

  /* SE RECORRE LA LISTA CON LOS CAMPOS ,A LA VARIABLE SELECT_LINE SE LE CONCATENAN TODOS LOS CAMPOS FORMANDO UNA QUERY*/
  for (campo <- arreglo_string if campo.trim() != "") {
    variable = campo.trim().substring(0, campo.trim().indexOf(" "))
    println("[INFO] " + variable)
    select_line = select_line.concat(variable + ",")
  }

  // SE LISTAN LOS ARCHIVOS EN EL DIRECTORIO LANDING
  // ORDENAR LISTA DE ARCHIVOS POR FECHA
  list = listFiles(path_landing).sortBy(_.name)
  // SE CREA UN CONTADOR
  var counter = 1
  
  if (list != null && list.length > 0) {
    for (k <- list) {
      filename = k.name
      if (!(filename.startsWith("."))) {
        delete(path_processing + filename)
        moverArchivoAbfs(path_landing + filename, path_processing + filename)
        println("[INFO] FILENAME landing " + filename)
        var columns = Seq.empty[String]
        var size_count = Seq.empty[Long]
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

        // SE OBTIENE LA FECHA DEL NOMBRE DEL ARCHIVO CON EL FORMATO DE SALIDA PROPORCIONADO
        partition_value = obtenerParticiondesdeArchivo(filename, formato_entrada, formato_salida)

        /*SE BUSCA EN EL HQL SI EXISTE LA LINEA CON "PARTITIONED BY"*/
        if (subida_datos.toLowerCase().contains("partitioned by")) {
          println("""[INFO] obtener particion desde archivo""")

          //SE OBTIENE EL NOMBRE DE LA COLUMNA O COLUMNAS DE PARTICION DESDE EL HQL. ej: (partition_date,year,month,day)
          val find_text_inicial = subida_datos.toLowerCase().indexOf("partitioned by") + 15
          val find_text_final = find_text_inicial + subida_datos.substring(find_text_inicial).indexOf(")")
          partition_name = subida_datos.substring(find_text_inicial, find_text_final).replace("(", "").replace("string", "").trim()

          //SE SEPARA LA COLUMNA DE PARTICION POR "," EN CASO QUE SEA MAS DE UNA COLUMNA
          var splits = partition_name.split(",").size
          select_line2 = select_line
          path_data2 = path_data
          println("[INFO] partition date: " + partition_name + "," + partition_value)
          println("[INFO] ver select  " + select_line)

          // SE RECORRE LA CANTIDAD DE COLUMNAS DE PARTICION,
          // SE MODIFICA EL SELECT AGREGANDO EL NOMBRE DE LA COLUMNA Y EL VALOR DE LA FECHA,
          // SE GENERA LA RUTA HDFS CON LOS NOMBRES DE LAS COLUMNAS DE PARTICION Y LOS VALORES DE LA FECHA
          // SE AGREGA EL NOMBRE DE LA COLUMNA A UNA LISTA
          for (x <- 0 to splits - 1 by 1) {
            if (x != splits - 1 && splits > 1) {
              select_line2 = select_line2.concat("'" + partition_value.split("-")(x) + "' as " + partition_name.split(",")(x).trim() + " ,")
              path_data2 = path_data2.concat(partition_name.split(",")(x).trim() + "=" + partition_value.split("-")(x) + "/")
              columns = columns :+ partition_name.split(",")(x).trim()

            } else if (x == splits - 1 && splits > 1) {
              select_line2 = select_line2.concat("'" + partition_value.split("-")(x) + "' as " + partition_name.split(",")(x).trim() + " from ing_schema")
              path_data2 = path_data2.concat(partition_name.split(",")(x).trim() + "=" + partition_value.split("-")(x))
              columns = columns :+ partition_name.split(",")(x).trim()

            } else if (x == splits - 1 && splits == 1) {
              select_line2 = select_line2.concat("'" + partition_value + "' as " + partition_name + " from ing_schema")
              path_data2 = path_data2.concat(partition_name.split(",")(x).trim() + "=" + partition_value)
              columns = columns :+ partition_name.split(",")(x).trim()
            }

          }
          println("[INFO] ver select2  " + select_line2)
          println("[INFO] data_path:" + path_data2)
        } // EN CASO DE NO SER PARTICIONADO SE GENERA LA QUERY SOLO CON LOS CAMPOS, SIN PARTICIONES
        else {
          select_line2 = select_line
          select_line2 = select_line.substring(0, select_line.length() - 1).concat(" from ing_schema")
        }

        //VALIDA SI EXISTE EL ARCHIVO TRANSFORMATION.JSON LA INGESTA REQUIERE PRE PROCESAMIENTO
        
        if (exists_file(dir_adls + "/transformation/transformation.json")) {
          println("[INFO Transformations] START")
          
          // SE LEE EL ARCHIVO JSON EL CUAL CONTIENE LAS ESTRUCTURA Y ESQUEMA CON SUS TIPOS DE DATOS. 
          println("[INFO Transformations] SE LEE EL ARCHIVO JSON EL CUAL CONTIENE LAS ESTRUCTURA Y ESQUEMA CON SUS TIPOS DE DATOS")
          schema_data_trns = DataType.fromJson(openFile(dir_adls + "/transformation/transformation.json")).asInstanceOf[StructType]

          //CREA UN OBJETO CON TODO LO EXISTENTE EN EL DIRECTORIO TRANSFORMATION
          println("[INFO Transformations] CREA UN OBJETO CON TODO LO EXISTENTE EN EL DIRECTORIO TRANSFORMATION")
          val statSubquery = listFiles(dir_adls + "/transformation/")

          //FILTRA LOS ELEMENTOS QUE SEAN ARCHIVOS Y PREGUNTA POR LOS NOMBRES DE ARCHIVOS QUE COMIENCEN CON SUBQUERY
          println("[INFO Transformations] FILTRA LOS ELEMENTOS QUE SEAN ARCHIVOS Y PREGUNTA POR LOS NOMBRES DE ARCHIVOS QUE COMIENCEN CON SUBQUERY")
          val subQueryCount = statSubquery.filter(_.isFile).filter(_.name.startsWith("subquery"))

          //CREA UN ARREGLO VACIO DE TAMAÑO N SEGUN CANTIDAD DE ARCHIVOS SUBQUERY EXISTAN
          println("[INFO Transformations] CREA UN ARREGLO VACIO DE TAMAÑO N SEGUN CANTIDAD DE ARCHIVOS SUBQUERY EXISTAN")
          val querysResult = new Array[String](subQueryCount.size)

          // SE LEE EL ARCHIVO CSV CON LA OPCION FAILFAST Y SCHEMA. SI EL SCHEMA PROPORCIONADO NO CONCUERDA CON EL ARCHIVO EL PROCESO SE DETIENE POR REGISTROS MALFORMADOS
          println("[INFO Transformations] SE LEE EL ARCHIVO CSV CON LA OPCION FAILFAST Y SCHEMA")
          dataframe1 = spark.read.option("delimiter", delimitador).option("header", encabezado).option("mode", "FAILFAST").schema(schema_data_trns).csv(path_processing + filename)

          //CREA UNA VISTA TEMPORAL CON EL DATAFRAME
          println("[INFO Transformations] CREA UNA VISTA TEMPORAL CON EL DATAFRAME")
          println("dir_adls.split(/).last: " + dir_adls.split("/").last)
          dataframe1.createOrReplaceTempView(dir_adls.split("/").last + "_view")

          //CARGA LA QUERY CON LAS TRANSFORMACIONES
          println("[INFO Transformations] CARGA LA QUERY CON LAS TRANSFORMACIONES")
          query = openFile(dir_adls + "/transformation/transformation.hql")
          query2 = query

          //RECORRE EL ARREGLO Y APLICA LAS SUBQUERY EXISTENTES AKMACENANDO EL RESULTADO EN UNA VARIABLE
          println("[INFO Transformations] RECORRE EL ARREGLO Y APLICA LAS SUBQUERY EXISTENTES AKMACENANDO EL RESULTADO EN UNA VARIABLE")
          //CONCATENA A LA QUERY PRINCIPAL LOS VALORES DE LA SUBQUERY
          for (i <- 0 until subQueryCount.size) {
            var subq = openFile(subQueryCount(i).path.toString())
            querysResult(i) = spark.sql(subq).first.getString(0) //get(0)
            query2 = query2.replace("querysResult(" + i + ")", querysResult(i))
            //spark.sql("""set querysResult("""+i+""")='"""+querysResult(i)+"""'""")
          }

          //EJECUTA LA QUERY Y GUARDA EL RESULTADO EN UN DATAFRAME
          println("[INFO Transformations] EJECUTA LA QUERY Y GUARDA EL RESULTADO EN UN DATAFRAME")
          println("[INFO Transformations] query2 ----"+query2)
          df1 = spark.sql(query2)

          //ESCRIBE LA DATA CON LAS TRANSFORMACIONES APLICADAS A UN DIRECTORIO TEMPORAL
          println("[INFO Transformations] ESCRIBE LA DATA CON LAS TRANSFORMACIONES APLICADAS A UN DIRECTORIO TEMPORAL")
          df1.write.option("delimiter", delimitador).option("header", encabezado).option("emptyValue", "").mode(SaveMode.Overwrite).csv(path_tmp_transform)

          // SE LEE EL ARCHIVO CSV CON LA OPCION FAILFAST Y SCHEMA. SI EL SCHEMA PROPORCIONADO NO CONCUERDA CON EL ARCHIVO EL PROCESO SE DETIENE POR REGISTROS MALFORMADOS
          println("[INFO Transformations] SE LEE EL ARCHIVO CSV CON LA OPCION FAILFAST Y SCHEMA")
          df2 = spark.read.option("delimiter", delimitador).option("header", encabezado).
            option("mode", "FAILFAST").schema(schema_data).option("DateFormat", dateFormatFile).option("timestampFormat", timestampFormat).csv(path_tmp_transform + "*") 
          
        } else { //EN CASO DE NO NECESITAR TRANSFORMACIONES SE LEE EL ARCHIVO CON EL ESQUEMA PROPORCIONADO
          println("[INFO] LEYENDO PROCESSING ")
          df2 = spark.read.option("delimiter", delimitador).option("header", encabezado).
            option("mode", "FAILFAST").schema(schema_data).option("DateFormat", dateFormatFile).option("timestampFormat", timestampFormat).csv(path_processing + filename)
        }
        
        //ELIMINA DUPLICADOS
        df2 = df2.dropDuplicates
        println("[INFO] ELIMINA DUPLICADOS OK")

        // SE CUENTA LA CANTIDAD DE REGISTROS
        original_row_count = df2.count
        println("[INFO] SE CUENTA LA CANTIDAD DE REGISTROS OK")

        //ELIMINA FILAS NULAS
        df3 = df2.na.drop("all")
        println("[INFO] ELIMINA FILAS NULAS OK")

        // SE GENERA EL TIMESTAMP
        current_id = new Date().getTime

        //CREA UN FORMATO DE TIMESTAMP
        formatter_id = new SimpleDateFormat("yyyyMMddHHmmss")

        //SE CONCATENA EL TIMESTAMP MAS EL CONTADOR GENERANDO UN ID UNICO PARA LA TABLA DE CONTROL
        bigdata_ctrl_id = formatter_id.format(current_id) + "%03d".format(counter)

        // SE AGREGA LA COLUMNA BIGDATA_CLOSE_DATE Y BIGDATA_CTRL_ID AL DATAFRAME
        df3 = df3.withColumn("bigdata_close_date", to_date(lit(partition_value), formato_salida)).withColumn("bigdata_ctrl_id", lit(bigdata_ctrl_id) cast "long")
        println("[INFO] SE AGREGA LA COLUMNA BIGDATA_CLOSE_DATE Y BIGDATA_CTRL_ID AL DATAFRAME OK")

        // SE REGISTRA EL DATAFRAME COMO TABLA TEMPORAL
        df3.createOrReplaceTempView("ing_schema")

        // SE OBTIENE UN STATUS GLOBAL DE LA RUTA HDFS
        val status = listFiles(path_data)

        // VALIDA SI LA INGESTA DEBE IR PARTICIONADA
        if (subida_datos.toLowerCase().contains("partitioned by")) {

          // SE APLICA LA QUERY CREADA Y SE GUARDA EL RESULTADO EN UN DATAFRAME
          df_sal1 = spark.sql(select_line2)

          //SE ELIMINA LA RUTA HDFS Y SU PARTICION EN CASO DE EXISTIR, SI YA EXISTIA SE MARCA COMO REPROCESO.
          //EN CASO DE SER MEDIACION SE MARCA COMO NORMAL
          if (exists_file(path_data2)) process_type = "reproceso" else process_type = "normal"

          if ( cant_repartition == 0 ){
            // SE ESCRIBE EL RESULTADO SIN PARTICION.
            df_sal1.write.partitionBy(columns: _*).format("delta").option("partitionOverwriteMode", "dynamic").mode(SaveMode.Overwrite).save(path_data)
          } else if ( cant_repartition >= 1 ){
            df_sal1.repartition(cant_repartition).write.partitionBy(columns: _*).format("delta").option("partitionOverwriteMode", "dynamic").mode(SaveMode.Overwrite).save(path_data)
          } else if ( cant_repartition == -1 ){
            println("[INFO] SE ESCRIBE EL DATAFRAME A UN DIRECTORIO TEMPORAL")
            // SE ESCRIBE EL DATAFRAME A UN DIRECTORIO TEMPORAL
            df_sal1.write.mode(SaveMode.Overwrite).parquet(path_tmp)

            println("[INFO] SE CALCULA LA CANTIDAD DE ARCHIVOS A ESCRIBIR EN HDFS")
            // SE CALCULA LA CANTIDAD DE ARCHIVOS A ESCRIBIR EN HDFS.
            numPartitions = numPartitionsCalc(path_tmp)
            
            println("[INFO] SE ELIMINA LO ESCRITO EN LA RUTA TEMPORAL")
            // SE ELIMINA LO ESCRITO EN LA RUTA TEMPORAL
            delete(path_tmp)

            println("[INFO] SE ESCRIBE EL RESULTADO CON EL NUMERO DE ARCHIVOS CALCULADOS, SE PARTICIONA POR LA O LAS COLUMNAS DE PARTICION")
            // SE ESCRIBE EL RESULTADO CON EL NUMERO DE ARCHIVOS CALCULADOS, SE PARTICIONA POR LA O LAS COLUMNAS DE PARTICION.
            df_sal1.repartition(numPartitions).write.partitionBy(columns: _*).format("delta").option("partitionOverwriteMode", "dynamic").mode(SaveMode.Overwrite).save(path_data)
          }

          //SE FORMATEA EL TIMESTAMP Y SE ALMACENA EN LA VARIABLE QUE INDICA EL TIEMPO DE INSERCION DEL ARCHIVO/REGISTRO.
          insert_data_ctrl_date = formatter.format(new Date().getTime)

          println("[INFO] SE OBTIENE EL TAMAÑO Y CANTIDAD FINAL DE LOS ARCHIVOS GENERADOS")
          // SE OBTIENE EL TAMAÑO Y CANTIDAD FINAL DE LOS ARCHIVOS GENERADOS
          final_file_size = sizeFile(path_data2)
          final_number_of_files = countFiles(path_data2)
          
          println("[INFO] SE CUENTA LA CANTIDAD DE REGISTROS DEL ARCHIVO FINAL")
          // SE CUENTA LA CANTIDAD DE REGISTROS DEL ARCHIVO FINAL
          final_row_count = spark.read.format("delta").load(path_data).where(path_data2.replaceAll(path_data,"")).count
           println("[INFO] final_row_count " + final_row_count)

          // SE VALIDA SI LA CANTIDAD DE FILAS ORIGINALES VS LA CANTIDAD DE FILAS ESCRITAS ES DISTINTA.
          if (original_row_count != final_row_count) dif_row_count = 1 else dif_row_count = 0

          println("[INFO] SE GUARDA EL NOMBRE FINAL DEL ARCHIVO")
          println("[INFO] path_data2 "+path_data2)

          println("[INFO] SE MODIFICA LOS NOMBRES DE LOS ARCHIVOS PARQUET A LA NOMENCLATURA DE ARQUITECTURA")
          //SE GUARDA EL NOMBRE DE LA NOMENCLATURA DE ARQUITECTURA
          val current2 = format_2.format(new Date().getTime)
          end_file_name = dataType + "." + plataforma + "." + categoria + "." + loadType + "." + periodicity + "." + current2 + ".snappy.parquet"
          println("[INFO] final_name "+end_file_name)

          // SE ACTUALIZA LA TABLA EXTERNA PARA QUE TOME LAS PARTICIONES MODIFICADAS/NUEVAS
          //spark.sql("MSCK REPAIR TABLE " + nomb_proc + " SYNC METADATA")

          
          println("[INFO] SE AGREGAN LOS PARAMETOS DE CONTROL A LA LISTA")
          //SE AGREGAN LOS PARAMETOS DE CONTROL A LA LISTA
          controlData = controlData :+ (bigdata_ctrl_id, process_name, data_source_type, filename, original_file_date, starttime_nifi, endtime_nifi, totaltime_nifi, starttime_spark, process_type, original_file_size, final_file_size.toString, original_row_count.toString, final_row_count.toString, dif_row_count.toString, final_number_of_files.toString, end_file_name, insert_data_ctrl_date, hdfs_path, pipelineRunId)
        } else {

          // SE APLICA LA QUERY CREADA Y SE GUARDA EL RESULTADO EN UN DATAFRAME
          df_sal1 = spark.sql(select_line2)

          if ( cant_repartition == 0 ){
            // SE ESCRIBE EL RESULTADO SIN PARTICION.
            df_sal1.write.format("delta").mode(SaveMode.Overwrite).save(path_data)
          } else if ( cant_repartition >= 1 ){
            df_sal1.repartition(cant_repartition).write.format("delta").mode(SaveMode.Overwrite).save(path_data)
          } else if ( cant_repartition == -1 ){
            println("[INFO] SE ESCRIBE EL DATAFRAME A UN DIRECTORIO TEMPORAL")
            // SE ESCRIBE EL DATAFRAME A UN DIRECTORIO TEMPORAL
            df_sal1.write.mode(SaveMode.Overwrite).parquet(path_tmp)

            println("[INFO] SE CALCULA LA CANTIDAD DE ARCHIVOS A ESCRIBIR EN HDFS")
            // SE CALCULA LA CANTIDAD DE ARCHIVOS A ESCRIBIR EN HDFS.
            numPartitions = numPartitionsCalc(path_tmp)
            
            println("[INFO] SE ELIMINA LO ESCRITO EN LA RUTA TEMPORAL")
            // SE ELIMINA LO ESCRITO EN LA RUTA TEMPORAL
            delete(path_tmp)

            //AL SER TRUNCA Y CARGA EL TIPO DE PROCESO SIEMPRE SERA NORMAL
            process_type = "normal"

            println("[INFO] SE ESCRIBE EL RESULTADO CON EL NUMERO DE ARCHIVOS CALCULADOS, SE PARTICIONA POR LA O LAS COLUMNAS DE PARTICION")
            // SE ESCRIBE EL RESULTADO CON EL NUMERO DE ARCHIVOS CALCULADOS, SE PARTICIONA POR LA O LAS COLUMNAS DE PARTICION.
            df_sal1.repartition(numPartitions).write.format("delta").mode(SaveMode.Overwrite).save(path_data)
          }

          //SE FORMATEA EL TIMESTAMP Y SE ALMACENA EN LA VARIABLE QUE INDICA EL TIEMPO DE INSERCION DEL ARCHIVO/REGISTRO.
          insert_data_ctrl_date = formatter.format(new Date().getTime)

          // SE OBTIENE EL TAMAÑO Y CANTIDAD FINAL DE LOS ARCHIVOS GENERADOS
          final_file_size = sizeFile(path_data)
          final_number_of_files = countFiles(path_data)

          // SE CUENTA LA CANTIDAD DE REGISTROS DEL ARCHIVO FINAL
          final_row_count = spark.read.format("delta").load(path_data).count

          // SE VALIDA SI LA CANTIDAD DE FILAS ORIGINALES VS LA CANTIDAD DE FILAS ESCRITAS ES DISTINTA
          if (original_row_count != final_row_count) dif_row_count = 1 else dif_row_count = 0

          // SE GUARDA EL NOMBRE FINAL DEL ARCHIVO
          val current2 = format_2.format(new Date().getTime)
          end_file_name = dataType + "." + plataforma + "." + categoria + "." + loadType + "." + periodicity + "." + current2 + ".snappy.parquet" // preguntar 
          println("[INFO] final_name "+end_file_name)

          //SE AGREGAN LOS PARAMETOS DE CONTROL A LA LISTA
          controlData = controlData :+ (bigdata_ctrl_id, process_name, data_source_type, filename, original_file_date, starttime_nifi, endtime_nifi, totaltime_nifi, starttime_spark, process_type, original_file_size, final_file_size.toString, original_row_count.toString, final_row_count.toString, dif_row_count.toString, final_number_of_files.toString, end_file_name, insert_data_ctrl_date, hdfs_path, pipelineRunId)
        }
        println("""[INFO] Eliminar archivos en processing HDFS""")
        
        println("[INFO] SE ELIMINA EL ARCHIVO INGESTADO")
        //SE ELIMINA EL ARCHIVO INGESTADO
        delete(path_processing + "/" + filename)

        counter = counter + 1
      }
    }
    
  }

  salida = 0

} catch {
  case e: Exception =>
    {
      salida = 1
      println("[ERROR] " + e)
      throw e;
    }
} finally {
  val RDDsalida = spark.sparkContext.parallelize(List(salida))
  RDDsalida.coalesce(1).saveAsTextFile(path_log)
}
  

// COMMAND ----------

println("[INFO] proceso terminado")
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


println("[INFO] SE FORMATEAN LOS TIMESTAMP AL DATAFRAME DE CONTROL")
// SE FORMATEAN LOS TIMESTAMP AL DATAFRAME DE CONTROL.
control_dataframe = control_dataframe.
  withColumn("endtime_spark", lit(endtime_spark)).
  withColumn("totaltime_spark", lit(totaltime_spark)).
  withColumn("totaltime_process", lit(totaltime_process)).
  withColumn("original_file_date", from_unixtime(unix_timestamp($"original_file_date", "yyyyMMdd HHmmss"), "yyyy-MM-dd HH:mm:ss")).
  withColumn("starttime_nifi", from_unixtime(unix_timestamp($"starttime_nifi", "yyyyMMdd HHmmss"), "yyyy-MM-dd HH:mm:ss")).
  withColumn("endtime_nifi", from_unixtime(unix_timestamp($"endtime_nifi", "yyyyMMdd HHmmss"), "yyyy-MM-dd HH:mm:ss")).
  select("big_data_ctrl_id", "process_name", "data_source_type", "data_source_name", "original_file_date", "starttime_nifi", "endtime_nifi", "totaltime_nifi", "starttime_spark", "endtime_spark", "totaltime_spark", "totaltime_process", "insert_data_ctrl_date", "process_type", "original_file_size", "final_file_size", "original_row_count", "final_row_count", "dif_row_count", "final_number_of_files", "end_file_name", "hdfs_path", "pipelineRunId")
// SE ESCRIBEN LOS REGISTROS DE CONTROL EN HIVE
// control_dataframe.write.format("hive").mode(SaveMode.Append).saveAsTable("devtmp.interacciones.control_ingestas")
control_dataframe.write.mode(SaveMode.Append).saveAsTable(catalog_control)

// COMMAND ----------

if (salida == 1){
  throw new Exception("Error en el Try-Catch del proceso.")
}
