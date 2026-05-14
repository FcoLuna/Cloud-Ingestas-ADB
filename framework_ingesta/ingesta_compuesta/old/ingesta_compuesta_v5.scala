// Databricks notebook source
// val delimitador = dbutils.widgets.get("delimitador")
// val encabezado = dbutils.widgets.get("encabezado").toLowerCase()
// val dir_hdfs = "/mnt/flightdata-TestKeyVault" + dbutils.widgets.get("dir_hdfs")
// val formato_entrada = dbutils.widgets.get("formato_entrada")
// val dataType = dbutils.widgets.get("dataType")
// val plataforma = dbutils.widgets.get("plataforma")
// val categoria = dbutils.widgets.get("categoria")
// val loadType = dbutils.widgets.get("loadType")
// val periodicity = dbutils.widgets.get("periodicity")
// val starttime_nifi = dbutils.widgets.get("starttime_nifi")
// val endtime_nifi = dbutils.widgets.get("endtime_nifi")
// val totaltime_nifi = dbutils.widgets.get("totaltime_nifi")
// val original_file_size = dbutils.widgets.get("original_file_size")
// val original_file_date = dbutils.widgets.get("original_file_date")
// val formato_salida = Option(dbutils.widgets.get("formato_salida")).filterNot(_.isEmpty).getOrElse("yyyy-MM-dd")
// val dateFormatFile = Option(dbutils.widgets.get("dateFormatFile")).filterNot(_.isEmpty).getOrElse("yyyy-MM-dd")
// val timestampFormat = Option(dbutils.widgets.get("timestampFormat")).filterNot(_.isEmpty).getOrElse("yyyy-MM-dd'T'HH:mm:ss[.SSS][XXX]").replace("|",":")
// val dir_hdfs_2 = "/mnt/flightdata-TestKeyVault" + dbutils.widgets.get("dir_hdfs_2")
// val filename = dbutils.widgets.get("filename")
// val partition_date_column = dbutils.widgets.get("partition_date_column")
// val quote = scala.util.Try(dbutils.widgets.get("quote")).getOrElse("\"").replace("none","")
// val nomb_proc = dbutils.widgets.get("nomb_proc")
// val nomb_schema = nomb_proc.split('.')(0)+"."+nomb_proc.split('.')(1)
// timestampFormat = timestampFormat.replace("|",":")
// quote = quote.replace("none","")


// COMMAND ----------

val delimitador = ";"
val encabezado = "true"
val dir_hdfs = "abfss://contenedor-test@stbigdatadev02.dfs.core.windows.net/data/gss_feedback_callout"
val dir_hdfs_2 = "abfss://contenedor-test@stbigdatadev02.dfs.core.windows.net/data/gss_feedback_callout/year=2023/month=12"
val formato_entrada = "yyyyMMdd"
val dataType = "raw"
val plataforma = "campanas"
val categoria = "trafico"
val loadType = "i"
val periodicity ="d" 
val starttime_nifi = "20231221 123259"
val endtime_nifi = "20231221 123508"
val totaltime_nifi = "1000"
val original_file_size = "139514785"
val original_file_date = "2023-11-11T12:06:02Z"
val formato_salida = "yyyy-MM-dd"
val dateFormatFile ="d/MM/yyyy"
val timestampFormat ="yyyy-MM-dd HH:mm:ss"
val filename = "Feedback_GSS_20231216.csv"
val partition_date_column = "Stats_Date"
val quote = "\""
val nomb_proc = "gss_feedback_callout"
val nomb_schema = "devtmp.raw_gss_feedback_callout"


// COMMAND ----------


import java.text.SimpleDateFormat
import java.time.LocalDateTime
import java.util.Date
import java.time.format.DateTimeFormatter
import java.util.concurrent.TimeUnit
// import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.functions._
import spark.implicits._
import org.apache.spark.sql.types.DataType

// COMMAND ----------

// MAGIC %run ../funciones/funciones_ingesta_compuesta_v3

// COMMAND ----------

// try {
//   println(nomb_proc)
//   spark.sql("drop TABLE if exists "+nomb_proc+" ")
//   spark.sql("drop SCHEMA if exists "+nomb_schema+" cascade")
//   spark.sql(s"""CREATE SCHEMA IF NOT EXISTS $nomb_schema MANAGED LOCATION '$dir_hdfs' COMMENT 'schema para datos de gss_feedback_callout'""")
// } catch {
//   case e: Exception =>
//     {
//       println("[ERROR] " + e)
//       throw e;
//     }
// }

// COMMAND ----------

val format_actual = new java.text.SimpleDateFormat("yyyyMMdd HHmmss")
val format_max = new java.text.SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")
val format_timestamp = new java.text.SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'")  // 2023-11-30T17:08:11Z

val datetime_fecha_archivo_procesando= format_timestamp.parse(original_file_date) // fecha actual en formato date
val string_fecha_archivo_procesando = format_max.format(datetime_fecha_archivo_procesando) // fecha actual en formato string

// si no existe el archivo con la fecha maxima, lo crea con la fecha actual (usado por los siguientes archivos al ser procesados)
if(!(exists_file(dir_hdfs + "/last_ingest_time.txt"))){ 
    println("no existe archivo con fecha maxima  [CREANDO ARCHIVO]");
    println("crear archivo")
    makeTxtFile(dir_hdfs + "/last_ingest_time.txt", "timestamp;\n"+string_fecha_archivo_procesando+";")
}else{ // si existe el archivo, lo lee y compara las fechas
  
  // fecha del archivo almacenado de la ultima ejecución de proceso
  val fecha_ultima_ejecucion = readTxtFile(dir_hdfs + "/last_ingest_time.txt").split("\n")(1).dropRight(1) 

  // fecha del archivo a procesar que esta en la carpeta landing
  val datetime_fecha_ultima_ejecucion = format_max.parse(fecha_ultima_ejecucion) // fecha maxima en formato string
  
  var result: Int = 0;
  
  result = datetime_fecha_archivo_procesando.compareTo(datetime_fecha_ultima_ejecucion);
  
  // si el valor es mayor a 0, significa que el archivo de last_ingest_time 
  if(result > 0){
    println("fecha del archivo a procesar es mayor que la fecha de ultima ejecución [ACTUALIZAR FECHA DEL ARCHIVO]");
    delete(dir_hdfs + "/last_ingest_time.txt")
    makeTxtFile(dir_hdfs + "/last_ingest_time.txt", "timestamp;\n"+string_fecha_archivo_procesando+";")
  }else{
    println("fecha del archivo de proceso menor o igual que la fecha de la ultima ejecución [NO HACER NADA]");
  }
}

// COMMAND ----------

val current = new Date().getTime
val formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
val formatter2 = new SimpleDateFormat("yyyy-MM-dd")
val starttime_spark = formatter.format(current)
val insert_date = formatter2.format(current)
println("[INFO] Operaciones_ETL_Batch")

// COMMAND ----------

val path_processing = dir_hdfs + "/processing/"
val path_landing = dir_hdfs + "/landing/"
val path_processing_error = dir_hdfs + "/processing_error/"
val path_log = dir_hdfs + "/log/"
val path_data = dir_hdfs + "/" + dataType + "/"
val path_tmp = dir_hdfs + "/tmp/"
val path_tmp_transform = dir_hdfs + "/tmp_transform/"

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
val nombre_archivo = dir_hdfs.split("/").last

// SE LEE EL ARCHIVO HQL EL CUAL CONTIENE LAS INSTRUCCIONES ddl DE CREATE TABLE PARA HIVE.
// (ESTE DEBE TENER EL MISMO NOMBRE QUE LA VARIABLE OBTENIDA EN EL PASO ANTERIOR EJ: detalle_numeros_portados.hql
val subida_datos = openFile(dir_hdfs + "/" + nombre_archivo + ".hql") 

println("[INFO] subida_datos " + subida_datos)


// SE LEE EL ARCHIVO JSON EL CUAL CONTIENE LAS ESTRUCTURA Y ESQUEMA CON SUS TIPOS DE DATOS .
// (ESTE DEBE TENER EL MISMO NOMBRE QUE LA VARIABLE OBTENIDA EN EL PASO ANTERIOR EJ: detalle_numeros_portados.json
val schema_data = DataType.fromJson(openFile(dir_hdfs + "/" + nombre_archivo + ".json")).asInstanceOf[StructType]
println("[INFO] schema_data " + schema_data)

//SE DEFINE UN ESQUEMA NULL PARA LAS TRANSFORMACIONES
var schema_data_trns = null.asInstanceOf[StructType]

//SE GENERAN VARIABLES STRING VACIAS PARA ALMACENAR LAS QUERYS PARA LAS TRANSFORMACIONES
var query = ""
var query2 = ""

// COMMAND ----------

/*
     * DESDE EL HQL LEIDO SE REALIZA UN SUBSTRING PARA OBTENER EL NOMBRE DEL PROCESO DESDE LA PRIMERA LINEA DEL ARCHIVO
     ej: CREATE EXTERNAL TABLE IF NOT EXISTS capa_semantica.detalle_numeros_portados tendria el valor de capa_semantica.detalle_numeros_portados
     */

     // CAMBIO: nomb_proc como parametro del pipeline
    // val nomb_proc = subida_datos.substring(0, subida_datos.indexOf('\n')).substring(subida_datos.toLowerCase().indexOf("create external table if not exists ") + 36).trim()
    
    //SE CONCATENA SPARK AL NOMBRE DEL PROCESO
    val process_name = "spark_" + nomb_proc
    
    // SE DEFINE QUE EL TIPO DE FUENTE SERA UN ARCHIVO
    val data_source_type = "file"
    val hdfs_path = path_data
    println(path_data)
    println("[INFO] nombre proceso " + nomb_proc)

    var salida = 1
    var bigdata_close_date = ""
    var partition_value = ""
    println("[INFO] Creación de tabla si no existe")
    
    //SE DEFINEN DATAFRAMES VACIOS Y SE EJECUTA EL HQL EN HIVE CREANDO LA TABLA EXTERNA
    var df1, df2, df3,df_join ,df4, df5, df_sal2, df_sal3, df_sal4 = spark.sql(subida_datos)
    var controlData = Seq.empty[(String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String)]


// COMMAND ----------

/*
  * DESDE EL HQL LEIDO SE REALIZA UN SUBSTRING PARA OBTENER EL NOMBRE DEL PROCESO DESDE LA PRIMERA LINEA DEL ARCHIVO
  ej: CREATE EXTERNAL TABLE IF NOT EXISTS capa_semantica.detalle_numeros_portados tendria el valor de capa_semantica.detalle_numeros_portados
  */
val nomb_proc = subida_datos.substring(0, subida_datos.indexOf('\n')).substring(subida_datos.toLowerCase().indexOf("create external table if not exists ") + 36).trim()

//SE CONCATENA SPARK AL NOMBRE DEL PROCESO
val process_name = "spark_" + nomb_proc

// SE DEFINE QUE EL TIPO DE FUENTE SERA UN ARCHIVO
val data_source_type = "file"
val hdfs_path = path_data
println("[INFO] nombre proceso " + nomb_proc)

// COMMAND ----------

try{
       // SE ELIMINA EL DIRECTORIO DE LOG EN CASO DE EXISTIR
      println("""[INFO] Borrando directorio logs""")
      delete(path_log)
      

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
      val status = listFiles(dir_hdfs)
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
      if (exists_file(dir_hdfs + "/transformation/transformation.json")) {

        println("Entro al if")

        /* SE LEE EL ARCHIVO JSON EL CUAL CONTIENE LAS ESTRUCTURA Y ESQUEMA CON SUS TIPOS DE DATOS. */
        schema_data_trns = DataType.fromJson(openFile(dir_hdfs + "/transformation/transformation.json")).asInstanceOf[StructType]
        
        //CREA UN OBJETO CON TODO LO EXISTENTE EN EL DIRECTORIO TRANSFORMATION
        val statSubquery = listFiles(dir_hdfs + "/transformation/")
        
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
        csv(dir_hdfs_2 + "/*")
        
        //GUARDA LAS COLUMNAS DEL DATAFRAME
        val groupCols = df1.columns
        
        // AGRUPA LA DATA CARGADA POR EL NOMBRE DEL ARCHIVO BUSCANDO PARA CADA UNO EL MAXIMO TIMESTAMP
        //CON ESTO SE ASEGURA QUE EN EL REPROCESO SIEMPRE SE TOME EL ARCHIVO REPROCESADO MAS ACTUAL
        val df_ts = df1.groupBy("filename_spark").agg(max("ts").as("ts"))

        //SE HACE UN JOIN PARA DEJAR SOLO LOS DATASET QUE TENGAN EL TIMESTAMP MAS ACTUAL POR ARCHIVO
        df_join=df1.join(df_ts,Seq("filename_spark","ts"))
        
        //SE SELECCIONAN LAS COLUMNAS Y SE ELIMINA LA COLUMNA TS
        df_join = df_join.select(groupCols.map(col): _*).drop("ts")
        
        //SE REGISTRA COMO VISTA TEMPORAL
        df_join.createOrReplaceTempView(dir_hdfs.split("/").last)
        
        //CARGA LA QUERY CON LAS TRANSFORMACIONES
        query = openFile(dir_hdfs + "/transformation/transformation.hql")
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
        println("display df1")
        display(df1)
        //GUARDA LAS COLUMNAS DEL DATAFRAME
        val groupCols = df1.columns
        // groupCols.foreach(println)

        // AGRUPA LA DATA CARGADA POR EL NOMBRE DEL ARCHIVO BUSCANDO PARA CADA UNO EL MAXIMO TIMESTAMP
        //CON ESTO SE ASEGURA QUE EN EL REPROCESO SIEMPRE SE TOME EL ARCHIVO REPROCESADO MAS ACTUAL
        val df_ts = df1.groupBy("filename_spark").agg(max("ts").as("ts"))

        //SE HACE UN JOIN PARA DEJAR SOLO LOS DATASET QUE TENGAN EL TIMESTAMP MAS ACTUAL POR ARCHIVO
        df_join=df1.join(df_ts,Seq("filename_spark", "ts"))

        //SE SELECCIONAN LAS COLUMNAS Y SE ELIMINA LA COLUMNA TS
        df3 = df_join.select(groupCols.map(col): _*).drop("ts")
      }

      //SE ELIMINAN DUPLICADOS
      df3 = df3.dropDuplicates
      
      //SE CUENTA LA CANTIDAD DE REGISTROS
      original_row_count = df3.count
      
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

      //SI LA COLUMNA DE PARTICION ES VACIA SE UTILIZA EL UDF Y SE GENERA LA COLUMNA BIGDATA_CLOSE_DATE DESDE LA COLUMNA FILENAME DEL DATAFRAME LA CUAL TIENE LOS FILENAMES DE LOS ARCHIVOS 
      //EN CASO DE TENER COLUMNA DE PARTICION SE FORMATEA LA COLUMNA DESDE EL FORMATO DE ENTRADA AL FORMATO DE SALIDA ESPECIFICADO
      // val partition_date_column = ""
      if (partition_date_column == "") {
        df4 = df4.withColumn("bigdata_close_date", date_format(to_date(regexp_replace(element_at(split(col("filename_spark"),"_"), -1), "[^0-9]", ""), formato_entrada), formato_salida) cast "date")
      } else {
        df4 = df4.withColumn("bigdata_close_date", from_unixtime(unix_timestamp(col(partition_date_column), formato_entrada), formato_salida) cast "date")
      }

      //GENERA UN ARREGLO CON LAS DISTINTAS FECHAS EXISTENTES EN EL DATAFRAME
      val dates = df4.select("bigdata_close_date").distinct().as[String].collect()

      //SE OBTIENE EL NOMBRE DE LA COLUMNA O COLUMNAS DE PARTICION DESDE EL HQL. ej: (partition_date,year,month,day)
      partition_name = subida_datos.substring(subida_datos.toLowerCase().indexOf("partitioned by") + 15, subida_datos.toLowerCase().indexOf("partitioned by") + 15 + subida_datos.substring(subida_datos.toLowerCase().indexOf("partitioned by") + 15).indexOf(")")).replace("(", "").replace("string", "").trim()
      // println("partition_name " + partition_name)
      
      //SE SEPARA LA COLUMNA DE PARTICION POR "," EN CASO QUE SEA MAS DE UNA COLUMNA
      var splits = partition_name.split(",").size
      
      //SE CREAN 2 LISTAS STRING, UNA PARA ALMACENAR LA LISTA DE RUTAS HDFS Y OTRA PARA LA LISTA DE COLUMNAS DE PARTICION
      var path_data2_seq = Seq.empty[String]
      
      /* SE RECORRE EL ARREGLO CON LAS FECHAS, SE CONSTRUYEN LAS DISTINTAS RUTAS HDFS PARTICIONADAS AGREGANDO EL NOMBRE DE LA COLUMNA Y SU VALOR DONDE SE CARGARA LA DATA.
      SE AGREGA A LA LISTA LOS NOMBRES DE LAS COLUMNAS DE PARTICION
      */
      for (i <- 0 until dates.length) {
        path_data2 = path_data

        for (x <- 0 to splits - 1 by 1) {

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

      }
      
      //SE ELIMINAN COLUMNAS Y RUTAS DE PARTICION DUPLICADAS
      path_data2_seq = path_data2_seq.distinct
      // path_data2_seq.foreach(println)
      
      columns = columns.distinct
      // columns.foreach(println)

      //SE GENERA UN SELECT DE LAS COLUMNAS DEL DATAFRAME RECORRIENDO SUS COLUMNAS Y SUMANDO LAS COLUMNAS DE PARTICION MAS SUS VALORES
      val selectExprs = df4.columns.map(col) ++ (0 until columns.size map (i => $"tmp".getItem(i).as(columns(i))))

      //SE APLICA EL SELECT CREADO AL DATAFRAME
      df5 = df4.withColumn("tmp", split($"bigdata_close_date", "-")).select(selectExprs: _*).drop("filename_spark")
      
      // SE ESCRIBE EL DATAFRAME A UN DIRECTORIO TEMPORAL
      df5.write.mode(SaveMode.Append).parquet(path_tmp)

      // SE CALCULA LA CANTIDAD DE ARCHIVOS A ESCRIBIR EN DATA LAKE.
      numPartitions = numPartitionsCalc(path_tmp)

      // SE ELIMINA LO ESCRITO EN LA RUTA TEMPORAL
      delete(path_tmp)

      //SE ELIMINAN TODOS LOS DIRECTORIOS PARTICIONADOS QUE SE CARGARAN EN CASO QUE EXISTAN
      deletePartitions(path_data2_seq)

      // SE ESCRIBE EL RESULTADO CON EL NUMERO DE ARCHIVOS CALCULADOS, SE PARTICIONA POR LA O LAS COLUMNAS DE PARTICION.
      df5.repartition(numPartitions).write.partitionBy(columns: _*).mode(SaveMode.Append).parquet(path_data)
      println("display df5")
      display(df5)

      // SE ELIMINA LO ESCRITO EN LA RUTA TEMPORAL DE TRANSFORMACIONES
      delete(path_tmp_transform)

      //SE RECORRE LA LISTA CON LOS DIRECTORIOS
      for (i <- 0 until path_data2_seq.length) {
        
        // SE CALCULA EL TIMESTAMP
        current_ins = new Date().getTime
        
        // SE CREA UN FORMATO DE FECHA
        formatter_ins = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
        
        //SE FORMATEA EL TIMESTAMP Y SE ALMACENA EN LA VARIABLE QUE INDICA EL TIEMPO DE INSERCION DEL ARCHIVO/REGISTRO.
        insert_data_ctrl_date = formatter_ins.format(current_ins)
        
        //SE MODIFICA LOS NOMBRES DE LOS ARCHIVOS PARQUET A LA NOMENCLATURA DE ARQUITECTURA
        parquetPutName(status, dataType, plataforma, categoria, loadType, periodicity)
        
        // SE OBTIENE EL TAMAÑO FINAL DE LOS ARCHIVOS ESCRITOS
        final_file_size = sizeFile(path_data2_seq(i))
        
        // SE CUENTA LA CANTIDAD DE REGISTROS DEL ARCHIVO FINAL
        final_row_count = final_row_count + spark.read.parquet(path_data2_seq(i)).count
        
        // SE VALIDA SI LA CANTIDAD DE FILAS ORIGINALES VS LA CANTIDAD DE FILAS ESCRITAS ES DISTINTA.
        if (original_row_count != final_row_count) dif_row_count = 1 else dif_row_count = 0
        
        // SE CUENTA LA CANTIDAD DE ARCHIVOS GENERADOS
        final_number_of_files = countFiles(path_data2_seq(i))
        
        // SE GUARDA EL NOMBRE FINAL DEL ARCHIVO
        final_name = findFile(listFiles(path_data2_seq(i)), dataType)
        end_file_name = final_name.substring(final_name.indexOf("/"+dataType+".") + 1, final_name.indexOf(".N")) + ".snappy.parquet"
        
        // SE ACTUALIZA LA TABLA EXTERNA PARA QUE TOME LAS PARTICIONES MODIFICADAS/NUEVAS
        // spark.sql("MSCK REPAIR TABLE " + nomb_proc)
        
        //SE AGREGAN LOS PARAMETOS DE CONTROL A LA LISTA
        controlData = controlData :+ (bigdata_ctrl_id, process_name, data_source_type, filename, original_file_date, starttime_nifi, endtime_nifi, totaltime_nifi, starttime_spark, process_type, original_file_size, final_file_size.toString, original_row_count.toString, final_row_count.toString, dif_row_count.toString, final_number_of_files.toString, end_file_name, insert_data_ctrl_date, hdfs_path)
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
      // Validar porque no funcionan estos comandos
      //  val RDDsalida = spark.sparkContext.parallelize(List(salida))
      //  RDDsalida.coalesce(1).saveAsTextFile(path_log)
    }
    println("[INFO] proceso terminado")

// COMMAND ----------

//SE CREA UN DATAFRAME CON TODOS LOS REGISTROS PARA LA TABLA DE CONTROL
var control_dataframe = controlData.toDF("big_data_ctrl_id", "process_name", "data_source_type", "data_source_name", "original_file_date", "starttime_nifi", "endtime_nifi", "totaltime_nifi", "starttime_spark", "process_type", "original_file_size", "final_file_size", "original_row_count", "final_row_count", "dif_row_count", "final_number_of_files", "end_file_name", "insert_data_ctrl_date", "hdfs_path")

// SE OBTIENE EL TIMESTAMP
val end = new Date().getTime

//SE FORMATEA EL TIMESTAMP 
val endtime_spark = formatter.format(end)
//SE CALCULA LA DURACION DEL PROCESO SPARK
val totaltime_spark = (end - current).toFloat / 1000

//SE CALCULA LA DURACION DEL PROCESO SPARK + NIFI
val totaltime_process = totaltime_spark + totaltime_nifi.toInt

// COMMAND ----------

// SE FORMATEAN LOS TIMESTAMP AL DATAFRAME DE CONTROL.
control_dataframe = control_dataframe.
  withColumn("endtime_spark", lit(endtime_spark)).
  withColumn("totaltime_spark", lit(totaltime_spark)).
  withColumn("totaltime_process", lit(totaltime_process)).
  withColumn("original_file_date", from_unixtime(unix_timestamp($"original_file_date", "yyyyMMdd HHmmss"), "yyyy-MM-dd HH:mm:ss")).
  withColumn("starttime_nifi", from_unixtime(unix_timestamp($"starttime_nifi", "yyyyMMdd HHmmss"), "yyyy-MM-dd HH:mm:ss")).
  withColumn("endtime_nifi", from_unixtime(unix_timestamp($"endtime_nifi", "yyyyMMdd HHmmss"), "yyyy-MM-dd HH:mm:ss")).
  select("big_data_ctrl_id", "process_name", "data_source_type", "data_source_name", "original_file_date", "starttime_nifi", "endtime_nifi", "totaltime_nifi", "starttime_spark", "endtime_spark", "totaltime_spark", "totaltime_process", "insert_data_ctrl_date", "process_type", "original_file_size", "final_file_size", "original_row_count", "final_row_count", "dif_row_count", "final_number_of_files", "end_file_name", "hdfs_path")

  display(control_dataframe)

// COMMAND ----------

// SE ESCRIBEN LOS REGISTROS DE CONTROL EN HIVE
// problema de permisos al no ser nosotros quienes crearon la tabla sino a través del usuario de miguel por datafactory
// control_dataframe.write.mode(SaveMode.Append).saveAsTable("raw_gss_feedback_callout.gss_feedback_callout")
