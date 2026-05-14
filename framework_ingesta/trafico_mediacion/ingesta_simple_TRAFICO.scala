// Databricks notebook source
// MAGIC %md
// MAGIC ### Importar librerías y definición de variables

// COMMAND ----------

spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")

// COMMAND ----------

import java.util.concurrent.TimeUnit
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types._

import java.util.{Calendar, TimeZone, Date}
import org.apache.spark.sql.types.DateType
import java.time.format.DateTimeFormatter
import java.time.LocalDateTime
import java.text.SimpleDateFormat

// COMMAND ----------

// MAGIC %run ../funciones/funciones_genericas

// COMMAND ----------

val format_actual = new SimpleDateFormat("yyyyMMdd HHmmss")
val format_max = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")
val format_timestamp = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'")  // 2023-11-30T17:08:11Z
val format_2 = new SimpleDateFormat("yyyyMMddHHmmss")
format_actual.setTimeZone(TimeZone.getTimeZone("Chile/Continental"))
format_max.setTimeZone(TimeZone.getTimeZone("Chile/Continental"))
format_timestamp.setTimeZone(TimeZone.getTimeZone("Chile/Continental"))
format_2.setTimeZone(TimeZone.getTimeZone("Chile/Continental"))


// COMMAND ----------

// MAGIC %md
// MAGIC ### Variables NOTEBOOK

// COMMAND ----------

var status_ejecucion = 0
var desc_status_ejecucion = "[OK]"
var status_ejecucion_str = "OK"
var status_error: Exception = null

// COMMAND ----------

// PARAMETROS
var delimitador, encabezado, dir_adls, formato_entrada, dataType, plataforma, categoria, loadType, periodicity, starttime_nifi, endtime_nifi, original_file_size, original_file_date, dateFormatFile, timestampFormat, formato_salida, nomb_proc, pipelineRunId = ""
var catalog_control = "bidesarrollo.control.control_ingestas"
var process_name, nombre_archivo = ""
val data_source_type = "file"
var cant_repartition = 0
var totaltime_nifi:String = ""
// parametros paths
var path_processing, path_landing, path_processing_error, path_log, path_data, path_tmp, path_tmp_transform, query_transformacion = ""



// VARIABLES
var create_table_query_hql: String = ""

var schema_data = null.asInstanceOf[StructType] //SE DEFINE UN ESQUEMA NULL 
var schema_data_trns = null.asInstanceOf[StructType] //SE DEFINE UN ESQUEMA NULL PARA LAS TRANSFORMACIONES
//SE GENERAN VARIABLES STRING VACIAS PARA ALMACENAR LAS QUERYS PARA LAS TRANSFORMACIONES
var query: String = ""
var query2: String = ""

var salida = 1
var bigdata_close_date = ""
var partition_value = ""
var controlData = Seq.empty[(String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String)]

var dataframe1, df1, df2, df3, df4, df5, df_sal1, df_sal2, df_good,df_bad, emptyDF : DataFrame = null

var list : Seq[com.databricks.backend.daemon.dbutils.FileInfo] = null
var filename = ""
var variable = ""
var select_line = "select "
var select_line2 = ""
var path_data2 = ""
var partition_format = ""
var partition_name = ""
var partition_date_column = ""

// COMMAND ----------

// MAGIC %md
// MAGIC ### PARAMETROS NOTEBOOK (Data factory)

// COMMAND ----------

def getVariableType[T](v: T) = v match {
  case _: Int    => "Int"
  case _: String => "String"
  case _         => "Unknown"
}


// ELIMINAR
/* starttime_nifi = "20240523 160152"
endtime_nifi = "20240523 160708"
original_file_size = "50567324"
original_file_date = "2024-05-17T13:03:41Z"
 */



if(status_ejecucion == 0){
    try {

        //delimitador por el cual se separaran las columnas del archivo (,;|~,etc)
        delimitador = dbutils.widgets.get("delimitador")
        //INDICA SI EL ARCHIVO TRAE O NO CABEZERA (true|false)
        encabezado = dbutils.widgets.get("encabezado").toLowerCase()
        //DIRECTORIO HDFS DESDE DONDE SE EJECUTARA LA LECTURA Y CARGA DE ARCHIVOS.
        dir_adls = dbutils.widgets.get("dir_adls")
        //FORMATO DE FECHA QUE VIENE EN EL NOMBRE DEL ARCHIVO DE ENTRADA (yyyy-MM-dd,yyyyMMdd,ddMMyyyy,etc)
        formato_entrada = dbutils.widgets.get("formato_entrada")
        //INDICA SI LA DATA ES DE TIPO RAW,NORAW,CONFORMADO.
        dataType = dbutils.widgets.get("dataType")
        //INDICA DESDE DONDE VIENE LA INFORMACION A CARGAR (ATIS,SAP,ETC)
        plataforma = dbutils.widgets.get("plataforma")
        //INDICA A QUE CATEGORIA DE LOS DOMINIOS DE INFORMACION CORRESPONDEN(INTERACCIONES,CAMPAÑAS,GESTION_RECURSOS,ETC)
        categoria = dbutils.widgets.get("categoria")
        //INDICA EL TIPO DE CARGA A EJECUTAR INCREMENTAL,FULL,etc (i,d)
        loadType = dbutils.widgets.get("loadType")
        //INDICA LA PERIODICIDAD DE LA INGESTA, diaria, semanal, mensual (d,s,m).
        periodicity = dbutils.widgets.get("periodicity")
        //INDICA EL TIMESTAMP DEL MOMENTO EN QUE NIFI INICIO EL FLUJO.
        starttime_nifi =  new SimpleDateFormat("yyyyMMdd HHmmss").format(new Date())
        //INDICA EL TIMESTAMP DEL MOMENTO EN QUE NIFI TERMINO EL FLUJO.
        endtime_nifi = new SimpleDateFormat("yyyyMMdd HHmmss").format(new Date())
        //EL TAMAÑO ORIGINAL DEL ARCHIVO EN BYTES EN EL REPOSITORIO DE ORIGEN
        original_file_size = dbutils.widgets.get("original_file_size")
        //EL TIMESTAMP ORIGINAL EN LA QUE EL ARCHIVO FUE CREADO/MOVIDO EN EL REPOSITORIO DE ORIGEN.
        original_file_date = new SimpleDateFormat("yyyyMMdd HHmmss").format(new Date())
        //COLUMNA DE PARTICION
        partition_date_column = dbutils.widgets.get("columna_particion")
        //FORMATO QUE TRAEN LOS CAMPOS TIPO FECHA EN EL ARCHIVO. POR DEFECTO yyyy-MM-dd
        dateFormatFile = Option(dbutils.widgets.get("dateFormatFile")).filterNot(_.isEmpty).getOrElse("yyyy-MM-dd")
        //FORMATO DE TIMESTAMP QUE TENDRAN LAS COLUMNAS TIPO TIMESTAMP EN EL ARCHIVO (POR DEFECTO yyyy-MM-dd'T'HH:mm:ss[.SSS][XXX])
        timestampFormat = Option(dbutils.widgets.get("timestampFormat")).filterNot(_.isEmpty).getOrElse("yyyy-MM-dd'T'HH:mm:ss[.SSS][XXX]").replace("|", ":")
        //FORMATO DE SALIDA PARA EL CAMPO BIGDATA CLOSE DATE. POR DEFECTO yyyy-MM-dd
        formato_salida = Option(dbutils.widgets.get("formato_salida")).filterNot(_.isEmpty).getOrElse("yyyy-MM-dd")
        
        //cant_repartition = Option(dbutils.widgets.get("cant_repartition")).filterNot(_.isEmpty).getOrElse("-1").toInt
      

        nomb_proc = dbutils.widgets.get("nomb_proc")

        // Pipeline Run ID del flujo en Data Factory
        pipelineRunId = dbutils.widgets.get("pipelineRunId")

        // Ctalogo, esquema y tabla de control
        catalog_control = dbutils.widgets.get("catalog_control")

        //SE CONCATENA SPARK AL NOMBRE DEL PROCESO
        process_name = "spark_" + nomb_proc

        // DESDE EL DIRECTORIO HDFS SE OBTIENE EL ULTIMO ELEMENTO HACIENDO SPLIT POR "/" CON EL FIN DE OBTENER EL NOMBRE DE LA ENTIDAD
        // ej: /data/interacciones/ordenes/oap/detalle_numeros_portados EL VALOR OBTENIDO SERIA detalle_numeros_portados
        nombre_archivo = dir_adls.split("/").last

        // Cálculo de Totaltime nifi/ADF
        //INDICA EL TOTAL DE TIEMPO EN SEGUNDOS QUE DEMORO LA EJECUCION DE NIFI.
        val starttime_nifi_dateFormat = format_actual.parse(starttime_nifi)
        val endtime_nifi_dateFormat = format_actual.parse(endtime_nifi)
        totaltime_nifi = ((endtime_nifi_dateFormat.getTime() - starttime_nifi_dateFormat.getTime())/1000).toString()


    } catch {
        case e: Exception =>
          status_ejecucion = 1
          desc_status_ejecucion = "[ERROR] " + e
          status_ejecucion_str = "ERROR"
          status_error = e
          println("[ERROR] " + e)
    } 
} else {
  println("Skipped")
} 


// COMMAND ----------

println("original_file_size "+original_file_size)
println("original_file_date "+original_file_date)
println("--")
println("dateFormatFile "+dateFormatFile)
println("timestampFormat "+timestampFormat)
println("formato_salida "+formato_salida)

println("starttime_nifi "+starttime_nifi)
println("endtime_nifi "+endtime_nifi)
println("totaltime_nifi "+totaltime_nifi)

// COMMAND ----------

// MAGIC %md
// MAGIC ## Fecha Inicio spark

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
// MAGIC ## Script ingesta simple

// COMMAND ----------

// MAGIC %md
// MAGIC ##### SE DEFINEN LOS DIRECTORIOS DE TRABAJO PARA EL PROCESO

// COMMAND ----------



if(status_ejecucion == 0){
  try {
      path_processing = dir_adls + "/processing/"
      path_landing = dir_adls + "/landing/"
      path_processing_error = dir_adls + "/processing_error/"
      path_log = dir_adls + "/log/"
      path_data = dir_adls + "/" + dataType + "/"
      path_tmp = dir_adls + "/tmp/"
      path_tmp_transform = dir_adls + "/tmp_transform/"
  } catch {
      case e: Exception =>
        status_ejecucion = 1
        desc_status_ejecucion = "[ERROR] " + e
        status_ejecucion_str = "ERROR"
        status_error = e
        println("[ERROR] " + e)
  } 
} else {
  println("Skipped")
} 

// COMMAND ----------

println("[INFO] path_processing " + path_processing)
println("[INFO] path_log " + path_log)
println("[INFO] path_landing " + path_landing)
println("[INFO] path_processing_error " + path_processing_error)
println("[INFO] delimitador " + delimitador)
println("[INFO] encabezado " + encabezado)
println("[INFO] formato_entrada " + formato_entrada)

// COMMAND ----------




if(status_ejecucion == 0){
  try {
          
      // SE LEE EL ARCHIVO HQL EL CUAL CONTIENE LAS INSTRUCCIONES ddl DE CREATE TABLE PARA HIVE.
      // (ESTE DEBE TENER EL MISMO NOMBRE QUE LA VARIABLE OBTENIDA EN EL PASO ANTERIOR EJ: detalle_numeros_portados.hql
      create_table_query_hql = openFile(dir_adls + "/" + nombre_archivo + ".hql") 

      println("[INFO] create_table_query_hql " + create_table_query_hql)

      // REPLACE LOCATION MANAGED
      create_table_query_hql = create_table_query_hql.replace("LOCATIONMANAGEDCONTAINERHQL", path_data)
      create_table_query_hql = create_table_query_hql.replace("NOMBREPROCUNITYCATALOGDATABRICKS", nomb_proc)

      println("[INFO] create_table_query_hql " + create_table_query_hql)

      // SE LEE EL ARCHIVO JSON EL CUAL CONTIENE LAS ESTRUCTURA Y ESQUEMA CON SUS TIPOS DE DATOS .
      // (ESTE DEBE TENER EL MISMO NOMBRE QUE LA VARIABLE OBTENIDA EN EL PASO ANTERIOR EJ: detalle_numeros_portados.json
      schema_data = DataType.fromJson(openFile(dir_adls + "/" + nombre_archivo + ".json")).asInstanceOf[StructType]
      println("[INFO] schema_data " + schema_data)


      //SE DEFINEN DATAFRAMES VACIOS Y SE EJECUTA EL HQL EN HIVE CREANDO LA TABLA EXTERNA
      dataframe1 = spark.sql(create_table_query_hql)
      df1 = dataframe1
      df2 = dataframe1
      df3 = dataframe1
      df4 = dataframe1
      df5 = dataframe1
      df_sal1 = dataframe1
      df_sal2 = dataframe1
      emptyDF = dataframe1
      //
  } catch {
      case e: Exception =>
        status_ejecucion = 1
        desc_status_ejecucion = "[ERROR] " + e
        status_ejecucion_str = "ERROR"
        status_error = e
        println("[ERROR] " + e)
  } 
} else {
  println("Skipped")
} 



// COMMAND ----------

// MAGIC %md
// MAGIC #### Try Catch

// COMMAND ----------


if(status_ejecucion == 0){
  try {
      
      // SE ELIMINA EL DIRECTORIO DE LOG EN CASO DE EXISTIR
      delete(path_log)
      println("""[INFO] Mover archivos HDFS""")

      //SE LISTA TODOS LOS ARCHIVOS EXISTENTES EN EL DIRECTORIO PROCESSING
      list = listFiles(path_processing)

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
      val arreglo_string = create_table_query_hql.substring(create_table_query_hql.indexOf("(") + 2, create_table_query_hql.indexOf(")\n")).split("\n")
      


      /* SE RECORRE LA LISTA CON LOS CAMPOS ,A LA VARIABLE SELECT_LINE SE LE CONCATENAN TODOS LOS CAMPOS FORMANDO UNA QUERY*/
      for (campo <- arreglo_string if campo.trim() != "") {
        variable = campo.trim().substring(0, campo.trim().indexOf(" "))
        println("[INFO] " + variable)
        select_line = select_line.concat(variable + ",")
      }
      
      // SE LISTAN LOS ARCHIVOS EN EL DIRECTORIO LANDING
      // ORDENAR LISTA DE ARCHIVOS POR FECHA
      list = listFiles(path_landing).sortBy(_.name)
      println(list)

  } catch {
      case e: Exception =>
        status_ejecucion = 1
        desc_status_ejecucion = "[ERROR] " + e
        status_ejecucion_str = "ERROR"
        status_error = e
        println("[ERROR] " + e)
  } 
} else {
  println("Skipped")
} 

// COMMAND ----------


var columns = Seq.empty[String]
var size_count = Seq.empty[Long]
var process_type = ""
var numPartitions = 0
var final_file_size = 0L
var original_row_count = 0L
var final_row_count = 0L
var dif_row_count:Int = 0
var final_number_of_files = 0L
var final_name = ""
var end_file_name = ""
var bigdata_ctrl_id = ""
var insert_data_ctrl_date = ""
var current_ins = 0L
var formatter_ins: SimpleDateFormat = null
var current_id = 0L
var formatter_id: SimpleDateFormat = null
var cantidad_df_bad = 0L

if(status_ejecucion == 0){
  try {
      
      // SE CREA UN CONTADOR
      var counter = 1
      
    if (list != null && list.length > 0) {
      for (k <- list) {
        filename = k.name

        //SE PROCESAN SOLO ARCHIVOS QUE NO COMIENZAN CON "."
        if (!(filename.startsWith("."))) {

          //PREPARAR DIRECTORIO PROCESSING
          delete(path_processing + filename)
          moverArchivoAbfs(path_landing + filename, path_processing + filename)
          println("[INFO] FILENAME landing " + filename)

        } // fin if no empieza por "."
      } // fin for archivos     
    } // fin if lista vacia

          df2 = spark.read.option("delimiter", delimitador).
          option("header", encabezado).
          option("mode", "PERMISSIVE").
          option("columnNameOfCorruptRecord", "_corrupt_record").
          schema(schema_data).
          option("DateFormat", dateFormatFile).
          option("timestampFormat", timestampFormat).
          csv(path_processing + "*") 

                
                //crea df que identifican los registros corruptos de la lectura del csv MODIFICACION 05112024
                if (df2.columns.contains("_corrupt_record")) {
                  df_good  = df2.filter(col("_corrupt_record").isNull)
                  df_bad   = df2.filter(col("_corrupt_record").isNotNull)
                  df2 = df_good.drop("_corrupt_record","columnNameOfCorruptRecord")
                  cantidad_df_bad = df_bad.count()

                  if (cantidad_df_bad > 0 ){
                    println(s"EXISTEN $cantidad_df_bad REGISTROS CORRUPTOS")
                    status_ejecucion = 1
                  }
                }

        df2.createOrReplaceTempView("df2_view")

        if (nombre_archivo == "xdr"){
              query_transformacion = """
                  SELECT CASE WHEN LENGTH(f_linea) = 8 THEN '9' || f_linea ELSE f_linea END AS sm_linea, 
                  f_abonado, 
                  (CAST(UNIX_TIMESTAMP(CONCAT(f_fecha, f_hora), 'yyMMddHHmmss') AS TIMESTAMP) + INTERVAL 0 SECONDS) AS sm_datetime,
                  cast(f_trafico as int) as f_trafico,
                  f_ruta_origen,
                  f_ruta_destino,
                  cast(f_hh as tinyint) as f_hh,
                  f_regla,
                  cast(f_dir as tinyint) as f_dir,
                  f_tipo_ori,
                  cast(f_oper_codigo as smallint) as f_oper_codigo,
                  f_cate_ori,
                  f_cate2_ori,
                  f_linea_plan,
                  y_oper_ori,
                  y_ind_camb_r_ori,
                  f_tipo_des,
                  f_oper_des,
                  f_cate_des,
                  f_cate2_des,
                  f_plan_des,
                  y_oper_des,
                  y_ind_camb_r_des,
                  f_alarma,
                  f_ccm,
                  f_horario_subtel,
                  cast(y_precio_llamada as int) as y_precio_llamada,
                  cast(f_tiempo_ring as int) as f_tiempo_ring,
                  f_tec_ori,
                  y_tec_des,
                  f_numa_rec,
                  f_numb_rec,
                  f_cod_abo_ori,
                  f_cod_cli_ori,
                  f_info1_ori,
                  x_cod_abo_des,
                  x_cod_cli_des,
                  f_info1_des,
                  x_info2_ori,
                  x_info2_des,
                  f_imsi,
                  f_imei,
                  x_imsi_des,
                  x_imei_des,
                  f_red,
                  x_xnum,
                  f_cellid1,
                  x_cellid2,
                  f_record_type,
                  f_ttfile,
                  (CAST(UNIX_TIMESTAMP(CONCAT(f_fecha_proc,f_hora_proc), 'yyMMddHHmmss') AS TIMESTAMP) + INTERVAL 0 SECONDS) AS sm_datetime_proc,
                  x_fecha_carga,
                  x_hora_carga,
                  f_tipo_central,
                  x_call_type,
                  x_md3,
                  x_tipo_corte,
                  x_canal_corte,
                  (CAST(UNIX_TIMESTAMP(CONCAT(f_fecha_ttfile,f_hh_ttfile), 'yyMMddHHmmss') AS TIMESTAMP) + INTERVAL 0 SECONDS) AS sm_datetime_ttfile,
                  f_filename 
                  FROM df2_view"""
        }

        if (nombre_archivo == "cdr"){
              query_transformacion = """
                  select
                  CASE WHEN LENGTH(numa) = 8 THEN '9' || numa ELSE numa END AS sm_numa,
                  CASE WHEN LENGTH(numb) = 8 THEN '9' || numb ELSE numb END AS sm_numb,
                  (CAST(UNIX_TIMESTAMP(CONCAT(fecha,hora), 'yyMMddHHmmss') AS TIMESTAMP) + INTERVAL 0 SECONDS) AS sm_datetime,
                  cast(dur as int) as dur,
                  rout,
                  rin,
                  cast(hh as smallint) as hh,
                  regla,
                  cast(dir as tinyint) as dir,
                  tipo_ori,
                  cast(oper_ori as smallint) as oper_ori,
                  cate_ori,
                  cate2_ori,
                  plan_ori,
                  ope_r_ori,
                  ind_camb_r_ori,
                  tipo_des,
                  oper_des,
                  cate_des,
                  cate2_des,
                  plan_des,
                  ope_r_des,
                  ind_camb_r_des,
                  alarma,
                  ccm,
                  horario_subtel,
                  cast(precio_llamada as int) as precio_llamada,
                  cast(tiempo_ring as int) as tiempo_ring,
                  tec_ori,
                  tec_des,
                  numa_rec,
                  numb_rec,
                  cod_abo_ori,
                  cod_cli_ori,
                  info1_ori,
                  cod_abo_des,
                  cod_cli_des,
                  info1_des,
                  info2_ori,
                  info2_des,
                  imsi_ori,
                  imei_ori,
                  imsi_des,
                  imei_des,
                  red,
                  xnum,
                  cellid1,
                  cellid2,
                  record_type,
                  ttfile,
                  (CAST(UNIX_TIMESTAMP(CONCAT(fecha_proc,hora_proc), 'yyMMddHHmmss') AS TIMESTAMP) + INTERVAL 0 SECONDS) AS sm_datetime_proc,
                  fecha_carga,
                  hora_carga,
                  tipo_central,
                  call_type,
                  md3,
                  tipo_corte,
                  canal_corte,
                  (CAST(UNIX_TIMESTAMP(CONCAT(fecha_ttfile,hh_ttfile), 'yyMMddHHmmss') AS TIMESTAMP) + INTERVAL 0 SECONDS) AS sm_datetime_ttfile,
                  filename
                  from df2_view"""
        }

            df3 = spark.sql(query_transformacion)
             
             //ELIMINA DUPLICADOS
            df3 = df3.dropDuplicates
            println("[INFO] ELIMINA DUPLICADOS OK")
    
            if (partition_date_column != ""){
              //ELIMINA SOLO LAS FILAS CON partition_date_column NULL
              df4 = df3.na.drop(Seq(partition_date_column))
            } else {
              //ELIMINA TODAS LAS FILAS CON DATA NULA
              df4 = df3.na.drop("all")
            }

            // SE GENERA EL TIMESTAMP
            current_id = new Date().getTime

            //CREA UN FORMATO DE TIMESTAMP
            formatter_id = new SimpleDateFormat("yyyyMMddHHmmss")

            //SE CONCATENA EL TIMESTAMP MAS EL CONTADOR GENERANDO UN ID UNICO PARA LA TABLA DE CONTROL
            bigdata_ctrl_id = formatter_id.format(current_id) + "%03d".format(counter)

            // SE AGREGA LA COLUMNA BIGDATA_CLOSE_DATE Y BIGDATA_CTRL_ID AL DATAFRAME
            df4 = df4.withColumn("bigdata_close_date", lit(null)).withColumn("bigdata_ctrl_id", lit(bigdata_ctrl_id) cast "long")
        
            println("[INFO] SE AGREGA LA COLUMNA BIGDATA_CLOSE_DATE Y BIGDATA_CTRL_ID AL DATAFRAME OK")

            //LA FUNCION DE OBTENER PARTICIONES DESDE EL NOMBRE DEL ARCHIVO SE REGISTRA COMO UDF PARA SER APLICADA AL DATAFRAME
            val udf_particion = udf(obtenerParticiondesdeArchivo _)
        
          //SI LA COLUMNA DE PARTICION ES VACIA SE UTILIZA EL UDF Y SE GENERA LA COLUMNA BIGDATA_CLOSE_DATE DESDE LA COLUMNA FILENAME DEL DATAFRAME LA CUAL TIENE LOS FILENAMES DE LOS ARCHIVOS
          //EN CASO DE TENER COLUMNA DE PARTICION SE FORMATEA LA COLUMNA DESDE EL FORMATO DE ENTRADA AL FORMATO DE SALIDA ESPECIFICADO
          if (partition_date_column == "") {
            df4 = df4.withColumn("bigdata_close_date", udf_particion($"filename_spark", lit(formato_entrada), lit(formato_salida)) cast "date")
          } else {
            df4 = df4.withColumn("bigdata_close_date", from_unixtime(unix_timestamp(col(partition_date_column), formato_entrada), formato_salida) cast "date")
          }
        
            // SE REGISTRA EL DATAFRAME COMO TABLA TEMPORAL
            df4.createOrReplaceTempView("ing_schema")

        val dates = df4.select("bigdata_close_date").distinct().as[String].collect()
        

        //SE OBTIENE EL NOMBRE DE LA COLUMNA O COLUMNAS DE PARTICION DESDE EL HQL. ej: (partition_date,year,month,day)
        val find_text_inicial = create_table_query_hql.toLowerCase().indexOf("partitioned by") + 15
        val find_text_final = find_text_inicial + create_table_query_hql.substring(find_text_inicial).indexOf(")")
        partition_name = create_table_query_hql.substring(find_text_inicial, find_text_final).replace("(", "").replace("string", "").trim()

        //SE SEPARA LA COLUMNA DE PARTICION POR "," EN CASO QUE SEA MAS DE UNA COLUMNA
        var splits = partition_name.split(",").size
        var path_data2_seq = Seq.empty[String]
        select_line2 = select_line
        path_data2 = path_data
        println("[INFO] partition date: " + partition_name + "," + partition_value)
        println("[INFO] ver select  " + select_line)

        // SE RECORRE LA CANTIDAD DE COLUMNAS DE PARTICION,
        // SE MODIFICA EL SELECT AGREGANDO EL NOMBRE DE LA COLUMNA Y EL VALOR DE LA FECHA,
        // SE GENERA LA RUTA HDFS CON LOS NOMBRES DE LAS COLUMNAS DE PARTICION Y LOS VALORES DE LA FECHA
        // SE AGREGA EL NOMBRE DE LA COLUMNA A UNA LISTA
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
        columns = columns.distinct

        val selectExprs = df4.columns.map(col) ++ (0 until columns.size map (i => $"tmp".getItem(i).as(columns(i))))
        
        df5 = df4.withColumn("tmp", split($"bigdata_close_date", "-")).select(selectExprs: _*).drop("filename_spark")
      
      
      // SE ESCRIBE EL DATAFRAME A UN DIRECTORIO TEMPORAL
      df5.write.format("delta").mode(SaveMode.Overwrite).save(path_tmp)

      numPartitions = numPartitionsCalc(path_tmp)
      //SE ELIMINAN TODOS LOS DIRECTORIOS PARTICIONADOS QUE SE CARGARAN EN CASO QUE EXISTAN
      // deletePartitions(path_data2_seq
      val valor = df5.count()

      // SE ESCRIBE EL RESULTADO CON EL NUMERO DE ARCHIVOS CALCULADOS, SE PARTICIONA POR LA O LAS COLUMNAS DE PARTICION.
      df5.repartition(numPartitions).write.partitionBy(columns: _*).format("delta").option("partitionOverwriteMode", "dynamic").mode(SaveMode.Append).save(path_data)

      dbutils.fs.rm(path_tmp, recurse=true)
      dbutils.fs.rm(path_processing, recurse=true)
      dbutils.fs.mkdirs(path_processing)
      dbutils.fs.rm(path_processing_error, recurse=true)
      dbutils.fs.mkdirs(path_processing_error)

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
      end_file_name =  "part-.c000.snappy.parquet"
      //validar si este es el end_file_name
      //println("[INFO] final_name "+end_file_name)

      // SE ACTUALIZA LA TABLA EXTERNA PARA QUE TOME LAS PARTICIONES MODIFICADAS/NUEVAS
      // spark.sql("MSCK REPAIR TABLE " + nomb_proc)
      
      //SE AGREGAN LOS PARAMETOS DE CONTROL A LA LISTA
      controlData = controlData :+ ((status_ejecucion.toString, desc_status_ejecucion, bigdata_ctrl_id, process_name, data_source_type, filename, original_file_date, starttime_nifi, endtime_nifi, totaltime_nifi, starttime_spark, process_type, original_file_size, final_file_size.toString, original_row_count.toString, final_row_count.toString, dif_row_count.toString, final_number_of_files.toString, end_file_name, insert_data_ctrl_date, path_data, pipelineRunId))
    }          
  } catch {
      case e: Exception =>
        status_ejecucion = 1
        desc_status_ejecucion = "[ERROR] " + e
        status_ejecucion_str = "ERROR"
        status_error = e
        println("[ERROR] " + e)
  } 
} else {
  println("Skipped")
} 



// COMMAND ----------


if(status_ejecucion == 1){
    // agrega fila de error para tabla de control
    controlData = controlData :+ (status_ejecucion_str, desc_status_ejecucion, bigdata_ctrl_id, process_name, data_source_type, filename, original_file_date, starttime_nifi, endtime_nifi, totaltime_nifi, starttime_spark, process_type, original_file_size, final_file_size.toString, original_row_count.toString, final_row_count.toString, dif_row_count.toString, final_number_of_files.toString, end_file_name, insert_data_ctrl_date, path_data, pipelineRunId)
    // hdfs_path = path_data
}

// COMMAND ----------

//SE CREA UN DATAFRAME CON TODOS LOS REGISTROS PARA LA TABLA DE CONTROL
var control_dataframe = controlData.toDF("status", "desc_status", "big_data_ctrl_id", "process_name", "data_source_type", "data_source_name", "original_file_date", "starttime_nifi", "endtime_nifi", "totaltime_nifi", "starttime_spark", "process_type", "original_file_size", "final_file_size", "original_row_count", "final_row_count", "dif_row_count", "final_number_of_files", "end_file_name", "insert_data_ctrl_date", "hdfs_path", "pipelineRunId")

// SE OBTIENE EL TIMESTAMP
val end = new Date().getTime

//SE FORMATEA EL TIMESTAMP
val endtime_spark = formatter.format(end)
//SE CALCULA LA DURACION DEL PROCESO SPARK
val totaltime_spark = (end - current).toFloat / 1000

//SE CALCULA LA DURACION DEL PROCESO SPARK + NIFI
var totaltime_process = totaltime_spark

if(totaltime_nifi != ""){
    totaltime_process = totaltime_spark + totaltime_nifi.toInt
}

println("[INFO] SE FORMATEAN LOS TIMESTAMP AL DATAFRAME DE CONTROL")
// SE FORMATEAN LOS TIMESTAMP AL DATAFRAME DE CONTROL.
control_dataframe = control_dataframe.
  withColumn("endtime_spark", lit(endtime_spark)).
  withColumn("totaltime_spark", lit(totaltime_spark)).
  withColumn("totaltime_process", lit(totaltime_process)).
  withColumn("original_file_date", from_unixtime(unix_timestamp($"original_file_date", "yyyyMMdd HHmmss"), "yyyy-MM-dd HH:mm:ss")).
  withColumn("starttime_nifi", from_unixtime(unix_timestamp($"starttime_nifi", "yyyyMMdd HHmmss"), "yyyy-MM-dd HH:mm:ss")).
  withColumn("endtime_nifi", from_unixtime(unix_timestamp($"endtime_nifi", "yyyyMMdd HHmmss"), "yyyy-MM-dd HH:mm:ss")).
  select("status", "desc_status", "big_data_ctrl_id", "process_name", "data_source_type", "data_source_name", "original_file_date", "starttime_nifi", "endtime_nifi", "totaltime_nifi", "starttime_spark", "endtime_spark", "totaltime_spark", "totaltime_process", "insert_data_ctrl_date", "process_type", "original_file_size", "final_file_size", "original_row_count", "final_row_count", "dif_row_count", "final_number_of_files", "end_file_name", "hdfs_path", "pipelineRunId")
// SE ESCRIBEN LOS REGISTROS DE CONTROL EN HIVE
// control_dataframe.write.format("hive").mode(SaveMode.Append).saveAsTable("devtmp.interacciones.control_ingestas")
control_dataframe.write.mode(SaveMode.Append).option("mergeSchema", "true").saveAsTable(catalog_control)

// COMMAND ----------


if(status_ejecucion == 0){
  dbutils.notebook.exit("OK")
}else{
  throw new Exception("Error en el Try-Catch del proceso.")
}
