// Databricks notebook source
//val parametros_ingesta = dbutils.widgets.get("parametros_ingesta")

// COMMAND ----------

//delimitador por el cual se separaran las columnas del archivo (,;|~,etc)
val delimitador = dbutils.widgets.get("delimitador")
//INDICA SI EL ARCHIVO TRAE O NO CABEZERA (true|false)
val encabezado = dbutils.widgets.get("encabezado").toLowerCase()
//DIRECTORIO HDFS DESDE DONDE SE EJECUTARA LA LECTURA Y CARGA DE ARCHIVOS.
val dir_hdfs = dbutils.widgets.get("dir_hdfs")
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
//INDICA EL TOTAL DE TIEMPO EN SEGUNDOS QUE DEMORO LA EJECUCION DE NIFI.
val totaltime_nifi = dbutils.widgets.get("totaltime_nifi")
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
//INDICA SI EL TIPO DE INGESTA ES INGESTA O MEDIACION. POR DEFECTO INGESTA
val nomb_proc = Option(dbutils.widgets.get("nomb_proc"))


// COMMAND ----------



// COMMAND ----------

/* case class FechaModificacion(original_file_date: String)
val dir_hdfs = "abfss://contenedor-test@stbigdatadev02.dfs.core.windows.net/data/interacciones/asignacion_ofertas/usuario_mkt/activacion_disney_fija"
val df = Seq(new FechaModificacion("20231207")).toDF
df.write.text(dir_hdfs + "/prueba_fecha_ultima_modificacion.txt") */

// COMMAND ----------

/* case class FechaModificacion(original_file_date: String)

val df = Seq(new FechaModificacion(original_file_date)).toDF */

/* df.write.format("text").mode(SaveMode.Overwrite).save(dir_hdfs + "/fecha_ultima_modificacion.txt")
df.write.json(dir_hdfs + "/fecha_ultima_modificacion.json") */
//df.write.text(dir_hdfs + "/fecha_ultima_modificacion1.txt")
//df.write.format("json").mode(SaveMode.Overwrite).save(dir_hdfs + "/fecha_ultima_modificacion2.json") 
/* println(dir_hdfs + "/fecha_ultima_modificacion.txt")
df.coalesce(1).write.format("text").mode(SaveMode.Overwrite).option("header", "true").save("abfss://contenedor-test@stbigdatadev02.dfs.core.windows.net/data/interacciones/asignacion_ofertas/usuario_mkt/activacion_disney_fija/fecha_ultima_modificacion4.txt") */


// COMMAND ----------

/* val df2 = spark.read.text("abfss://contenedor-test@stbigdatadev02.dfs.core.windows.net/data/interacciones/asignacion_ofertas/usuario_mkt/activacion_disney_fija/fecha_ultima_modificacion4.txt")
display(df2) */

// COMMAND ----------

// MAGIC %md
// MAGIC ### Importar librerías y definición de variables

// COMMAND ----------

// package com.tchile.bigdata.etl

import java.text.SimpleDateFormat
import java.time.LocalDateTime
import java.util.Date
import java.time.format.DateTimeFormatter
import java.util.concurrent.TimeUnit
import org.apache.hadoop.conf.Configuration
import org.apache.spark.SparkConf
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType
// import com.tchile.bigdata.hdfs.ManejoHdfs
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DataType
//librerias HDFS

import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.hadoop.fs.RemoteIterator
import org.apache.hadoop.fs.LocatedFileStatus
import org.apache.hadoop.fs.FileStatus
/* import org.apache.hadoop.fs.File */

// import org.apache.hadoop.fs.globStatus
// import org.apache.hadoop.fs.delete
// import org.apache.hadoop.fs.getContentSummary
// import org.apache.hadoop.fs.listStatus
//import org.apache.hadoop.fs.{globStatus, delete, getContentSummary, listStatus}
// import org.apache.hadoop.fs.{FileSystem, Path, RemoteIterator, LocatedFileStatus, FileStatus, globStatus, delete, getContentSummary, listStatus}
//import org.apache.hadoop.fs.FileStatus
//import org.apache.hadoop.fs.FileSystem
//import org.apache.hadoop.fs.Path

import org.apache.commons.io
import org.apache.commons.io.FileUtils


import org.apache.hadoop.conf.Configuration


// VARIABLE QUE INSTANCIA UN OBJETO PARA ACCEDER AL FILESYSTEM DEL CLUSTER
val abfs = FileSystem.get(new Configuration())

val fs = FileSystem.get(new Configuration())

// COMMAND ----------

// MAGIC %sql
// MAGIC -- PRUEBA: ELIMINAR SCHEMA (elimina tabla en el blob storage)
// MAGIC drop SCHEMA if exists devtmp.interacciones cascade

// COMMAND ----------

// MAGIC %sql
// MAGIC drop TABLE if exists devtmp.interacciones.activacion_disney_fijo  

// COMMAND ----------

// MAGIC %sql
// MAGIC -- PRUEBA: Crear SCHEMA (estos schemas hay que crearlos luego de la migracion inicial de todas las tablas)
// MAGIC
// MAGIC --USE CATALOG <catalog>;
// MAGIC CREATE SCHEMA IF NOT EXISTS devtmp.interacciones
// MAGIC   MANAGED LOCATION 'abfss://contenedor-test@stbigdatadev02.dfs.core.windows.net/data/interacciones'
// MAGIC   COMMENT 'schema para datos de interacciones'

// COMMAND ----------

// MAGIC %sql
// MAGIC CREATE SCHEMA IF NOT EXISTS devtmp.schemaprueba
// MAGIC   LOCATION 'abfss://contenedor-test@stbigdatadev02.dfs.core.windows.net/data/schemaprueba'
// MAGIC   COMMENT 'schema para datos de schemaprueba'
// MAGIC   

// COMMAND ----------

// MAGIC %run /Shared/framework_ingesta/funciones/funciones_ingesta_simple_v3

// COMMAND ----------

/* val mensaje_nifi = ${delimitador}: val delimitador = mensaje_nifi.split(":")(0)
${encabezado}: encabezado = mensaje_nifi.split(":")(1).toLowerCase()
${dir_hdfs}: dir_hdfs = "/mnt/flightdata-TestKeyVault" + mensaje_nifi.split(":")(2)
${formato_entrada}: formato_entrada = mensaje_nifi.split(":")(3)
${dataType}: dataType = mensaje_nifi.split(":")(4)
${plataforma}: plataforma = mensaje_nifi.split(":")(5)
${categoria}: categoria = mensaje_nifi.split(":")(6)
${loadType}: loadType = mensaje_nifi.split(":")(7)
${periodicity}: periodicity = mensaje_nifi.split(":")(8)
${starttime_nifi}: starttime_nifi = mensaje_nifi.split(":")(9)
${finishtime_nifi}: endtime_nifi = mensaje_nifi.split(":")(10)
${duration_nifi}: totaltime_nifi = mensaje_nifi.split(":")(11)
${original_size_file}: original_file_size = mensaje_nifi.split(":")(12)
${original_date_file}: original_file_date = mensaje_nifi.split(":")(13)
: dateFormatFile = Option(mensaje_nifi.split(":", -1)(14)).filterNot(_.isEmpty).getOrElse("yyyy-MM-dd")
${formato_salida}: timestampFormat = Option(mensaje_nifi.split(":", -1)(15)).filterNot(_.isEmpty).getOrElse("yyyy-MM-dd'T'HH:mm:ss[.SSS][XXX]")
${dateFormatFile}" formato_salida = Option(mensaje_nifi.split(":", -1)(16)).filterNot(_.isEmpty).getOrElse("yyyy-MM-dd")
*/

// COMMAND ----------

// MAGIC %sql DROP SCHEMA IF EXISTS devtmp.interacciones;

// COMMAND ----------


// val mensaje_nifi = ";:true:/mnt/Test/test_ingesta_simple2:yyyyMMdd:noraw:REAJUSTE_IPC_FIJO:FACTURACION:f:d:20231018 121400:20231018 131400:3600000:25647820:20231018 114805:yyyy-MM-dd:yyyyMMddHHmmssSSS:yyyy-MM-dd"
//val mensaje_nifi = parametros_ingesta

//starttime_nifi = ${lineageStartDate:format("yyyyMMdd HHmmss")}
//duration_nifi = ${now():toNumber():minus(${lineageStartDate}):divide(1000)}
//finishtime_nifi = ${now():toNumber():format("yyyyMMdd HHmmss")}
//original_date_file = ${file.lastModifiedTime:toDate("yyyy-MM-dd'T'HH:mm:ssZ"):format("yyyyMMdd HHmmss")}
//ts ${now():toNumber():format('yyyyMMddHHmmssSSS')}


// COMMAND ----------

/* // PRUEBA: IMPRIMIR INDICES DE LOS PARAMETROS (ELIMINAR/COMENTAR)
val params_names_nifi = "${delimitador}:${encabezado}:${dir_hdfs}:${formato_entrada}:${dataType}:${plataforma}:${categoria}:${loadType}:${periodicity}:${starttime_nifi}:${finishtime_nifi}:${duration_nifi}:${original_size_file}:${original_date_file:{NULL}: ${formato_salida}: ${dateFormatFile}" 
val params_names_nifi_split = params_names_nifi.split(":")
var cont = 0
for( i <- mensaje_nifi.split(":")){
  println(i, cont, params_names_nifi_split(cont))
  cont=cont+1
} */

// COMMAND ----------



// COMMAND ----------

// MAGIC %md
// MAGIC ### Prueba script ingesta simple

// COMMAND ----------

val current = new Date().getTime
val formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
val formatter2 = new SimpleDateFormat("yyyy-MM-dd")
val starttime_spark = formatter.format(current)
val insert_date = formatter2.format(current)
println("[INFO] Operaciones_ETL_Batch")

// COMMAND ----------

val conf = new SparkConf()
  .setMaster("yarn")
  .setAppName("framework_ingestas")
  .set("spark.driver.allowMultipleContexts", "true")
  .set("spark.yarn.queue", "ingesta")
  .set("spark.sql.crossJoin.enabled", "true")
  .set("spark.sql.debug.maxToStringFields", "1000")
  .set("spark.sql.autoBroadcastJoinThreshold", "-1")

val sparkh = SparkSession.builder().config(conf)
  .config("spark.sql.warehouse.dir", "/usr/hdp/current/spark-client/conf/hive-site.xml")
  .config("spark.submit.deployMode", "cluster")
  .enableHiveSupport()
  .getOrCreate()

import sparkh.implicits._

//sparkh.conf.set("spark.sql.parquet.writeLegacyFormat", "true")
//sparkh.conf.set("spark.sql.sources.partitionColumnTypeInference.enabled", "false")

// COMMAND ----------

// MAGIC %md
// MAGIC ##### SE DEFINEN LOS DIRECTORIOS DE TRABAJO PARA EL PROCESO

// COMMAND ----------

val path_processing = dir_hdfs.concat("/processing").concat("/")
val path_landing = path_processing.replace("processing", "landing")
val path_processing_error = path_processing.replace("processing", "processing_error")
val path_log = path_processing.replace("processing", "log")
val path_data = path_processing.replace("processing", dataType)
val path_tmp = path_processing.replace("processing", "tmp")
val path_tmp_transform = path_processing.replace("processing", "tmp_transform")

println("[INFO] path_processing " + path_processing)
println("[INFO] path_log " + path_log)
println("[INFO] path_landing " + path_landing)
println("[INFO] path_processing_error " + path_processing_error)
println("[INFO] delimitador " + delimitador)
println("[INFO] encabezado " + encabezado)
println("[INFO] formato_entrada " + formato_entrada)

// COMMAND ----------

/*
DESDE EL DIRECTORIO HDFS SE OBTIENE EL ULTIMO ELEMENTO HACIENDO SPLIT POR "/" CON EL FIN DE OBTENER EL NOMBRE DE LA ENTIDAD
ej: /data/interacciones/ordenes/oap/detalle_numeros_portados EL VALOR OBTENIDO SERIA detalle_numeros_portados
*/
val nombre_archivo = dir_hdfs.split("/").last

/*
  SE LEE EL ARCHIVO HQL EL CUAL CONTIENE LAS INSTRUCCIONES ddl DE CREATE TABLE PARA HIVE.
(ESTE DEBE TENER EL MISMO NOMBRE QUE LA VARIABLE OBTENIDA EN EL PASO ANTERIOR EJ: detalle_numeros_portados.hql
*/
// val subida_datos = openFile(dir_hdfs + "/" + nombre_archivo + ".hql") -- DESCOMENTAR
val subida_datos = """
create external table if not exists devtmp.interacciones.activaciones_disney_fijo
(
ott string,
id string,
estado string,
fechaultestadogmt0 date,
producto string,
vinculacion string,
msisdn string,
fechaaltagmt0 string,
fechavinculogmt0 string,
valor_wdp string,
rev_share string,
tipo_transaccion string,
retail_price string,
vat_rate string,
net_price string,
servicio_terra string,
tipo_moneda string,
yearmonth string,
bigdata_close_date date,
bigdata_ctrl_id bigint
)
using parquet
partitioned by (year string, month string)
location 'abfss://contenedor-test@stbigdatadev02.dfs.core.windows.net/data/interacciones/asignacion_ofertas/usuario_mkt/activacion_disney_fija/raw'
"""
println("[INFO] subida_datos " + subida_datos)

/*
  SE LEE EL ARCHIVO JSON EL CUAL CONTIENE LAS ESTRUCTURA Y ESQUEMA CON SUS TIPOS DE DATOS .
(ESTE DEBE TENER EL MISMO NOMBRE QUE LA VARIABLE OBTENIDA EN EL PASO ANTERIOR EJ: detalle_numeros_portados.json
*/
// val schema_data = DataType.fromJson(openFile(dir_hdfs + "/" + nombre_archivo + ".json")).asInstanceOf[StructType]
val schema_data_aux= """ 
{
  "type": "struct",
  "fields": [
    {
      "name": "ott",
      "type": "string",
      "nullable": true,
      "metadata": {}
    },
    {
      "name": "id",
      "type": "string",
      "nullable": true,
      "metadata": {}
    },
    {
      "name": "estado",
      "type": "string",
      "nullable": true,
      "metadata": {}
    },
    {
      "name": "fechaultestadogmt0",
      "type": "date",
      "nullable": true,
      "metadata": {}
    },
    {
      "name": "producto",
      "type": "string",
      "nullable": true,
      "metadata": {}
    },
    {
      "name": "vinculacion",
      "type": "string",
      "nullable": true,
      "metadata": {}
    },
    {
      "name": "msisdn",
      "type": "string",
      "nullable": true,
      "metadata": {}
    },
    {
      "name": "fechaaltagmt0",
      "type": "string",
      "nullable": true,
      "metadata": {}
    },
    {
      "name": "fechavinculogmt0",
      "type": "string",
      "nullable": true,
      "metadata": {}
    },
    {
      "name": "valor_wdp",
      "type": "string",
      "nullable": true,
      "metadata": {}
    },
    {
      "name": "rev_share",
      "type": "string",
      "nullable": true,
      "metadata": {}
    },
    {
      "name": "tipo_transaccion",
      "type": "string",
      "nullable": true,
      "metadata": {}
    },
    {
      "name": "retail_price",
      "type": "string",
      "nullable": true,
      "metadata": {}
    },
    {
      "name": "vat_rate",
      "type": "string",
      "nullable": true,
      "metadata": {}
    },
    {
      "name": "net_price",
      "type": "string",
      "nullable": true,
      "metadata": {}
    },
    {
      "name": "servicio_terra",
      "type": "string",
      "nullable": true,
      "metadata": {}
    },
    {
      "name": "tipo_moneda",
      "type": "string",
      "nullable": true,
      "metadata": {}
    },
    {
      "name": "yearmonth",
      "type": "string",
      "nullable": true,
      "metadata": {}
    }
  ]
}
"""
val schema_data = DataType.fromJson(schema_data_aux).asInstanceOf[StructType]

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
//val nomb_proc = subida_datos.substring(0, subida_datos.indexOf('\n')).substring(subida_datos.toLowerCase().indexOf("create external table if not exists ") + 36).trim()

//SE CONCATENA SPARK AL NOMBRE DEL PROCESO
val process_name = "spark_" + nomb_proc
// SE DEFINE QUE EL TIPO DE FUENTE SERA UN ARCHIVO
val data_source_type = "file"
val hdfs_path = path_data
println("[INFO] nombre proceso " + process_name)
println("[INFO] nombre proceso " + nomb_proc)

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
var controlData = Seq.empty[(String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String)]


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

      /*SE RECORRE LA LISTA Y SI EXISTEN ARCHIVOS ESTOS SON MOVIDOS A ERROR YA QUE QUEDARON DE UNA EJECUCION ANTERIOR FALLIDA*/
      if (list != null && list.hasNext) {
        filename = list.next().getPath().getName()
        delete(path_processing_error + filename)
        moverArchivoAbfs(path_processing + filename, path_processing_error)
        delete(path_processing + filename)
        println("[INFO] FILENAME ERROR to processing error " + filename)
      } else {
        println("""[INFO] NO EXISTE ARCHIVO EN PROCESSING""")
      }

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

      // SE LISTAN LOS ARCHIVOS EN EL DIRECTORIO LANDING
      list = listFiles(path_landing)
      // SE CREA UN CONTADOR
      var counter = 1

      while (list != null && list.hasNext) {
        filename = list.next().getPath().getName()
        if (!(filename.startsWith("."))) {
          delete(path_processing + filename)
          moverArchivoAbfs(path_landing + filename, path_processing)
          delete(path_landing + filename)
          println("[INFO] FILENAME landing " + filename)
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

          // SE OBTIENE LA FECHA DEL NOMBRE DEL ARCHIVO CON EL FORMATO DE SALIDA PROPORCIONADO
          partition_value = obtenerParticiondesdeArchivo(filename, formato_entrada, formato_salida)

          /*SE BUSCA EN EL HQL SI EXISTE LA LINEA CON "PARTITIONED BY"*/
          if (subida_datos.toLowerCase().contains("partitioned by")) {
            println("""[INFO] obtener particion desde archivo""")

            //SE OBTIENE EL NOMBRE DE LA COLUMNA O COLUMNAS DE PARTICION DESDE EL HQL. ej: (partition_date,year,month,day)
            partition_name = subida_datos.substring(subida_datos.toLowerCase().indexOf("partitioned by") + 15, subida_datos.toLowerCase().indexOf("partitioned by") + 15 + subida_datos.substring(subida_datos.toLowerCase().indexOf("partitioned by") + 15).indexOf(")")).replace("(", "").replace("string", "").trim()

            //SE SEPARA LA COLUMNA DE PARTICION POR "," EN CASO QUE SEA MAS DE UNA COLUMNA
            var splits = partition_name.split(",").size
            select_line2 = select_line
            path_data2 = path_data
            println("[INFO] partition date: " + partition_name + "," + partition_value)
            println("[INFO] ver select  " + select_line)

            /*
             SE RECORRE LA CANTIDAD DE COLUMNAS DE PARTICION,
             SE MODIFICA EL SELECT AGREGANDO EL NOMBRE DE LA COLUMNA Y EL VALOR DE LA FECHA,
             SE GENERA LA RUTA HDFS CON LOS NOMBRES DE LAS COLUMNAS DE PARTICION Y LOS VALORES DE LA FECHA
             SE AGREGA EL NOMBRE DE LA COLUMNA A UNA LISTA
             */

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
          if (exists_file(dir_hdfs + "/transformation/transformation.json")) {

            /* SE LEE EL ARCHIVO JSON EL CUAL CONTIENE LAS ESTRUCTURA Y ESQUEMA CON SUS TIPOS DE DATOS. */
            schema_data_trns = DataType.fromJson(openFile(dir_hdfs + "/transformation/transformation.json")).asInstanceOf[StructType]

            //CREA UN OBJETO CON TODO LO EXISTENTE EN EL DIRECTORIO TRANSFORMATION
            val statSubquery = fs.globStatus(new Path(dir_hdfs + "/transformation/*"))

            //FILTRA LOS ELEMENTOS QUE SEAN ARCHIVOS Y PREGUNTA POR LOS NOMBRES DE ARCHIVOS QUE COMIENCEN CON SUBQUERY
            val subQueryCount = statSubquery.filter(_.isFile).filter(_.getPath.getName.startsWith("subquery"))

            //CREA UN ARREGLO VACIO DE TAMAÑO N SEGUN CANTIDAD DE ARCHIVOS SUBQUERY EXISTAN
            val querysResult = new Array[String](subQueryCount.size)

            // SE LEE EL ARCHIVO CSV CON LA OPCION FAILFAST Y SCHEMA. SI EL SCHEMA PROPORCIONADO NO CONCUERDA CON EL ARCHIVO EL PROCESO SE DETIENE POR REGISTROS MALFORMADOS
            dataframe1 = sparkh.read.option("delimiter", delimitador).option("header", encabezado).option("mode", "FAILFAST").schema(schema_data_trns).csv(path_processing + filename)

            //CREA UNA VISTA TEMPORAL CON EL DATAFRAME
            dataframe1.createOrReplaceTempView(dir_hdfs.split("/").last)

            //CARGA LA QUERY CON LAS TRANSFORMACIONES
            query = openFile(dir_hdfs + "/transformation/transformation.hql")
            query2 = query

            //RECORRE EL ARREGLO Y APLICA LAS SUBQUERY EXISTENTES AKMACENANDO EL RESULTADO EN UNA VARIABLE
            //CONCATENA A LA QUERY PRINCIPAL LOS VALORES DE LA SUBQUERY
            for (i <- 0 until subQueryCount.size) {
              var subq = openFile(subQueryCount(i).getPath.toString())
              querysResult(i) = sparkh.sql(subq).first.getString(0) //get(0)
              query2 = query2.replace("querysResult(" + i + ")", querysResult(i))
              //sparkh.sql("""set querysResult("""+i+""")='"""+querysResult(i)+"""'""")
            }

            //EJECUTA LA QUERY Y GUARDA EL RESULTADO EN UN DATAFRAME
            df1 = sparkh.sql(query2)

            //ESCRIBE LA DATA CON LAS TRANSFORMACIONES APLICADAS A UN DIRECTORIO TEMPORAL
            df1.write.option("delimiter", delimitador).option("header", encabezado).option("emptyValue", "").mode(SaveMode.Overwrite).csv(path_tmp_transform)

            // SE LEE EL ARCHIVO CSV CON LA OPCION FAILFAST Y SCHEMA. SI EL SCHEMA PROPORCIONADO NO CONCUERDA CON EL ARCHIVO EL PROCESO SE DETIENE POR REGISTROS MALFORMADOS
            df2 = sparkh.read.option("delimiter", delimitador).option("header", encabezado).
              option("mode", "FAILFAST").schema(schema_data).option("DateFormat", dateFormatFile).option("timestampFormat", timestampFormat).csv(path_tmp_transform + "*")
          } //EN CASO DE NO NECESITAR TRANSFORMACIONES SE LEE EL ARCHIVO CON EL ESQUEMA PROPORCIONADO
          else {
            df2 = sparkh.read.option("delimiter", delimitador).option("header", encabezado).
              option("mode", "FAILFAST").schema(schema_data).option("DateFormat", dateFormatFile).option("timestampFormat", timestampFormat).csv(path_processing + filename)
          }

          //ELIMINA DUPLICADOS
          df2 = df2.dropDuplicates

          // SE CUENTA LA CANTIDAD DE REGISTROS
          original_row_count = df2.count

          //ELIMINA FILAS NULAS
          df3 = df2.na.drop("all")

          // SE GENERA EL TIMESTAMP
          current_id = new Date().getTime

          //CREA UN FORMATO DE TIMESTAMP
          formatter_id = new SimpleDateFormat("yyyyMMddHHmmss")

          //SE CONCATENA EL TIMESTAMP MAS EL CONTADOR GENERANDO UN ID UNICO PARA LA TABLA DE CONTROL
          bigdata_ctrl_id = formatter_id.format(current_id) + "%03d".format(counter)

          // SE AGREGA LA COLUMNA BIGDATA_CLOSE_DATE Y BIGDATA_CTRL_ID AL DATAFRAME
          df3 = df3.withColumn("bigdata_close_date", to_date(lit(partition_value), formato_salida)).withColumn("bigdata_ctrl_id", lit(bigdata_ctrl_id) cast "long")

          // SE REGISTRA EL DATAFRAME COMO TABLA TEMPORAL
          df3.createOrReplaceTempView("ing_schema")

          // SE OBTIENE UN STATUS GLOBAL DE LA RUTA HDFS
          val status = fs.globStatus(new Path(path_data))

          // VALIDA SI LA INGESTA DEBE IR PARTICIONADA
          if (subida_datos.toLowerCase().contains("partitioned by")) {

            // SE APLICA LA QUERY CREADA Y SE GUARDA EL RESULTADO EN UN DATAFRAME
            df_sal1 = sparkh.sql(select_line2)

            // SE ESCRIBE EL DATAFRAME A UN DIRECTORIO TEMPORAL
            df_sal1.write.mode(SaveMode.Overwrite).parquet(path_tmp)

            // SE CALCULA LA CANTIDAD DE ARCHIVOS A ESCRIBIR EN HDFS.
            numPartitions = numPartitionsCalc(path_tmp)

            // SE ELIMINA LO ESCRITO EN LA RUTA TEMPORAL
            fs.delete(new Path(path_tmp), true)

            //SE ELIMINA LA RUTA HDFS Y SU PARTICION EN CASO DE EXISTIR, SI YA EXISTIA SE MARCA COMO REPROCESO.
            //EN CASO DE SER MEDIACION SE MARCA COMO NORMAL
            if (fs.delete(new Path(path_data2), true)) process_type = "reproceso" else process_type = "normal"

            // SE ESCRIBE EL RESULTADO CON EL NUMERO DE ARCHIVOS CALCULADOS, SE PARTICIONA POR LA O LAS COLUMNAS DE PARTICION.
            df_sal1.repartition(numPartitions).write.partitionBy(columns: _*).mode(SaveMode.Append).parquet(path_data)

            // SE CALCULA EL TIMESTAMP
            current_ins = new Date().getTime

            // SE CREA UN FORMATO DE FECHA
            formatter_ins = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")

            //SE FORMATEA EL TIMESTAMP Y SE ALMACENA EN LA VARIABLE QUE INDICA EL TIEMPO DE INSERCION DEL ARCHIVO/REGISTRO.
            insert_data_ctrl_date = formatter_ins.format(current_ins)

            //SE MODIFICA LOS NOMBRES DE LOS ARCHIVOS PARQUET A LA NOMENCLATURA DE ARQUITECTURA
            parquetPutName(status, dataType, plataforma, categoria, loadType, periodicity)

            // SE OBTIENE EL TAMAÑO FINAL DE LOS ARCHIVOS ESCRITOS
            final_file_size = fs.getContentSummary(new Path(path_data2)).getLength

            // SE CUENTA LA CANTIDAD DE REGISTROS DEL ARCHIVO FINAL
            final_row_count = sparkh.read.parquet(path_data2).count

            // SE VALIDA SI LA CANTIDAD DE FILAS ORIGINALES VS LA CANTIDAD DE FILAS ESCRITAS ES DISTINTA.
            if (original_row_count != final_row_count) dif_row_count = 1 else dif_row_count = 0

            // SE CUENTA LA CANTIDAD DE ARCHIVOS GENERADOS
            final_number_of_files=fs.getContentSummary(new Path(path_data2)).toString(false,false,true).
              split(" ").filterNot(_.isEmpty)(1).toInt

            // SE GUARDA EL NOMBRE FINAL DEL ARCHIVO
            final_name = findFile(listFiles(path_data2), dataType)
            end_file_name = final_name.substring(final_name.indexOf("/"+dataType+".") + 1, final_name.indexOf(".N")) + ".snappy.parquet"

            // SE ACTUALIZA LA TABLA EXTERNA PARA QUE TOME LAS PARTICIONES MODIFICADAS/NUEVAS
            sparkh.sql("MSCK REPAIR TABLE " + nomb_proc)

            //SE AGREGAN LOS PARAMETOS DE CONTROL A LA LISTA
            controlData = controlData :+ (bigdata_ctrl_id, process_name, data_source_type, filename, original_file_date, starttime_nifi, endtime_nifi, totaltime_nifi, starttime_spark, process_type, original_file_size, final_file_size.toString, original_row_count.toString, final_row_count.toString, dif_row_count.toString, final_number_of_files.toString, end_file_name, insert_data_ctrl_date, hdfs_path)
          } else {

            // SE APLICA LA QUERY CREADA Y SE GUARDA EL RESULTADO EN UN DATAFRAME
            df_sal1 = sparkh.sql(select_line2)

            // SE ESCRIBE EL DATAFRAME A UN DIRECTORIO TEMPORAL
            df_sal1.write.mode(SaveMode.Append).parquet(path_tmp)

            // SE CALCULA LA CANTIDAD DE ARCHIVOS A ESCRIBIR EN HDFS.
            numPartitions = numPartitionsCalc(path_tmp)

            // SE ELIMINA LO ESCRITO EN LA RUTA TEMPORAL
            fs.delete(new Path(path_tmp), true)

            //AL SER TRUNCA Y CARGA EL TIPO DE PROCESO SIEMPRE SERA NORMAL
            process_type = "normal"

            // SE ESCRIBE EL RESULTADO CON EL NUMERO DE ARCHIVOS CALCULADOS, HACIENDO OVERWRITE YA QUE ES TRUNCA Y CARGA
            df_sal1.repartition(numPartitions).write.mode(SaveMode.Overwrite).parquet(path_data)

            // SE CALCULA EL TIMESTAMP
            current_ins = new Date().getTime

            // SE CREA UN FORMATO DE FECHA
            formatter_ins = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")

            //SE FORMATEA EL TIMESTAMP Y SE ALMACENA EN LA VARIABLE QUE INDICA EL TIEMPO DE INSERCION DEL ARCHIVO/REGISTRO.
            insert_data_ctrl_date = formatter_ins.format(current_ins)

            //SE MODIFICA LOS NOMBRES DE LOS ARCHIVOS PARQUET A LA NOMENCLATURA DE ARQUITECTURA
            parquetPutName(status, dataType, plataforma, categoria, loadType, periodicity)

            // SE OBTIENE EL TAMAÑO FINAL DE LOS ARCHIVOS ESCRITOS
            final_file_size = fs.getContentSummary(new Path(path_data)).getLength

            // SE CUENTA LA CANTIDAD DE REGISTROS DEL ARCHIVO FINAL
            final_row_count = sparkh.read.parquet(path_data).count

            // SE VALIDA SI LA CANTIDAD DE FILAS ORIGINALES VS LA CANTIDAD DE FILAS ESCRITAS ES DISTINTA
            if (original_row_count != final_row_count) dif_row_count = 1 else dif_row_count = 0

            // SE CUENTA LA CANTIDAD DE ARCHIVOS GENERADOS
            final_number_of_files=fs.getContentSummary(new Path(path_data)).toString(false,false,true).
              split(" ").filterNot(_.isEmpty)(1).toInt - 1

            // SE GUARDA EL NOMBRE FINAL DEL ARCHIVO
            final_name = findFile(listFiles(path_data2), dataType)
            end_file_name = final_name.substring(final_name.indexOf("/"+dataType+".") + 1, final_name.indexOf(".N")) + ".snappy.parquet"

            //SE AGREGAN LOS PARAMETOS DE CONTROL A LA LISTA
            controlData = controlData :+ (bigdata_ctrl_id, process_name, data_source_type, filename, original_file_date, starttime_nifi, endtime_nifi, totaltime_nifi, starttime_spark, process_type, original_file_size, final_file_size.toString, original_row_count.toString, final_row_count.toString, dif_row_count.toString, final_number_of_files.toString, end_file_name, insert_data_ctrl_date, hdfs_path)
          }
          println("""[INFO] Eliminar archivos en processing HDFS""")

          //SE ELIMINA EL ARCHIVO INGESTADO
          delete(path_processing.concat("/" + filename))

          //SE VUELVE A LISTAR EL DIRECTORIO PARA SU SIGUIENTE ITERACION
          list = listFiles(path_landing)
          counter = counter + 1

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
      val RDDsalida = sparkh.sparkContext.parallelize(List(salida))
      RDDsalida.coalesce(1).saveAsTextFile(path_log)
    }
    println("[INFO] proceso terminado")
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

    // SE FORMATEAN LOS TIMESTAMP AL DATAFRAME DE CONTROL.
    control_dataframe = control_dataframe.
      withColumn("endtime_spark", lit(endtime_spark)).
      withColumn("totaltime_spark", lit(totaltime_spark)).
      withColumn("totaltime_process", lit(totaltime_process)).
      withColumn("original_file_date", from_unixtime(unix_timestamp($"original_file_date", "yyyyMMdd HHmmss"), "yyyy-MM-dd HH:mm:ss")).
      withColumn("starttime_nifi", from_unixtime(unix_timestamp($"starttime_nifi", "yyyyMMdd HHmmss"), "yyyy-MM-dd HH:mm:ss")).
      withColumn("endtime_nifi", from_unixtime(unix_timestamp($"endtime_nifi", "yyyyMMdd HHmmss"), "yyyy-MM-dd HH:mm:ss")).
      select("big_data_ctrl_id", "process_name", "data_source_type", "data_source_name", "original_file_date", "starttime_nifi", "endtime_nifi", "totaltime_nifi", "starttime_spark", "endtime_spark", "totaltime_spark", "totaltime_process", "insert_data_ctrl_date", "process_type", "original_file_size", "final_file_size", "original_row_count", "final_row_count", "dif_row_count", "final_number_of_files", "end_file_name", "hdfs_path")
    // SE ESCRIBEN LOS REGISTROS DE CONTROL EN HIVE
    control_dataframe.write.format("hive").mode(SaveMode.Append).saveAsTable("prueba_control_procesos.control_ingestas")

  

// COMMAND ----------

// MAGIC %md
// MAGIC # PRUEBAS A DATAFRAME CREADO
// MAGIC

// COMMAND ----------

 
val df = spark.read.parquet("/mnt/Test/test_ingesta_simple2/noraw")
df.select("month").distinct().show()


display(df)
