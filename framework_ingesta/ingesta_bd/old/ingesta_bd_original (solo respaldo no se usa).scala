// Databricks notebook source
// MAGIC %md
// MAGIC ## Librerías

// COMMAND ----------

// DBTITLE 0,Librerías
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import java.util.Properties
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileStatus
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.spark.SparkConf
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions.split
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.types.IntegerType
import java.text.SimpleDateFormat
import java.time.LocalDateTime
import java.util.Date
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.functions._
import org.apache.hadoop.fs.RemoteIterator
import org.apache.hadoop.fs.LocatedFileStatus
import java.util.Calendar

// Librerías repetidas
// import org.apache.hadoop.fs.Path
// import org.apache.spark.sql.DataFrame

// COMMAND ----------

// MAGIC %run ../funciones/Manejo_ADLS

// COMMAND ----------

// MAGIC %python
// MAGIC
// MAGIC # PRUEBA: COPIAR ARCHIVO A LANDING
// MAGIC
// MAGIC # ELIMINAR ARCHIVOS MOVIDOS DENTRO DE LA EJECUCION ANTERIOR
// MAGIC dbutils.fs.rm("/mnt/Test/gpon_resumen_linea/processing/part-m-00000")
// MAGIC dbutils.fs.rm("/mnt/Test/gpon_resumen_linea/processing_error/part-m-00000")
// MAGIC dbutils.fs.rm("/mnt/Test/gpon_resumen_linea/landing/part-m-00000")
// MAGIC
// MAGIC # COPIAR ARCHIVO CSV A CARPETA LANDING
// MAGIC origen = "/mnt/Test/gpon_resumen_linea/landing_aux/part-m-00000"
// MAGIC destino = "/mnt/Test/gpon_resumen_linea/landing/part-m-00000"
// MAGIC dbutils.fs.cp(origen, destino)

// COMMAND ----------

// MAGIC %md
// MAGIC ## Administración de recursos y tareas en un clúster de Hadoop
// MAGIC

// COMMAND ----------

// MAGIC %md
// MAGIC
// MAGIC ##Mensaje NIFI y Seteo de variables

// COMMAND ----------

// val delimitador = dbutils.widgets.get("delimitador")
// val dir_hdfs = "/mnt/flightdata-TestKeyVault" + dbutils.widgets.get("dir_hdfs")
// val dataType = dbutils.widgets.get("dataType")
// val plataforma = dbutils.widgets.get("plataforma")
// val categoria = dbutils.widgets.get("categoria")
// val loadType = dbutils.widgets.get("loadType")
// val periodicity = dbutils.widgets.get("periodicity")
// val partition_date_column = Option(dbutils.widgets.get("partition_date_column")).filterNot(_.isEmpty).getOrElse("")
// val starttime_nifi = dbutils.widgets.get("starttime_nifi")
// val endtime_nifi = dbutils.widgets.get("endtime_nifi")
// val totaltime_nifi = dbutils.widgets.get("totaltime_nifi")
// val original_file_size = dbutils.widgets.get("original_file_size")
// val original_file_date = dbutils.widgets.get("original_file_date")
// val data_source_name = dbutils.widgets.get("data_source_name")
// val formato_partition_column = Option(dbutils.widgets.get("formato_partition_column")).filterNot(_.isEmpty).getOrElse("")
// val formato_partition_column_out = Option(dbutils.widgets.get("formato_partition_column_out")).filterNot(_.isEmpty).getOrElse("yyyy-MM-dd")
// val auxiliar_partition_value = Option(dbutils.widgets.get("auxiliar_partition_value")).filterNot(_.isEmpty).getOrElse("")
// val timestampFormat = Option(dbutils.widgets.get("timestampFormat")).filterNot(_.isEmpty).getOrElse("yyyy-MM-dd HH:mm:ss[.SSS][XXX]")
// val quote = Option(dbutils.widgets.get("quote")).filterNot(_.isEmpty).getOrElse("\"")

// COMMAND ----------

val path_dl = "/mnt/contenedor-test/data/gestion_recursos/operacion_red/assia/gpon_expresse/landing"
// println(path_dl)


display(spark.read.load(path_dl))

// COMMAND ----------

// DBTITLE 0,MENSAJE NIFI
// val mensaje_nifi = "/mnt/Test/gpon_resumen_linea:~:raw:gestion_recursos:gpon_expresse:f:d:ESTIMATION_DATE:20231019 160000:finish_time_nifi:1000:DSLEXPRESSE.V_PON_LINE_SUMMAR:yyyy-MM-dd::::quote"

val current = new Date().getTime
val formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
val formatter2 = new SimpleDateFormat("yyyy-MM-dd")
val starttime_spark = formatter.format(current)
val insert_date = formatter2.format(current)

// println("##################")

// //DIRECTORIO HDFS DESDE DONDE SE EJECUTARA LA LECTURA Y CARGA DE ARCHIVOS.
// val dir_hdfs = mensaje_nifi.split(":")(0)//mensaje_nifi.split(":",-1)(0)
// //delimitador por el cual se separaran las columnas del archivo (,;|~,etc)
// val delimitador = mensaje_nifi.split(":",-1)(1)
// //INDICA SI LA DATA ES DE TIPO RAW,NORAW,CONFORMADO.
// val dataType = mensaje_nifi.split(":",-1)(2)
// //INDICA DESDE DONDE VIENE LA INFORMACION A CARGAR (ATIS,SAP,ETC)
// val plataforma = mensaje_nifi.split(":",-1)(3)
// //INDICA A QUE CATEGORIA DE LOS DOMINIOS DE INFORMACION CORRESPONDEN(INTERACCIONES,CAMPAÑAS,GESTION_RECURSOS,ETC)
// val categoria = mensaje_nifi.split(":",-1)(4)
// //INDICA EL TIPO DE CARGA A EJECUTAR INCREMENTAL,FULL,etc (i,d)
// val loadType = mensaje_nifi.split(":",-1)(5)
// //INDICA LA PERIODICIDAD DE LA INGESTA, diaria, semanal, mensual (d,s,m).
// val periodicity = mensaje_nifi.split(":",-1)(6)
// //COLUMNA POR LA CUAL LA DATA SERA PARTICIONADA EN HDFS, VACIO POR DEFECTO
// var partition_date_column = Option(mensaje_nifi.split(":",-1)(7)).filterNot(_.isEmpty).getOrElse("")
// //INDICA EL TIMESTAMP DEL MOMENTO EN QUE NIFI INICIO EL FLUJO.
// val starttime_nifi = mensaje_nifi.split(":",-1)(8)
// //INDICA EL TIMESTAMP DEL MOMENTO EN QUE NIFI TERMINO EL FLUJO.
// val endtime_nifi = mensaje_nifi.split(":",-1)(9)
// //INDICA EL TOTAL DE TIEMPO EN SEGUNDOS QUE DEMORO LA EJECUCION DE NIFI.
// val totaltime_nifi = mensaje_nifi.split(":",-1)(10)
// //N/A
// val original_file_size = ""
// //N/A
// val original_file_date = ""
// //NOMBRE DE LA TABLA Y ESQUEMA DE LA BD CON FORMATO ESQUEMA.TABLA
// val data_source_name = mensaje_nifi.split(":",-1)(11)
// //FORMATO DE FECHA EN LA QUE LA COLUMNA DE PARTICION VIENE (Ej: yyyyMMdd,yyyy-MM-dd,ETC. VACIA POR DEFECTO)
// var formato_partition_column = Option(mensaje_nifi.split(":",-1)(12)).filterNot(_.isEmpty).getOrElse("")
// //FORMATO DE FECHA EN LA QUE LA COLUMNA DE PARTICION QUEDARA EN EL ARCHIVO DE SALIDA (Ej: yyyyMMdd,yyyy-MM-dd,ETC. yyyy-MM-dd POR DEFECTO)
// var formato_partition_column_out = Option(mensaje_nifi.split(":",-1)(13)).filterNot(_.isEmpty).getOrElse("yyyy-MM-dd")
// //EN CASO QUE LA TABLA NO TENGA COLUMNA DE PARTICION, SE PERMITE INGRESAR UNA FECHA EXTERNA QUE REEMPLAZE LA COLUMNA (ej FECHA DE CARGA SACADA DESD UNA TABLA DE LOG)
// var auxiliar_partition_value =  Option(mensaje_nifi.split(":",-1)(14)).filterNot(_.isEmpty).getOrElse("")
// //FORMATO EN QUE VIENEN LAS COLUMNAS TIPO TIMESTAMP DESDE LA BD. yyyy-MM-dd'T'HH:mm:ss[.SSS][XXX] POR DEFECTO
// // var timestampFormat = Option(mensaje_nifi.split(":",-1)(15)).filterNot(_.isEmpty).getOrElse("yyyy-MM-dd'T'HH:mm:ss[.SSS][XXX]")
// var timestampFormat = "yyyy-MM-dd HH:mm:ss[.SSS][XXX]"

// //EN CASO DE TENER DATA ENCOMILLADA SE DEFINEN COMILLAS DOBLES, SIMPLES, ETC. COMILLAS DOBLES POR DEFECTO
// //var quote = Option(mensaje_nifi.split(":",-1)(16)).filterNot(_.isEmpty).getOrElse("\"")

// var quote = "\""
// quote = quote.replace("none","")

// COMMAND ----------

// %sql drop table db.prueba

// COMMAND ----------

// MAGIC %md
// MAGIC ##Funciones Ingesta desde BD
// MAGIC
// MAGIC

// COMMAND ----------

// DBTITLE 0,Funciones
// VARIABLE QUE INSTANCIA UN OBJETO PARA ACCEDER AL FILESYSTEM DEL CLUSTER
val fs = FileSystem.get(new Configuration())

  /*FUNCION QUE TOMA LOS ARCHIVOS PARQUET GENERADOS Y LES MODIFICA EL NOMBRE SEGUN NOMENCLATURA DE ARQUITECTURA
    * EJ: MODIFICA LOS ARCHIVOS PARQUET GENERADOS COMO PART-00001.SNAPPY A raw.interacciones.sap.i.d.20200801205689.snappy
    * RECIBE UN ARREGLO CON EL LISTADO DE ARCHIVOS,DIRECTORIOS Y SUBDIRECTORIOS EXISTENTES DONDE SE REALIZA LA INGESTA EN HDFS
    * ITERANDO DE FORMA RECURSIVA BUSCA CUALQUIER ELEMENTO QUE SEA UN ARCHIVO Y QUE SU NOMBRE COMIENCE CON "part-*", LUEGO HACE UN MV
    * A LA MISMA RUTA CAMBIANDOLE EL NOMBRE CON LOS PARAMETROS INDICADOS
    */
def parquetPutName(status: Array[FileStatus]): Unit = {
    val current = DateTimeFormatter.ofPattern("yyyyMMddHHmmss").format(LocalDateTime.now)
    for (i <- status.indices) {
    val fileStatus = status(i)
    if (fileStatus.isDirectory) {
        val conf = new Configuration();
        val filesystem = FileSystem.get(fileStatus.getPath().toUri(), conf)
        val subStatus = filesystem.listStatus(fileStatus.getPath())
        println("directory:" + fileStatus.getPath())
        parquetPutName(subStatus)
    } else if (fileStatus.getPath.toString.contains("_SUCCESS") != true && fileStatus.getPath.toString.contains("part-")) {
        println("     file:" + fileStatus.getPath().toString)
        println("     filesub" + fileStatus.getPath.toString.substring(0, fileStatus.getPath.toString.indexOf("part")) + dataType + "." + plataforma + "." + categoria + "." + loadType + "." + periodicity + "." + current + ".N" + i + ".snappy.parquet")
        fs.rename(new Path(fileStatus.getPath.toString), new Path(fileStatus.getPath.toString.substring(0, fileStatus.getPath.toString.indexOf("part")) + dataType + "." + plataforma + "." + categoria + "." + loadType + "." + periodicity + "." + current + ".N" + i + ".snappy.parquet"))
    }
    }
}
 /*
 * FUNCION QUE CALCULA CUANTOS ARCHIVOS PARQUET DEBEN GENERARSE COMO SALIDA GENERANDO ARCHIVOS DE TAMAÑO SUPERIOR A 128 MB
 * (PESO MINIMO RECOMENDADO PARA EL CLUSTER).
 * SE ESCRIBE LA DATA A UN DIRECTORIO TEMPORAL, SE OBTIENE EL PESO TOTAL EN BYTES DE LA DATA ESCRITA, SE APLICA LA FUNCION MATH.CEIL
 * LA CUAL RECIBE EL PESO Y CALCULA CELDAS CERCANAS O SUPERIORES A 128 MB, OBTENIENDO COMO SALIDA LA CANTIDAD DE CELDAS A GENERAR.
 * ESTO SE ALMACENA EN LA VARIABLE numPartitions LA CUAL INDICA CUANTOS ARCHIVOS DE SALIDA SE CREARAN CON EL PESO MINIMO.
 */

def numPartitionsCalc(tmp: String): Int = {
    var dataSize = fs.getContentSummary(new Path(tmp)).getLength
    var numPartitions = 0
    numPartitions = Math.ceil(dataSize / 1073741824L).toInt
    numPartitions = if (numPartitions == 0) 1 else numPartitions
    return numPartitions
  }
  
/* FUNCION QUE RECORRE EL LISTADO DE DIRECTORIOS A CARGAR EN HDFS, ELIMINANDO RUTAS QUE YA EXISTAN */
def deletePartitions(path_data2_seq: Seq[String]): Unit = {
    for (i <- 0 until path_data2_seq.length) {
    fs.delete(new Path(path_data2_seq(i).toString()), true)
    }
    }

// COMMAND ----------

// MAGIC %md
// MAGIC ##Funciones Manejo HDFS

// COMMAND ----------

// VARIABLE QUE INSTANCIA UN OBJETO PARA ACCEDER AL FILESYSTEM DEL CLUSTER
val abfs = FileSystem.get(new Configuration())

// COMMAND ----------

// MAGIC %md
// MAGIC ##Directorios de trabajo para el proceso

// COMMAND ----------

val path_processing = dir_hdfs.concat("/processing").concat("/")
val path_landing = path_processing.replace("processing", "landing")
val path_processing_error = path_processing.replace("processing", "processing_error")
val path_log = path_processing.replace("processing", "log")
val path_data = path_processing.replace("processing", dataType)
val path_tmp = path_processing.replace("processing", "tmp")

// println("[INFO] path_processing " + path_processing)
// println("[INFO] path_log " + path_log)
// println("[INFO] path_landing " + path_landing)
// println("[INFO] path_processing_error " + path_processing_error)

// COMMAND ----------

/*
DESDE EL DIRECTORIO HDFS SE OBTIENE EL ULTIMO ELEMENTO HACIENDO SPLIT POR "/" CON EL FIN DE OBTENER EL NOMBRE DE LA ENTIDAD
ej: /data/interacciones/ordenes/oap/detalle_numeros_portados EL VALOR OBTENIDO SERIA detalle_numeros_portados 
*/
val nombre_archivo = dir_hdfs.split("/").last

/* 
  SE LEE EL ARCHIVO HQL EL CUAL CONTIENE LAS INSTRUCCIONES ddl DE CREATE TABLE PARA HIVE.
ESTE DEBE TENER EL MISMO NOMBRE QUE LA VARIABLE OBTENIDA EN EL PASO ANTERIOR EJ: detalle_numeros_portados.hql
*/
val subida_datos = openFile(dir_hdfs + "/" + nombre_archivo + ".hql")
println("[INFO] subida_datos " + subida_datos)

//   SE LEE EL ARCHIVO JSON EL CUAL CONTIENE LAS ESTRUCTURA Y ESQUEMA CON SUS TIPOS DE DATOS .
// (ESTE DEBE TENER EL MISMO NOMBRE QUE LA VARIABLE OBTENIDA EN EL PASO ANTERIOR EJ: detalle_numeros_portados.json
val schema_data = DataType.fromJson(openFile(dir_hdfs + "/" + nombre_archivo + ".json")).asInstanceOf[StructType]

  /*
  DESDE EL HQL LEIDO SE REALIZA UN SUBSTRING PARA OBTENER EL NOMBRE DEL PROCESO DESDE LA PRIMERA LINEA DEL ARCHIVO
  ej: CREATE EXTERNAL TABLE IF NOT EXISTS capa_semantica.detalle_numeros_portados tendria el valor de capa_semantica.detalle_numeros_portados
  */
val nomb_proc = subida_datos.substring(0, subida_datos.indexOf('\n')).substring(subida_datos.toLowerCase().indexOf("create external table if not exists ") + 36).trim()
val process_name = "spark_" + nomb_proc
// //SE CONCATENA SPARK AL NOMBRE DEL PROCESO
val data_source_type = "table"
// SE DEFINE QUE EL TIPO DE FUENTE SERA UN ARCHIVO
val hdfs_path = path_data
println("[INFO] nombre proceso " + nomb_proc) 

var salida = 1
var filename = ""
var bigdata_close_date = ""
var partition_value = ""
println("[INFO] Creación de tabla si no existe")
// //SE DEFINEN DATAFRAMES VACIOS Y SE EJECUTA EL HQL EN HIVE CREANDO LA TABLA EXTERNA
// var dataframe1, df1, df2, df_sal1, df_sal2 = sparkh.sql(subida_datos)
var dataframe1, df1, df2, df_sal1, df_sal2 = spark.sql(subida_datos)
var controlData = Seq.empty[(String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String)]

// COMMAND ----------

// MAGIC %md
// MAGIC #TRY

// COMMAND ----------

try {

  // SE ELIMINA EL DIRECTORIO DE LOG EN CASO DE EXISTIR
  delete(dir_hdfs.concat("/log"))

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

  /* SE RECORRE LA LISTA CON LOS CAMPOS , POR CADA CAMPO A LA VARIABLE SELECT_LINE SE LE CONCATENAN TODOS LOS CAMPOS FORMANDO UNA QUERY*/
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

  /*
    SE RECORREN TODOS LOS ARCHIVOS EN LANDING, SE BORRA EL ARCHIVO SI ES QUE EXISTE EN PROCESSING, 
    POSTERIORMENTE SE MUEVE DESDE LANDING A PROCESSING PARA SU PROCESAMIENTO
    */
  while (list != null && list.hasNext) {
    filename = list.next().getPath().getName()
    delete(path_processing + filename)
    moverArchivoAbfs(path_landing + filename, path_processing)
    delete(path_landing + filename)
    println("[INFO] FILENAME landing " + filename)
  }

  val status = fs.globStatus(new Path(dir_hdfs))
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

  // SE LEE EL ARCHIVO CSV CON LA OPCION FAILFAST Y SCHEMA. SI EL SCHEMA PROPORCIONADO NO CONCUERDA CON EL ARCHIVO EL PROCESO SE DETIENE POR REGISTROS MALFORMADOS
  dataframe1 = spark.read.
  option("delimiter", delimitador).
  option("header", "false").
  option("mode", "FAILFAST").
  option("timestampFormat",timestampFormat).
  option("quote",quote).
  schema(schema_data).
  csv(path_processing + "part*")

  //SE ELIMINAN DUPLICADOS
  dataframe1 = dataframe1.dropDuplicates

  //SE CUENTA LA CANTIDAD DE REGISTROS
  original_row_count = dataframe1.count

  // SE ELIMINAN FILAS NULAS
  df2 = dataframe1.na.drop("all")

  // SE GENERA EL TIMESTAMP
  current_id = new Date().getTime

  //SE CREA UN FORMATO DE TIMESTAMP
  formatter_id = new SimpleDateFormat("yyyyMMddHHmmss")

  //SE CONCATENA EL TIMESTAMP MAS EL CONTADOR GENERANDO UN ID UNICO PARA LA TABLA DE CONTROL
  bigdata_ctrl_id = formatter_id.format(current_id) + "%03d".format(counter)

  // SE AGREGA LA COLUMNA BIGDATA_CLOSE_DATE Y BIGDATA_CTRL_ID AL DATAFRAME
  df2 = df2.withColumn("bigdata_close_date",lit(null)).withColumn("bigdata_ctrl_id", lit(bigdata_ctrl_id) cast "long")
  var partition_date = ""

  /*SE BUSCA EN EL HQL SI EXISTE LA LINEA CON "PARTITIONED BY"*/
  if (subida_datos.toLowerCase().contains("partitioned by")) {
    
    /* EVALUA SI LA COLUMNA DE PARTICION ESTA VACIA, 
      * GENERANDO UNA COLUMNA DE PARTICION NUEVA CON LA FECHA DEL DIA Y AGREGANDOLA AL DATAFRAME
      * 
      * EN CASO QUE LA COLUMNA DE PARTICION Y LA COLUMNA AUXILIAR ESTAN VACIAS, SE OBTIENE EL TIMESTAMP Y SE FORMATEA A YYYY-MM-DD
      * UTILIZANDO ESTA FECHA COMO COLUMNA DE PARTICION
      */
    if (partition_date_column == "" && auxiliar_partition_value != "") {
      df2 = df2.withColumn("bigdata_close_date", lit(auxiliar_partition_value) cast "date")
      val partition_date_column = "bigdata_close_date"
      val formato_partition_column = "yyyy-MM-dd"
    } else if (partition_date_column == "" && auxiliar_partition_value == "") {
      val format = new SimpleDateFormat("yyyy-MM-dd")
      bigdata_close_date = format.format(Calendar.getInstance().getTime())
      df2 = df2.withColumn("bigdata_close_date", lit(bigdata_close_date) cast "date")
      val partition_date_column = "bigdata_close_date"
      val formato_partition_column = "yyyy-MM-dd"
    }
    
    
      //SE CREA UNA COLUMNA DE PARTICION TEMPORAL, TOMANDO LA COLUMNA DE PARTICION Y CAMBIANDO EL FORMATO DE FECHA DE ENTRADA AL DE SALIDA
    df2 = df2.withColumn("bigdata_close_date", from_unixtime(unix_timestamp(col(partition_date_column), formato_partition_column), formato_partition_column_out) cast "date")
    
    //GENERA UN ARREGLO CON LAS DISTINTAS FECHAS EXISTENTES EN EL DATAFRAME
    val dates = df2.select("bigdata_close_date").distinct().rdd.map(r => r(0)).collect()
    
    //SE OBTIENE EL NOMBRE DE LA COLUMNA O COLUMNAS DE PARTICION DESDE EL HQL. ej: (partition_date,year,month,day)
    partition_name = subida_datos.substring(subida_datos.toLowerCase().indexOf("partitioned by") + 15, subida_datos.toLowerCase().indexOf("partitioned by") + 15 + subida_datos.substring(subida_datos.toLowerCase().indexOf("partitioned by") + 15).indexOf(")")).replace("(", "").replace("string", "").trim()
    
    //SE SEPARA LA COLUMNA DE PARTICION POR "," EN CASO QUE SEA MAS DE UNA COLUMNA
    var splits = partition_name.split(",").size
    
    //SE CREAN 2 LISTAS STRING, UNA PARA ALMACENAR LA LISTA DE RUTAS HDFS Y OTRA PARA LA LISTA DE COLUMNAS DE PARTICION
    var path_data2_seq = Seq.empty[String]
    var columns = Seq.empty[String]

    
    /* SE RECORRE EL ARREGLO CON LAS FECHAS, SE CONSTRUYEN LAS DISTINTAS RUTAS HDFS PARTICIONADAS 
    AGREGANDO EL NOMBRE DE LA COLUMNA Y SU VALOR DONDE SE CARGARA LA DATA.
    
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
    path_data2_seq=path_data2_seq.distinct
    columns = columns.distinct 

    //SE GENERA UN SELECT DE LAS COLUMNAS DEL DATAFRAME RECORRIENDO SUS COLUMNAS Y SUMANDO LAS COLUMNAS DE PARTICION MAS SUS VALORES
    val selectExprs = df2.columns.map(col) ++ (0 until columns.size  map (i => $"tmp".getItem(i).as(columns(i))))
    
    //SE APLICA EL SELECT CREADO AL DATAFRAME
    val df3 = df2.withColumn("tmp", split($"bigdata_close_date", "-")).select(selectExprs:_*)

    // SE ESCRIBE EL DATAFRAME A UN DIRECTORIO TEMPORAL
    df3.write.mode(SaveMode.Append).parquet(path_tmp)

    // SE CALCULA LA CANTIDAD DE ARCHIVOS A ESCRIBIR EN HDFS.
    var numPartitions = numPartitionsCalc(path_tmp)

    // SE ELIMINA LO ESCRITO EN LA RUTA TEMPORAL
    fs.delete(new Path(path_tmp), true)

    //SE ELIMINAN TODOS LOS DIRECTORIOS PARTICIONADOS QUE SE CARGARAN EN CASO QUE EXISTAN
    deletePartitions(path_data2_seq)

    // SE ESCRIBE EL RESULTADO CON EL NUMERO DE ARCHIVOS CALCULADOS, SE PARTICIONA POR LA O LAS COLUMNAS DE PARTICION.
    df3.repartition(numPartitions).write.partitionBy(columns: _*).mode(SaveMode.Append).parquet(path_data)

    //SE RECORRE LA LISTA CON LOS DIRECTORIOS
    for (i <- 0 until path_data2_seq.length) {
      
      // SE CALCULA EL TIMESTAMP
      current_ins = new Date().getTime
      
      // SE CREA UN FORMATO DE FECHA
      formatter_ins = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
      
      //SE FORMATEA EL TIMESTAMP Y SE ALMACENA EN LA VARIABLE QUE INDICA EL TIEMPO DE INSERCION DEL ARCHIVO/REGISTRO.
      insert_data_ctrl_date = formatter_ins.format(current_ins)
      
      //SE MODIFICA LOS NOMBRES DE LOS ARCHIVOS PARQUET A LA NOMENCLATURA DE ARQUITECTURA
      parquetPutName(status)
      
      // SE OBTIENE EL TAMAÑO FINAL DE LOS ARCHIVOS ESCRITOS
      final_file_size = fs.getContentSummary(new Path(path_data2_seq(i))).getLength
      
      // SE CUENTA LA CANTIDAD DE REGISTROS DEL ARCHIVO FINAL
      final_row_count = final_row_count + spark.read.parquet(path_data2_seq(i)).count
      
        // SE VALIDA SI LA CANTIDAD DE FILAS ORIGINALES VS LA CANTIDAD DE FILAS ESCRITAS ES DISTINTA
      if (original_row_count != final_row_count) dif_row_count = 1 else dif_row_count = 0
      
        // SE CUENTA LA CANTIDAD DE ARCHIVOS GENERADOS
      final_number_of_files = fs.getContentSummary(new Path(path_data2_seq(i))).getFileCount
      
      // SE GUARDA EL NOMBRE FINAL DEL ARCHIVO
      // final_name = objHdfs.listFiles(path_data2_seq(i)).next().getPath().getName()
      // end_file_name = final_name.substring(final_name.indexOf("/raw.") + 1, final_name.indexOf(".N")) + ".snappy.parquet"
      final_name = findFile(listFiles(path_data2_seq(i)), dataType)
      end_file_name = final_name.substring(final_name.indexOf("/"+dataType+".") + 1, final_name.indexOf(".N")) + ".snappy.parquet"  


      // SE ACTUALIZA LA TABLA EXTERNA PARA QUE TOME LAS PARTICIONES MODIFICADAS/NUEVAS
      spark.sql("MSCK REPAIR TABLE " + nomb_proc)
      
      //SE AGREGAN LOS PARAMETOS DE CONTROL A LA LISTA
      controlData = controlData :+ (bigdata_ctrl_id, process_name, data_source_type, data_source_name, original_file_date, starttime_nifi, endtime_nifi, totaltime_nifi, starttime_spark, process_type, original_file_size, final_file_size.toString, original_row_count.toString, final_row_count.toString, dif_row_count.toString, final_number_of_files.toString, end_file_name, insert_data_ctrl_date, hdfs_path)
      // var controlData = controlData1 :+ (bigdata_ctrl_id, process_name, data_source_type, data_source_name, original_file_date, starttime_nifi, endtime_nifi, totaltime_nifi, starttime_spark, process_type, original_file_size, final_file_size.toString, original_row_count.toString, final_row_count.toString, dif_row_count.toString, final_number_of_files.toString, end_file_name, insert_data_ctrl_date, hdfs_path)
    }

  } else {
    
    /* EVALUA SI LA COLUMNA DE PARTICION ESTA VACIA, 
      * GENERANDO UNA COLUMNA DE PARTICION NUEVA CON LA FECHA DEL DIA Y AGREGANDOLA AL DATAFRAME
      * 
      * EN CASO QUE LA COLUMNA DE PARTICION Y LA COLUMNA AUXILIAR ESTAN VACIAS, SE OBTIENE EL TIMESTAMP Y SE FORMATEA A YYYY-MM-DD
      * UTILIZANDO ESTA FECHA COMO COLUMNA DE PARTICION
      */
    
    if (auxiliar_partition_value != "") {
      df2 = df2.withColumn("bigdata_close_date", lit(auxiliar_partition_value) cast "date")
    } else{
      val format = new SimpleDateFormat("yyyy-MM-dd")
      bigdata_close_date = format.format(Calendar.getInstance().getTime())
      df2 = df2.withColumn("bigdata_close_date", lit(bigdata_close_date) cast "date")
    }
    
    // SE ESCRIBE EL DATAFRAME A UN DIRECTORIO TEMPORAL
    df2.write.mode(SaveMode.Overwrite).parquet(path_tmp)
    
    // SE CALCULA LA CANTIDAD DE ARCHIVOS A ESCRIBIR EN HDFS.
    var numPartitions = numPartitionsCalc(path_tmp)
    
    // SE ELIMINA LO ESCRITO EN LA RUTA TEMPORAL
    fs.delete(new Path(path_tmp), true)
    
    //SE LISTAN TODOS LOS ARCHIVOS EXISTENTES EN LA RUTA HDFS
    val deletePaths = fs.globStatus(new Path(path_data + "/*")).map(_.getPath)
    
    //SE ELIMINAN TODOS LOS ARCHIVOS LISTADOS 
    deletePaths.foreach { path => fs.delete(path, true) }
    
    // SE ESCRIBE EL RESULTADO CON EL NUMERO DE ARCHIVOS CALCULADOS, HACIENDO OVERWRITE YA QUE ES TRUNCA Y CARGA
    df2.repartition(numPartitions).write.mode(SaveMode.Append).parquet(path_data)
    
      // SE CALCULA EL TIMESTAMP
    current_ins = new Date().getTime
    
    // SE CREA UN FORMATO DE FECHA
    formatter_ins = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    
    //SE FORMATEA EL TIMESTAMP Y SE ALMACENA EN LA VARIABLE QUE INDICA EL TIEMPO DE INSERCION DEL ARCHIVO/REGISTRO.
    insert_data_ctrl_date = formatter_ins.format(current_ins)
    
    //SE MODIFICA LOS NOMBRES DE LOS ARCHIVOS PARQUET A LA NOMENCLATURA DE ARQUITECTURA
    parquetPutName(status)
    
      // SE OBTIENE EL TAMAÑO FINAL DE LOS ARCHIVOS ESCRITOS
    final_file_size = fs.getContentSummary(new Path(path_data)).getLength
    
    // SE CUENTA LA CANTIDAD DE REGISTROS DEL ARCHIVO FINAL
    final_row_count = spark.read.parquet(path_data).count
    
    // SE VALIDA SI LA CANTIDAD DE FILAS ORIGINALES VS LA CANTIDAD DE FILAS ESCRITAS ES DISTINTA
    if (original_row_count != final_row_count) dif_row_count = 1 else dif_row_count = 0
    
    // SE CUENTA LA CANTIDAD DE ARCHIVOS GENERADOS
    final_number_of_files = fs.getContentSummary(new Path(path_data)).getFileCount - 1
    
    // SE GUARDA EL NOMBRE FINAL DEL ARCHIVO
    final_name = fs.listStatus(new Path(path_data)).map(_.getPath.getName).drop(final_number_of_files.toInt)(0).toString
    end_file_name = final_name.substring(final_name.indexOf("/raw.") + 1, final_name.indexOf(".N")) + ".snappy.parquet"
    
    //SE AGREGAN LOS PARAMETOS DE CONTROL A LA LISTA
    controlData = controlData :+ (bigdata_ctrl_id, process_name, data_source_type, data_source_name, original_file_date, starttime_nifi, endtime_nifi, totaltime_nifi, starttime_spark, process_type, original_file_size, final_file_size.toString, original_row_count.toString, final_row_count.toString, dif_row_count.toString, final_number_of_files.toString, end_file_name, insert_data_ctrl_date, hdfs_path)
    // var controlData = controlData1 :+ (bigdata_ctrl_id, process_name, data_source_type, data_source_name, original_file_date, starttime_nifi, endtime_nifi, totaltime_nifi, starttime_spark, process_type, original_file_size, final_file_size.toString, original_row_count.toString, final_row_count.toString, dif_row_count.toString, final_number_of_files.toString, end_file_name, insert_data_ctrl_date, hdfs_path)

  }

  println("""[INFO] Eliminar archivos en processing HDFS""")

  //SE LISTAN TODOS LOS ARCHIVOS EXISTENTES EN LA RUTA HDFS
  var deletePaths = fs.globStatus(new Path(path_processing + "/*")).map(_.getPath)

  //SE ELIMINAN TODOS LOS ARCHIVOS LISTADOS 
  deletePaths.foreach { path => fs.delete(path, true) }

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
println("[INFO] proceso terminado")


// COMMAND ----------

//SE CREA UN DATAFRAME CON TODOS LOS REGISTROS PARA LA TABLA DE CONTROL
var control_dataframe = controlData.toDF("big_data_ctrl_id", "process_name", "data_source_type", "data_source_name", "original_file_date", "starttime_nifi", "endtime_nifi", "totaltime_nifi", "starttime_spark", "process_type", "original_file_size", "final_file_size", "original_row_count", "final_row_count", "dif_row_count", "final_number_of_files", "end_file_name", "insert_data_ctrl_date","hdfs_path")

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
select("big_data_ctrl_id", "process_name", "data_source_type", "data_source_name", "original_file_date", "starttime_nifi", "endtime_nifi", "totaltime_nifi", "starttime_spark", "endtime_spark", "totaltime_spark", "totaltime_process", "insert_data_ctrl_date", "process_type", "original_file_size", "final_file_size", "original_row_count", "final_row_count", "dif_row_count", "final_number_of_files", "end_file_name","hdfs_path")

// SE ESCRIBEN LOS REGISTROS DE CONTROL EN HIVE
control_dataframe.write.format("hive").mode(SaveMode.Append).saveAsTable("prueba_control_procesos.control_ingestas")
display(control_dataframe)
