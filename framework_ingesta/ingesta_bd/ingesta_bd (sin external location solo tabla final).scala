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
var partition_date_column = Option(dbutils.widgets.get("partition_date_column")).filterNot(_.isEmpty).getOrElse("")
//EN CASO QUE LA TABLA NO TENGA COLUMNA DE PARTICION, SE PERMITE INGRESAR UNA FECHA EXTERNA QUE REEMPLAZE LA COLUMNA (ej FECHA DE CARGA SACADA DESD UNA TABLA DE LOG)
var auxiliar_partition_value =  Option(dbutils.widgets.get("auxiliar_partition_value")).filterNot(_.isEmpty).getOrElse("")
//FORMATO DE FECHA EN LA QUE LA COLUMNA DE PARTICION VIENE (Ej: yyyyMMdd,yyyy-MM-dd,ETC. VACIA POR DEFECTO)
var formato_partition_column = Option(dbutils.widgets.get("formato_partition_column")).filterNot(_.isEmpty).getOrElse("")
//FORMATO DE FECHA EN LA QUE LA COLUMNA DE PARTICION QUEDARA EN EL ARCHIVO DE SALIDA (Ej: yyyyMMdd,yyyy-MM-dd,ETC. yyyy-MM-dd POR DEFECTO)
var formato_partition_column_out = Option(dbutils.widgets.get("formato_partition_column_out")).filterNot(_.isEmpty).getOrElse("yyyy-MM-dd")
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

// ESQUEMAS DE DATOS Y QUERY HQL
val hql_query = dbutils.widgets.get("hql_query")
val json_schema = dbutils.widgets.get("json_schema")



var nombre_catalogo = nomb_proc.split("\\.")(0) //NOMBRE DEL CATALOGO DE UNITY CATALOG
val nombre_archivo = dir_adls.split("/").last //NOMBRE DE LOS ARCHIVOS HQL Y JSON
val process_name = "spark_" + nomb_proc
val data_source_type = "table"


val spark = SparkSession.builder.getOrCreate()
val current = new Date().getTime
val formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
val formatter2 = new SimpleDateFormat("yyyy-MM-dd")
val starttime_spark = formatter.format(current)
val insert_date = formatter2.format(current)


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

def mergeData2(table_name: String, df: DataFrame, merge_string: String, partition: Boolean, columns: Seq[String]): Boolean = {

  try{
    println("Modo Merge")
    // Crea las DeltaTable a partir de las rutas de Delta Lake
    val deltaTable = DeltaTable.forName(table_name)
    //val deltaTable = spark.read.table(table_name)

    // Realiza la operación MERGE INTO
    deltaTable.alias("a")
            .merge(df.alias("b"),  merge_string)
            .whenMatched().updateAll()
            .whenNotMatched().insertAll()
            .execute()
          
    true
            
  }catch{
   case e: Exception =>
      if (partition) {
        println("Modo Write")
        df.write.format("delta")
                .mode("overwrite")
                .partitionBy(columns: _*)
                .option("overwriteSchema", "true")
                .saveAsTable(table_name)
      } else {
        println("Modo Write")
        df.write.format("delta")
                .mode("overwrite")
                .option("overwriteSchema", "true")
                .saveAsTable(table_name)
      }

      false 
  }
}

// COMMAND ----------


// import scala.util.parsing.json._

def defineSchemaJson2(json_schema: String, path_df: String): DataFrame = {
  val dfConverted = spark.read.json(Seq(json_schema).toDS).drop("type")
  val flatList = dfConverted.select(explode($"fields").as("fieldsFlat")).collect().toList.flatMap(row => row.toSeq)

  val df = spark.read.format("parquet").load(path_df)

  var df2 = df  // Dataframe

  for (elem <- flatList) {
      val row = elem.asInstanceOf[Row]
      val columnName = row.getString(0)
      val tipo = row.getString(2)

      // El DF debe tener la misma estructura que el json, luego se procede a cambiar el tipo de la columna
      df2 = df2.withColumn(columnName, col(columnName).cast(tipo))
    
      // println(s"columnName: $columnName, Tipo: $tipo")
      // df2.printSchema()
  }
  return df2
}

// COMMAND ----------

// MAGIC %md
// MAGIC ##Directorios de trabajo para el proceso

// COMMAND ----------

//Cambio
/* val path_landing   = dir_adls + "/landing/" */
val path_stage     = dir_adls + "/stage/"
/* val path_log       = dir_adls + "/log/"
val path_diario    = dir_adls + "/diario/"
val path_mensual   = dir_adls + "/mensual/"
val path_data      = dir_adls + "/"+dataType+"/"
val path_tmp       = dir_adls + "/tmp" */
/* var path_json = dir_adls + "/" + nombre_archivo + ".json"
var path_hql2 = dir_adls + "/" + nombre_archivo + ".hql" */

// COMMAND ----------

// MAGIC %md
// MAGIC # Lee archivo parquet (Carpeta Stage) y se guarda en tabla final tipo Delta

// COMMAND ----------

//Se define el schema para el dataframe
var df = defineSchemaJson2(json_schema, path_stage)

// COMMAND ----------

// MAGIC %md
// MAGIC ### Crea tabla manejada Delta

// COMMAND ----------

var subida_datos =  hql_query // openFile(dir_adls + "/" + nombre_archivo + ".hql")

// REPLACE LOCATION MANAGED
subida_datos = subida_datos.replace("LOCATIONMANAGEDCONTAINERHQL", dir_adls + "/" + dataType)
subida_datos = subida_datos.replace("NOMBREPROCUNITYCATALOGDATABRICKS", nomb_proc)

println("[INFO] subida_datos " + subida_datos)

//CREA TABLA DEL CONTENIDO DEL ARCHIVO HQL
var df_sal1 = spark.sql(subida_datos)

// COMMAND ----------

// MAGIC %md
// MAGIC ### Agregar columnas (bigdata_close_date) y (bigdata_ctrl_id)

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

// COMMAND ----------

// MAGIC %md
// MAGIC ### Eliminacion de duplicados
// MAGIC
// MAGIC - En la version anterior se guardaba el dataset que contenia los duplicados dentro de landing

// COMMAND ----------

//SE CUENTA LA CANTIDAD DE REGISTROS
val original_row_count = df.count

//se eliminan valores nulos y duplicados
println("SE ELIMINAN DUPLICADOS y NULOS")
df = df.dropDuplicates
df = df.na.drop("all")



// COMMAND ----------

// MAGIC %md
// MAGIC ### Se obtienen las columnas y particiones

// COMMAND ----------

/* val (columns, select_line) = listPartitions(subida_datos) */

// COMMAND ----------



println("[INFO] prueba 1")
println("subida_datos\n "+subida_datos)
val format = new SimpleDateFormat("yyyy-MM-dd")

println("[INFO] prueba 2")

if (subida_datos.toLowerCase().contains("partitioned by")) {
    // EVALUA SI LA COLUMNA DE PARTICION ESTA VACIA, GENERANDO UNA COLUMNA DE PARTICION NUEVA CON LA FECHA DEL DIA Y AGREGANDOLA AL DATAFRAME. EN CASO QUE LA COLUMNA DE PARTICION Y LA COLUMNA AUXILIAR ESTAN VACIAS, SE OBTIENE EL TIMESTAMP Y SE FORMATEA A YYYY-MM-DD UTILIZANDO ESTA FECHA COMO COLUMNA DE PARTICION
    
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

    //SE CREA UNA COLUMNA DE PARTICION TEMPORAL, TOMANDO LA COLUMNA DE PARTICION Y CAMBIANDO EL FORMATO DE FECHA DE ENTRADA AL DE SALIDA
    df = df.withColumn("bigdata_close_date", from_unixtime(unix_timestamp(col(partition_date_column), formato_partition_column), formato_partition_column_out) cast "date")
      
    //SE OBTIENE EL NOMBRE DE LA COLUMNA O COLUMNAS DE PARTICION DESDE EL HQL. ej: (partition_date,year,month,day)
    val find_text_inicial = subida_datos.toLowerCase().indexOf("partitioned by") + 15
    val find_text_final = find_text_inicial + subida_datos.substring(find_text_inicial).indexOf(")")
    val partition_name = subida_datos.substring(find_text_inicial, find_text_final).replace("(", "").replace("string", "").trim()

    //SE SEPARA LA COLUMNA DE PARTICION POR "," EN CASO QUE SEA MAS DE UNA COLUMNA
    var splits = partition_name.split(",").size

    // SE RECORRE LA CANTIDAD DE COLUMNAS DE PARTICION,
    // SE MODIFICA EL SELECT AGREGANDO EL NOMBRE DE LA COLUMNA Y EL VALOR DE LA FECHA,
    // SE GENERA LA RUTA HDFS CON LOS NOMBRES DE LAS COLUMNAS DE PARTICION Y LOS VALORES DE LA FECHA
    // SE AGREGA EL NOMBRE DE LA COLUMNA A UNA LISTA
    
    println("[INFO] prueba 3")
    for (x <- 0 to splits - 1 by 1) {
      columns = columns :+ partition_name.split(",")(x).trim()
    }

    //SE GENERA UN SELECT DE LAS COLUMNAS DEL DATAFRAME RECORRIENDO SUS COLUMNAS Y SUMANDO LAS COLUMNAS DE PARTICION MAS SUS VALORES
    val selectExprs = df.columns.map(col) ++ (0 until columns.size  map (i => $"tmp".getItem(i).as(columns(i))))
    
    //SE APLICA EL SELECT CREADO AL DATAFRAME
    df_sal1 = df.withColumn("tmp", split($"bigdata_close_date", "-")).select(selectExprs:_*)
}


// COMMAND ----------

// MAGIC %md
// MAGIC ### Creacion de vista 
// MAGIC - Este paso es util si el dataframe de entrada contiene columnas extras a las esperadas

// COMMAND ----------

// // SE REGISTRA EL DATAFRAME COMO TABLA TEMPORAL
// df.createOrReplaceTempView("ing_schema")

// // SE APLICA LA QUERY CREADA Y SE GUARDA EL RESULTADO EN UN DATAFRAME
// val df_sal1 = spark.sql(select_line)

// COMMAND ----------

// MAGIC %md
// MAGIC #### Actualizar tabla delta (con particiones) 
// MAGIC - contiene "partitioned by" dentro de hql

// COMMAND ----------

if (subida_datos.toLowerCase().contains("partitioned by")) {

  if (loadType == "incremental"){   //Si la carga es incremental...
    println("Si es incremental, Hacer Merge")
    mergeData2(nomb_proc, df_sal1, merge_string, true, columns)

  }else{   //Si la carga es full...
    println("Si es full, hacer overwrite")
    df_sal1
    .write
    .format("delta")
    .mode("overwrite")
    .partitionBy(columns: _*)
    .option("partitionOverwriteMode", "dynamic")
    .option("mergeSchema", "true")
    .saveAsTable(nomb_proc)
  }
} 

// COMMAND ----------

// MAGIC %md
// MAGIC #### Actualizar tabla delta (sin particiones) 
// MAGIC - No contiene "partitioned by" dentro de hql

// COMMAND ----------

if ( ! subida_datos.toLowerCase().contains("partitioned by") ){

  if (loadType == "incremental"){ //Si la carga es incremental...
    println("Si es incremental, Hacer Merge")
    mergeData2(nomb_proc, df, merge_string, false, columns)

  }else{ //Si la carga es full...
    println("Si es full, hacer overwrite")
    df
    .write
    .format("delta")
    .mode("overwrite")
    .option("mergeSchema", "true")
    .saveAsTable(nomb_proc)
  }
}

// COMMAND ----------


// DEPRECADO
//optimiza archivos delta
/* val deltaTable = DeltaTable.forPath(spark, path_data)
deltaTable.optimize().executeCompaction() */

// DEPRECADO
//se crea archivo Delta en Landing 
// SE CALCULA LA CANTIDAD DE ARCHIVOS A ESCRIBIR EN HD
/* var numPartitions = numPartitionsCalc(path_landing) */

// display(df)

// COMMAND ----------


//SE CUENTA LA CANTIDAD DE REGISTROS
final_row_count = df.count

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
/* delete_files(path_landing) */
delete_files(path_stage)
