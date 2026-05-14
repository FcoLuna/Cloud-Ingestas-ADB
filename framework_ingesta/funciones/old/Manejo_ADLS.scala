// Databricks notebook source
import org.apache.spark.sql.SparkSession
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.fs.RemoteIterator
import org.apache.hadoop.fs.LocatedFileStatus
import org.apache.spark.sql.DataFrame

// val serviceCredential: String = dbutils.secrets.get("key-vault-secret-test", "secretDataLakeTCH")

// spark.conf.set("fs.azure.account.auth.type.adlsv2desarrollo.dfs.core.windows.net", "OAuth")
// spark.conf.set("fs.azure.account.oauth.provider.type.adlsv2desarrollo.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
// spark.conf.set("fs.azure.account.oauth2.client.id.adlsv2desarrollo.dfs.core.windows.net", "47519033-2c5b-44b0-8a8e-97fa386b0e87")
// spark.conf.set("fs.azure.account.oauth2.client.secret.adlsv2desarrollo.dfs.core.windows.net", serviceCredential)
// spark.conf.set("fs.azure.account.oauth2.client.endpoint.adlsv2desarrollo.dfs.core.windows.net", "https://login.microsoftonline.com/4e42ae79-d70f-4222-9154-b466e12b00c1/oauth2/token")

// val conf = new Configuration()
// val abfs = FileSystem.get(new java.net.URI("abfss://contenedor-test@stbigdatadev02.dfs.core.windows.net", conf))


// COMMAND ----------

dbutils.fs.help()

// COMMAND ----------

// MAGIC %md
// MAGIC ###Probar existencia de archivos en ADLS

// COMMAND ----------

def exists_file(filepath: String) : Boolean = {
  try
  {
    if(abfs.exists(new Path(filepath)))
    {
      return true
    }else{
      return false
    }
  }
  catch
    { case e: Exception => e.printStackTrace
      return false
    }
}

// COMMAND ----------

// MAGIC %md
// MAGIC ### Probar creación de archivos

// COMMAND ----------

def createNewFile(filepath: String): Boolean = {
  try{
    if(abfs.exists(new Path(filepath))){
      return false
      }
    else{
      abfs.createNewFile(new Path(filepath))
      return true
      }
  }
  catch {
    case e: Exception => e.printStackTrace
      return false
  }
}

// COMMAND ----------

// println(createNewFile("/mnt/flightdata-TestKeyVault/2.txt"))

// COMMAND ----------

// MAGIC %md
// MAGIC ### Probar eliminación de archivo

// COMMAND ----------

def delete(filepath: String) : Boolean = {
  try
  {
    if(abfs.exists(new Path(filepath)))
    {
      abfs.delete(new Path(filepath), true)
      return true
    }else{
      return false
    }
  }
  catch
    { case e: Exception => e.printStackTrace
      return false
    }
}

// COMMAND ----------

// println(delete("/mnt/flightdata-TestKeyVault/2.txt"))

// COMMAND ----------

// MAGIC %md
// MAGIC ### Probar creación directorios

// COMMAND ----------

def mkdir(dirPath: String) : Boolean = {
  try{
    abfs.mkdirs(new Path(dirPath))
    return true
  }
  catch {
    case e: Exception => e.printStackTrace
      return false
  }
}

// COMMAND ----------

// println(mkdir("/mnt/flightdata-TestKeyVault/dirtest"))

// COMMAND ----------

// MAGIC %md
// MAGIC ### Probar listar archivos

// COMMAND ----------

def listFiles (abfsPath: String) : RemoteIterator[LocatedFileStatus] = {
  try
  {
    return abfs.listFiles(new Path(abfsPath), false)
  }
  catch
  {
    case e: Exception => e.printStackTrace
      return null
  }
}

// COMMAND ----------

// println(listFiles("/mnt/flightdata-TestKeyVault/test_ingesta_simple/raw/year=2023"))

// COMMAND ----------

def findRawFile(remoteIterator: RemoteIterator[LocatedFileStatus]): String = {
  var result: String = null
  while (remoteIterator.hasNext()) {
    val file = remoteIterator.next()
    if (file.getPath.getName.startsWith("raw")){
      result = file.getPath.getName
      return result
    }
  }
  return result
}

// COMMAND ----------

// println(findRawFile(listFiles("/mnt/flightdata-TestKeyVault/test_ingesta_simple/raw/year=2023")))

// COMMAND ----------

// MAGIC %md
// MAGIC ### Probar mover archivos

// COMMAND ----------

def moverArchivoAbfs (pathOrigen: String, pathDestino: String) : Boolean = {
  try
  {
    abfs.rename(new Path(pathOrigen), new Path(pathDestino))
    return true
  }
  catch
  {
    case e: Exception => e.printStackTrace
      return false
  }
}

// COMMAND ----------

// println(moverArchivoAbfs("/mnt/flightdata-TestKeyVault/1.txt", "/mnt/flightdata-TestKeyVault/dirtest/1.txt"))

// COMMAND ----------

// MAGIC %md
// MAGIC ### Probar lectura de archivos

// COMMAND ----------

def openFile(filepath: String): String = {
  try {
    // Leer el archivo como un DataFrame
    val df = spark.read.textFile(filepath)

    // Convertir el DataFrame a una cadena
    val text = df.collect().mkString("\n")
    
    return text
  } catch {
    case e: Exception =>
      e.printStackTrace()
      return ""
  }
}

// COMMAND ----------

// println(openFile("/mnt/flightdata/dirtest/package.json"))

// COMMAND ----------

// MAGIC %md
// MAGIC ### Función que busca archivos que comiencen con "raw.." o "noraw.." dependiendo del dataType

// COMMAND ----------

def findRawFile(remoteIterator: RemoteIterator[LocatedFileStatus]): String = {
  var result: String = null
  while (remoteIterator.hasNext()) {
    val file = remoteIterator.next()
    if (file.getPath.getName.startsWith("raw")){
      result = file.getPath.getName()
      return result
    }
  }
  return result
}

def findFile(remoteIterator: RemoteIterator[LocatedFileStatus], dataType:String): String = {
  var result: String = null
  while (remoteIterator.hasNext()) {
    val file = remoteIterator.next()
    if (file.getPath.getName.startsWith(dataType)){
      result = file.getPath.getName()
      return result
    }
  }
  return result
}

// COMMAND ----------

// MAGIC %md
// MAGIC ### Probar guardado de DF a archivo de texto

// COMMAND ----------

def guardarArchivoTexto (dataframe: DataFrame, pathDestino: String) : Boolean = {
  try
  {
    dataframe.write.save(pathDestino)
    return true
  }
  catch
  {
    case e: Exception => e.printStackTrace
      return false
  }
}

// COMMAND ----------

// val flightDF = spark.read.format("parquet").options(Map("header" -> "true", "inferschema" -> "true")).load("/mnt/flightdata/parquet/flights")
// val flightDF = spark.read.format("csv").options(Map("header" -> "true", "inferschema" -> "true")).load("/mnt/flightdata-TestKeyVault/On_Time_Reporting_Carrier_On_Time_Performance_(1987_present)_2016_1.csv")
// display(flightDF)

// COMMAND ----------

// println(guardarArchivoTexto(flightDF, "/mnt/flightdata-TestKeyVault/df-txt"))

// COMMAND ----------

// MAGIC %md
// MAGIC ### Probar guardado de DF a JSON

// COMMAND ----------

def guardarArchivoJSON (dataframe: DataFrame, pathDestino: String) : Boolean = {
  try
  {
    dataframe.write.format("json").save(pathDestino)
  //      dataframe.toJSON.saveAsTextFile(pathDestino)
 
    return true
  }
  catch
  {
    case e: Exception => e.printStackTrace
      return false
  }
}

// COMMAND ----------

// println(guardarArchivoJSON(flightDF, "/mnt/flightdata-TestKeyVault/df-json"))

// COMMAND ----------



// COMMAND ----------

// actualizada
def listFiles (abfsPath: String) : Seq[String] = {
  try
  {
    return dbutils.fs.ls(abfsPath).map(_.name)
  }
  catch
  {
    case e: Exception => e.printStackTrace
      return null
  }
}

// COMMAND ----------

val status = dbutils.fs.ls("abfss://contenedor-test@stbigdatadev02.dfs.core.windows.net/data/interacciones/asignacion_ofertas/usuario_mkt/activacion_disney_fija/landing/")
// for (i <- 0 to status.length-1){
//   val fileStatus = status(i)
//   if (fileStatus.isDir) {
//     println(fileStatus)
//   }
//   // println(fileStatus)
// }
// println(status)

if (status != null && status.length > 0) {
  println("hola")
}



// COMMAND ----------

val a = dbutils.fs.ls("abfss://contenedor-test@stbigdatadev02.dfs.core.windows.net/data/descarga/parque_ott6/raw/year=2023/month=12/")
val b = a.map(_.size).sum
println(b)

// COMMAND ----------

import java.text.SimpleDateFormat
import java.time.LocalDateTime
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

val schema_data = DataType.fromJson(openFile("abfss://contenedor-test@stbigdatadev02.dfs.core.windows.net/data/descarga/parque_ott_DELTA/parque_ott_DELTA.json")).asInstanceOf[StructType]
val a = spark.read.option("delimiter", ",").option("header", false).option("mode", "FAILFAST").option("DateFormat", "yyyy-MM-dd").option("timestampFormat", "yyyy-MM-dd HH|mm|ss").csv("abfss://contenedor-test@stbigdatadev02.dfs.core.windows.net/data/descarga/parque_ott_DELTA/stage/parqueOTT_20231209.csv")
display(a)

// COMMAND ----------

dbutils.fs.head("abfss://contenedor-test@stbigdatadev02.dfs.core.windows.net/data/descarga/parque_ott/parque_ott.hql")

// COMMAND ----------

val hql = """create external table if not exists devtmp.raw_producto_asignado.parque_ott
(
productname string,
status string,
contractdate string,
bocode string,
utx string,
rut string,
dv string,
email string,
idservice string,
alias string,
access_id string,
canceldate string,
bigdata_close_date date,
bigdata_ctrl_id bigint
)
partitioned by (year string, month string)
location 'abfss://bigdata@stbigdatadev02.dfs.core.windows.net/data/descarga/parque_ott/raw'"""



// COMMAND ----------

dbutils.fs.mkdirs("abfss://bigdata@stbigdatadev02.dfs.core.windows.net/data/interacciones/campanas/mkthub/mh_oferta_recambio_equipo10/raw")

// COMMAND ----------

dbutils.fs.help()

// COMMAND ----------

dbutils.fs.rm("abfss://bigdata@stbigdatadev02.dfs.core.windows.net/data/interacciones/campanas/mkthub/mh_oferta_recambio_equipo10/raw", recurse=true)

// COMMAND ----------

dbutils.fs.put("abfss://contenedor-test@stbigdatadev02.dfs.core.windows.net/data/descarga/parque_ott/parque_ott.hql", hql)

// COMMAND ----------

dbutils.fs.cp("abfss://contenedor-test@stbigdatadev02.dfs.core.windows.net/data/descarga/parque_ott/last_ingest_time.txt", "abfss://contenedor-test@stbigdatadev02.dfs.core.windows.net/data/descarga/parque_ott6/last_ingest_time.txt")

// COMMAND ----------

// MAGIC %sql
// MAGIC DELETE FROM devtmp.raw_producto_asignado.parque_ott2
// MAGIC -- DROP TABLE IF EXISTS devtmp.raw_producto_asignado.parque_ott_delta

// COMMAND ----------

spark.sql("MSCK REPAIR TABLE devtmp.raw_producto_asignado.parque_ott SYNC METADATA")

// COMMAND ----------

// spark.sql("CONVERT TO DELTA parquet.`abfss://contenedor-test@stbigdatadev02.dfs.core.windows.net/data/descarga/parque_ott/raw/` PARTITIONED BY (year string, month string)")
spark.sql("CONVERT TO DELTA devtmp.raw_producto_asignado.parque_ott6")

// COMMAND ----------

val old_path = "abfss://contenedor-test@stbigdatadev02.dfs.core.windows.net/data/descarga/parque_ott5/raw/year=2023/month=12/raw.PARQUE_SUBSERVICIO_AGREGADO.producto_asginado.f.d.20231221123216.N3.snappy.parquet"
val new_path = "abfss://contenedor-test@stbigdatadev02.dfs.core.windows.net/data/descarga/parque_ott5/raw/year=2023/month=12/HOLAAA.producto_asginado.f.d.20231221123216.N3.snappy.parquet"
mssparkutils.fs.help()

// COMMAND ----------

val a = dbutils.fs.ls("abfss://bigdata@stbigdatadev02.dfs.core.windows.net/data/descarga/parque_ott/stage")
display(a)

// COMMAND ----------

dbutils.fs.mv("abfss://contenedor-test@stbigdatadev02.dfs.core.windows.net/data/descarga/hola/part-00000-tid-5365584966458864861-b5683bd8-c955-49eb-bab6-604b65126e50-1738-1-c000.csv", "abfss://contenedor-test@stbigdatadev02.dfs.core.windows.net/data/descarga/hola/part-00000-tid-5690062097693957526-155f323a-e039-4bd2-9ef3-143ef3111727-1741-1-c000.csv")

// COMMAND ----------

println(a.map(_.size).sum)

// COMMAND ----------

val a = spark.sql("select count(*) from devtmp.raw_producto_asignado.parque_ott")
display(a)

// COMMAND ----------

val dfff = spark.sql("select * from devtmp.raw_producto_asignado.parque_ott")
display(dfff)

// COMMAND ----------

var columns = Seq.empty[String]
columns = columns :+ "year"
columns = columns :+ "month"

// dfff.write.partitionBy(columns: _*).format("delta").mode("overwrite").option("partitionOverwriteMode", "dynamic").save("abfss://contenedor-test@stbigdatadev02.dfs.core.windows.net/data/descarga/parque_ott/raw")
dfff.coalesce(1).write.partitionBy(columns: _*).format("delta").mode("append").save("abfss://contenedor-test@stbigdatadev02.dfs.core.windows.net/data/descarga/parque_ott/raw")
 

// COMMAND ----------

val a = 5
if (a == 5){
  throw new Exception("Te equivocaste")
}

// COMMAND ----------

// MAGIC %python
// MAGIC from pyspark.sql.types import StructType,StructField, StringType, IntegerType
// MAGIC data2 = [("James","","Smith","36636","M",3000),
// MAGIC     ("Michael","Rose","","40288","M",4000),
// MAGIC     ("Robert","","Williams","42114","M",4000),
// MAGIC     ("Maria","Anne","Jones","39192","F",4000),
// MAGIC     ("Jen","Mary","Brown","","F",-1)
// MAGIC   ]
// MAGIC
// MAGIC schema = StructType([ \
// MAGIC     StructField("firstname",StringType(),True), \
// MAGIC     StructField("middlename",StringType(),True), \
// MAGIC     StructField("lastname",StringType(),True), \
// MAGIC     StructField("id", StringType(), True), \
// MAGIC     StructField("gender", StringType(), True), \
// MAGIC     StructField("salary", IntegerType(), True) \
// MAGIC   ])
// MAGIC  
// MAGIC df = spark.createDataFrame(data=data2,schema=schema)
// MAGIC df.printSchema()
// MAGIC display(df)

// COMMAND ----------

// MAGIC %python
// MAGIC df2 = df.withColumn("test", lit("hola"))
// MAGIC display(df2)

// COMMAND ----------

// MAGIC %python
// MAGIC df.coalesce(1).write.format("csv").mode("append").save("abfss://contenedor-test@stbigdatadev02.dfs.core.windows.net/data/descarga/hola/", header=True)

// COMMAND ----------

// MAGIC %python
// MAGIC df = spark.read.option("header", True).csv(f"abfss://contenedor-test@stbigdatadev02.dfs.core.windows.net/data/gss_feedback_callout/landing/2023/12/Feedback_GSS_20231216.csv")
// MAGIC df.printSchema()
// MAGIC display(df)

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
import org.apache.spark.sql.types.DataType

// COMMAND ----------

val schema_data = DataType.fromJson(openFile("abfss://contenedor-test@stbigdatadev02.dfs.core.windows.net/data/gss_feedback_callout/gss_feedback_callout.json")).asInstanceOf[StructType]
println("[INFO] schema_data " + schema_data)

val df = spark.read.option("delimiter", ";").option("header", true).option("quote","\"").option("mode","FAILFAST").option("encoding", "UTF-8").csv(f"abfss://contenedor-test@stbigdatadev02.dfs.core.windows.net/data/gss_feedback_callout/landing/Feedback_GSS_20231216.csv")
println(df.schema)
display(df)

// COMMAND ----------

import java.time.temporal.ChronoUnit
val fecha_1 = "20231228 132425"
val fecha_2 = "20231228 132431"

val format_actual = new java.text.SimpleDateFormat("yyyyMMdd HHmmss")
val fecha_1_date = format_actual.parse(fecha_1)
val fecha_2_date = format_actual.parse(fecha_2)

val a = ((fecha_2_date.getTime() - fecha_1_date.getTime())/1000).toString()
println(a)

// COMMAND ----------

import io.delta.tables._
val deltaTable = DeltaTable.forPath(spark, "abfss://contenedor-test@stbigdatadev02.dfs.core.windows.net/data/descarga/parque_ott/raw/")
val snapshot = deltaTable.detail
display(snapshot)

// COMMAND ----------

val c = "abfss://contenedor-test@stbigdatadev02.dfs.core.windows.net/data/descarga/parque_ott/raw/"
val a = spark.sql("DESCRIBE DETAIL '" + c + "'").take(1)(0).getAs[Long]("sizeInBytes")
// val b = a.take(1)(0)
// println(b.getAs[Integer]("numFiles"))
println(a)

// COMMAND ----------

val a = dbutils.fs.ls("abfss://bigdata@stbigdatadev02.dfs.core.windows.net/datae")
// val b = a.map(_.modificationTime).max
// val c = a.filter(_.modificationTime == b)
// println(c.map(_.size).sum)
// println(c.size)
display(a)

// COMMAND ----------

import java.util.Date
import java.util.TimeZone
import java.text.SimpleDateFormat
import java.time.format.DateTimeFormatter
import java.time.LocalDateTime
val current = new Date().getTime
val formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
formatter.setTimeZone(TimeZone.getTimeZone("Chile/Continental"))
val starttime_spark = formatter.format(new Date())
println(starttime_spark)

// COMMAND ----------

val a = dbutils.fs.ls("abfss://bigdata@stbigdatadev02.dfs.core.windows.net/data/interacciones/campanas/mkthub/mh_oferta_recambio_equipo10/raw")
display(a)

// COMMAND ----------

val b = spark.read.option("delimiter", ";").option("header", false).option("DateFormat", "d/MM/yyyy").option("timestampFormat","yyyy-MM-dd HH:mm:ss").parquet("abfss://contenedor-test@stbigdatadev02.dfs.core.windows.net/data/gss_feedback_callout/raw/year=2023/month=10/nifi/raw.campanas.trafico.i.d.20231013030533.N0.snappy.parquet")
display(b)

// COMMAND ----------

display(b.filter("num_celular = '56999808770'"))

// COMMAND ----------

val a = dbutils.fs.ls("abfss://bigdata@stbigdatadev02.dfs.core.windows.net/modelos/ods/ar_charge/full_ar_charge/stage")
display(a)

// COMMAND ----------

val df = spark.read.parquet(f"abfss://contenedor-test@stbigdatadev02.dfs.core.windows.net/data/finanzas/facturacion/kfactor/ingreso3/stage/DM_INGRESO.parquet")
display(df)
