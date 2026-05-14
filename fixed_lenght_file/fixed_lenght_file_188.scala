// Databricks notebook source
import org.apache.spark.sql.{SparkSession, Row}
import org.apache.spark.sql.types.{StructType, StructField, StringType}
import scala.collection.JavaConverters._

val spark = SparkSession.builder 
  .appName("CSV to DataFrame")
  .master("local[*]")
  .getOrCreate()

val fieldLengths = List(18,10,18,8,6,8,1,4,3,4,4,2,2,2,1,1,8,4,8,4,4,4,1,9,9,9,12,12,12,1,8,2,10,10,4,4,16)

val filename1 = dbutils.widgets.get("nombre_archivo")
val filePath = s"abfss://bigdataprd@stbigdataprd02.dfs.core.windows.net/data/trafico/trafico_mediacion/meco/carrier_188_valorizado/stage_tmp/$filename1"


// Leer el contenido del archivo desde ABFSS  
val fileContent = spark.read.textFile(filePath).collect().toList

// Dividir cada línea en campos de longitud fija 
val fixedFields = fileContent.map { line =>
 var start = 0 
 fieldLengths.map { length => val 
 field = line.substring(start, start + length)
  start += length 
  field } 
  } 

// Definir la estructura del DataFrame 
val schema = StructType(fieldLengths.indices.map(i => StructField(s"field_$i", StringType, nullable = true))) 
// Crear filas para el DataFrame 
val rows = fixedFields.map(fields => Row(fields: _*)) 
// Crear el DataFrame utilizando la colección de filas
val javaRows = rows.asJava 
val df = spark.createDataFrame(javaRows, schema)
// Agregar una columna con el nombre del archivo
val dfWithFileName = df.withColumn("file_name", lit(filename1))

val destinationFilePath = "abfss://bigdataprd@stbigdataprd02.dfs.core.windows.net/data/trafico/trafico_mediacion/meco/carrier_188_valorizado/stage"

// Save the DataFrame as a CSV file
val tempPath = destinationFilePath + "/tmp/temp_output"
dfWithFileName.repartition(1).write.option("header", "false").option("delimiter", ";")csv(tempPath)

// Move the file to the desired location in ADLS
val destinationPath = s"abfss://bigdataprd@stbigdataprd02.dfs.core.windows.net/data/trafico/trafico_mediacion/meco/carrier_188_valorizado/stage"


dfWithFileName.show()

// COMMAND ----------

  val files_tmp = dbutils.fs.ls(tempPath).filter(file => file.name.contains("csv"))

  files_tmp.foreach { file_tmp =>
    val nom_tmp = file_tmp.name
    val origen = s"$tempPath/$nom_tmp"
    val destino = s"$destinationPath/$filename1"
    dbutils.fs.mv(origen, destino)
  }
  println(s"Archivo procesado y guardado en: $destinationFilePath")
  dbutils.fs.rm(tempPath, true)
