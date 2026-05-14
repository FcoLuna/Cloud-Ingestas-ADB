// Databricks notebook source
// Importa las bibliotecas necesarias para Spark y funciones
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DateType
import java.time.LocalDate
import java.time.format.DateTimeFormatter
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.types.{StructType, StructField, StringType}
import org.apache.spark.sql.Column
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.functions.{col, length, regexp_replace, split, when}

// Configuración de la sesión Spark
val spark = SparkSession.builder.appName("Parsing_Catalogo_Celdas_Movil").getOrCreate()

var parsing_in = dbutils.widgets.get("parsing_in")
var parsing_out = dbutils.widgets.get("parsing_out")
var filename = dbutils.widgets.get("nombre_archivo")

//val parsing_in = "abfss://ingestas@stbigdatadev02.dfs.core.windows.net/data/gestion_recursos/inventario_red/siiared/catalogo_celdas_movil/parsing_in/"

//val parsing_out = "abfss://ingestas@stbigdatadev02.dfs.core.windows.net/data/gestion_recursos/inventario_red/siiared/catalogo_celdas_movil/stage/"

//val filename = "catalogo_gis_siiared_20250106.csv"


try {

// Lectura del archivo con delimitador ;
val df1 = spark.read.option("header", "true").option("inferSchema", "false").option("delimiter", ";").csv(parsing_in + filename)

// Cambiar los nombres de las columnas a mayúsculas
val upperCaseDf = df1.columns.foldLeft(df1) { (tempDf, colName) =>
  tempDf.withColumnRenamed(colName, colName.toUpperCase). select("VENDOR","TECHNOLOGY","SITE","NE_NAME","NODE_ID","NODE","CELL_ID","CELL_NAME","NOMBRE","LATITUDE","LONGITUDE","REGION","COMUNA","HEIGHT","ANTENA_FISICA","AZIMUTH","MECH_DOWNTILT","ELEC_DOWNTILT","ANTENNA","BEAM_AZIMUTH","BEAMWIDTH","CLUSTER","LAC_TAC","RAC","SAC","FREQ_BAND","CARRIER","RNC_BSC_ID","RNC_BSC_NAME","BCCH_PSC_PCI","CELL_POWER","PILOT_DBM","TXRXMODE","TIPO_EMPLAZAMIENTO","ZONA_COBERTURA_ESPECIAL","ADMINISTRATIVE_STATE","IP_OYM","IP_SERVICIO","SUPPORT_OWNER","ADDRESS","ESTADO_CELDA")
}

  // Guarda el archivo en la carpeta de salida
  upperCaseDf.repartition(1).write.option("header", "true").option("delimiter", ";").mode("append").csv(parsing_out)

// Listar los archivos en el directorio y Filtrar el archivo que comienza con "part-"
val files = dbutils.fs.ls(parsing_out)
val oldFilePathOption = files.find(file => file.name.startsWith("part-"))

// Si encontramos el archivo, renombrarlo
oldFilePathOption match {
  case Some(oldFile) =>
    val oldFilePath = oldFile.path
    val newFilePath = parsing_out + filename
    // Renombrar el archivo
    dbutils.fs.mv(oldFilePath, newFilePath)
    println(s"El archivo ha sido renombrado a: $filename")

  case None =>
    println("No se encontró ningún archivo que cumpla con el criterio.")
}

} catch {
  case e: Exception =>
    println("[ERROR] " + e)
    throw e
}

println("[INFO] PROCESO TERMINADO")


// COMMAND ----------

val files = dbutils.fs.ls(parsing_in)
files.foreach(file => dbutils.fs.rm(file.path, true))
