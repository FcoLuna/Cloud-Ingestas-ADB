// Databricks notebook source
import org.apache.spark.sql.{DataFrame, Row, Column}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions.{col, format_number}

// COMMAND ----------

val delimitador = dbutils.widgets.get("delimitador")
val encabezado = dbutils.widgets.get("encabezado")
val path_stage = dbutils.widgets.get("path_stage")
val filename = dbutils.widgets.get("filename")

// COMMAND ----------

// MAGIC %run /Workspace/Repos/ingestas/Cloud-Ingestas-ADB/framework_ingesta/funciones/Manejo_ADLS

// COMMAND ----------

def defineSchemaJson(pathJson: String, path_df: String, delimeter: String, quote: String, timestampFormat: String, escape: String): DataFrame = {
  
  val header = "True"

  val schema_data = DataType.fromJson(openFile(pathJson)).asInstanceOf[StructType]

  // Cargar el DataFrame con las nuevas opciones
  val df = spark.read.format("csv")
    .option("header", header)
    .option("multiline", "true")
    .option("delimiter", delimeter)
    .option("quote", quote)
    .option("timestampFormat", timestampFormat) 
    .option("escape", escape) 
    .schema(schema_data)
    .load(path_df)
  
  return df
}

// COMMAND ----------

val files_and_folders = dbutils.fs.ls(path_stage)

// COMMAND ----------

val df = spark.read
  .option("inferSchema", "false")
  .option("header", encabezado)
  .option("delimiter", delimitador)
  .csv(path_stage)
  .dropDuplicates()

// COMMAND ----------

display(df)

// COMMAND ----------

// Columns to be fixed
val columnsToFix = Seq(
  "ds_attenuation", "us_attenuation", "ds_rx_power", "us_rx_power",
  "ds_change_from_baseline", "us_change_from_baseline",
  "average_retrain", "short_term_average_retrain"
)

// Function to round values to 2 decimal places and keep as string
def roundTo3Decimals(df: DataFrame, columns: Seq[String]): DataFrame = {
  columns.foldLeft(df) { (tempDf, colName) =>
    tempDf.withColumn(colName, format_number(col(colName).cast("double"), 3))
  }
}

// Apply the function to the specified columns
val resultDf = roundTo3Decimals(df, columnsToFix)

// COMMAND ----------

display(resultDf)

// COMMAND ----------

resultDf.repartition(1).write.mode("overwrite").option("header", encabezado).option("delimiter", delimitador).csv(path_stage)

// COMMAND ----------

val lista_archivos = dbutils.fs.ls(path_stage)
for(elemento <- lista_archivos) {
  if(!(elemento.path).endsWith(".csv")) {
    dbutils.fs.rm(elemento.path)
  }
}

