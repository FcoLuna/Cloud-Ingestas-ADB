// Databricks notebook source
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import java.nio.file.{Files, Paths}
import java.nio.charset.StandardCharsets
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField
import spark.implicits._
import scala.collection.JavaConverters._
import org.apache.spark.sql.expressions.Window

// COMMAND ----------

// dbutils.widgets.text("path_archivo","abfss://ingestas@stbigdatadev02.dfs.core.windows.net/data/interacciones/ordenes/oap/det_solicitudes_rechazadas/stage")
// dbutils.widgets.text("nombre_archivo","DetallesSolicitudesRechazadas_14012025_0606.csv")
// dbutils.widgets.text("codificacion","ISO-8859-1")
// dbutils.widgets.text("skip_lines","2")
// dbutils.widgets.text("delimiter_columnas",",")
// dbutils.widgets.text("inicio_parrafo","Codigos de Respuestas")
// dbutils.widgets.text("fin_parrafo","RECHAZADO PORT-IN Detalles de la solicitud PORT - Rechazado por TN")

// COMMAND ----------

val path_archivo = dbutils.widgets.get("path_archivo")
val nombre_archivo = dbutils.widgets.get("nombre_archivo")
val codificacion = dbutils.widgets.get("codificacion")
val skipLines = dbutils.widgets.get("skip_lines").toInt
val delimiter_columnas = dbutils.widgets.get("delimiter_columnas")
val inicio_parrafo = dbutils.widgets.get("inicio_parrafo")
val fin_parrafo = dbutils.widgets.get("fin_parrafo")

var status_ejecucion = 0
var desc_status_ejecucion = "[OK]"
var status_error: Exception = null

// COMMAND ----------

// MAGIC %md
// MAGIC ### Split del archivo

// COMMAND ----------


try {
  val df = spark.read.option("encoding", codificacion).option("delimiter","\u0000").csv(path_archivo+nombre_archivo)

  // val dfConSeccion = df.withColumn("seccion", when(col("_c0").rlike(inicio_parrafo), true)
  val dfConSeccion = df.withColumn("seccion", when(col("_c0") === inicio_parrafo, true)
    // .when(col("_c0").rlike(fin_parrafo), false)
    .when(col("_c0") === fin_parrafo, false)
    .otherwise(null))

  val windowSpec = Window.orderBy(monotonically_increasing_id()).rowsBetween(Window.unboundedPreceding, 0)
  val dfFinal = dfConSeccion.withColumn("seccion", last("seccion", ignoreNulls = true).over(windowSpec)).filter(col("seccion") === true)

  // Agregar número de fila (rownum)
  val windowRowNum = Window.orderBy(monotonically_increasing_id()) 
  val dfConRowNum = dfFinal.withColumn("rownum", row_number().over(windowRowNum)).filter(col("rownum") > skipLines).drop("seccion","rownum")

  val maxColumnas = dfConRowNum.select(size(split(col("_c0"), delimiter_columnas)).alias("size")).agg(max("size")).as[Int].collect()(0)

  val df_fin = dfConRowNum
  .select(
    (0 until maxColumnas).map(i => split(col("_c0"), delimiter_columnas).getItem(i).alias(s"columna_${i+1}")): _*)

  // display(df_fin)

  val tempDir = path_archivo + "tmp"
  val finalDir = path_archivo + nombre_archivo

  df_fin.coalesce(1).write.option("header", false).mode("overwrite").csv(tempDir)
  val csvFile = dbutils.fs.ls(tempDir).filter(file => file.name.endsWith(".csv"))(0).path
  dbutils.fs.mv(csvFile, finalDir)
  dbutils.fs.rm(tempDir, true)

} catch {
    case e: Exception =>
      status_ejecucion = 1
      desc_status_ejecucion = "[ERROR] " + e
      status_error = e
      println("[ERROR] " + e)
} 
