# Databricks notebook source
# MAGIC %scala
# MAGIC import java.text.SimpleDateFormat
# MAGIC import java.time.LocalDateTime
# MAGIC import java.util.Date
# MAGIC import java.time.format.DateTimeFormatter
# MAGIC import java.util.concurrent.TimeUnit
# MAGIC // import org.apache.spark.sql.SparkSession
# MAGIC import org.apache.spark.sql.SaveMode
# MAGIC import org.apache.spark.sql.types.StringType
# MAGIC import org.apache.spark.sql.types.StructField
# MAGIC import org.apache.spark.sql.types.StructType
# MAGIC import org.apache.spark.sql.functions._
# MAGIC import spark.implicits._
# MAGIC import org.apache.spark.sql.types.DataType

# COMMAND ----------

# MAGIC %scala
# MAGIC def copiarArchivoAbfs (pathOrigen: String, pathDestino: String, recurse:Boolean=true) : Boolean = {
# MAGIC   try
# MAGIC   {
# MAGIC     dbutils.fs.cp(pathOrigen, pathDestino, recurse=recurse)
# MAGIC     return true
# MAGIC   }
# MAGIC   catch
# MAGIC   {
# MAGIC     case e: Exception => e.printStackTrace
# MAGIC       return false
# MAGIC   }
# MAGIC }

# COMMAND ----------

# MAGIC %scala
# MAGIC val pathorigen = "abfss://contenedor-test@stbigdatadev02.dfs.core.windows.net/data/descarga/parque_ott"
# MAGIC val pathdestino = "abfss://bigdata@stbigdatadev02.dfs.core.windows.net/data/descarga/parque_ott"
# MAGIC
# MAGIC copiarArchivoAbfs(pathorigen, pathdestino, true)
