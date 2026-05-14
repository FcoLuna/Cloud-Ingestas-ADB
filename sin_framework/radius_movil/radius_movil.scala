// Databricks notebook source
// MAGIC %python
// MAGIC
// MAGIC dbutils.widgets.text("parsing_in", "", "Parsing In")
// MAGIC parsing_in = dbutils.widgets.get("parsing_in")
// MAGIC
// MAGIC # Ruta del archivo original
// MAGIC #input_path = "abfss://ingestas@stbigdatadev02.dfs.core.windows.net/data/gestion_recursos/inventario_red/accounting_radius_movil/parsing_in/"
// MAGIC
// MAGIC # Nombre archivo
// MAGIC #nombre_archivo = "detail-20241222"
// MAGIC dbutils.widgets.text("nombre_archivo", "", "Nombre Archivo")
// MAGIC nombre_archivo = dbutils.widgets.get("nombre_archivo")
// MAGIC
// MAGIC input_path_file = parsing_in + nombre_archivo
// MAGIC output_path = input_path_file + ".gz"
// MAGIC
// MAGIC # Renombrar el archivo
// MAGIC dbutils.fs.mv(input_path_file, output_path)

// COMMAND ----------

import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions.Window
import io.delta.tables._

// Definir la ruta de entrada
//val parsing_in = "abfss://ingestas@stbigdatadev02.dfs.core.windows.net/data/gestion_recursos/inventario_red/accounting_radius_movil/parsing_in/"

val parsing_in = dbutils.widgets.get("parsing_in")

val nombre_archivo = dbutils.widgets.get("nombre_archivo")

val output_file = dbutils.widgets.get("output_file")

val filename = nombre_archivo + ".gz"
//val nombre_archivo = "detail-20241222.gz"

// Leer el archivo CSV como texto
val df_init = spark.read
  .format("csv")
  .option("header", "false")  // No tiene cabecera
  .option("compression", "gzip")
  .load(parsing_in+filename)
  .withColumn("_c0", regexp_replace(col("_c0"), "^\\t+", ""))

// Asignar un índice a cada fila para poder agrupar más tarde
val df_with_index = df_init.withColumn("row_index", monotonically_increasing_id())

// Detectar cambios en los registros
val df_with_record_start = df_with_index.withColumn(
  "is_record_start", 
  when(col("_c0").rlike("^(Sat|Sun|Mon|Tue|Wed|Thu|Fri).*"), 1).otherwise(0)
)

// Crear un índice de grupo para cada registro
val windowSpec = Window.orderBy("row_index")
val df_with_group = df_with_record_start.withColumn(
  "record_group", 
  sum("is_record_start").over(windowSpec)
)

// Agrupar los registros por `record_group` y concatenar los valores de cada grupo
val df_grouped = df_with_group.groupBy("record_group")
  .agg(collect_list("_c0").as("record_lines"))

// Agregar columna `fecha` desde la primera línea del grupo
val df_with_fecha = df_grouped.withColumn(
  "fecha", 
  when(col("record_lines").isNotNull, expr("record_lines[0]")).otherwise(null)
)

// Paso 1: Extraer claves únicas de "record_lines"
val allKeys = df_grouped
  .selectExpr("explode(record_lines) as line")
  .filter(col("line").contains("="))
  .withColumn("key", trim(split(col("line"), "=")(0)))
  .select("key")
  .distinct()
  .as[String]
  .collect()

// Paso 2: Transformar "record_lines" en columnas con valores asignados
val df_transformed = df_grouped
  .withColumn("record_id", monotonically_increasing_id())
  .select(col("record_group"), col("record_id"), explode(col("record_lines")).as("line"))
  .filter(col("line").contains("="))
  .withColumn("key", trim(split(col("line"), "=")(0)))
  .withColumn("value", trim(split(col("line"), "=")(1)))
  .groupBy("record_group", "record_id")
  .pivot("key", allKeys)
  .agg(first("value"))

// Unir las fechas con el DataFrame transformado
val join_df = df_transformed
  .join(df_with_fecha.select("record_group", "fecha"), "record_group")

// Limpieza y construcción del DataFrame final
val df_no_quotes = join_df.columns.foldLeft(join_df) { (tempDf, colName) =>
  tempDf.withColumn(colName, regexp_replace(col(colName), "\"", ""))
}

val currentDate = current_date()
val currentTimestamp = current_timestamp()
val rawDate = nombre_archivo.split("-").last
val formattedDate = s"${rawDate.substring(0, 4)}-${rawDate.substring(4, 6)}-${rawDate.substring(6, 8)}"
val formattedTimestamp = date_format(currentTimestamp, "yyyyMMddHHmmssSSS")

val pattern = """[A-Za-z]{3}\s[A-Za-z]{3}\s\d{2}\s\d{2}:\d{2}:\d{2}\s\d{4}"""

val df_final = df_no_quotes.select(
  col("fecha").alias("Fecha").cast(StringType),
  col("NAS-IP-Address").alias("NAS_IP_Address").cast(StringType),
  col("NAS-Identifier").alias("NAS_Identifier").cast(StringType),
  col("NAS-Port").alias("NAS_Port").cast(StringType),
  col("Calling-Station-Id").alias("Calling_Station_Id").cast(StringType),
  col("Acct-Status-Type").alias("Acct_Status_Type").cast(StringType),
  col("Framed-IP-Address").alias("Framed_IP_Address").cast(StringType),
  col("Acct-Session-Time").alias("Acct_Session_Time").cast(StringType),
  col("Acct-Input-Octets").alias("Acct_Input_Octets").cast(StringType),
  col("Acct-Output-Octets").alias("Acct_Output_Octets").cast(StringType),
  col("Acct-Input-Packets").alias("Acct_Input_Packets").cast(StringType),
  col("Acct-Output-Packets").alias("Acct_Output_Packets").cast(StringType),
  col("Acct-Terminate-Cause").alias("Acct_Terminate_Cause").cast(StringType),
  col("3GPP-IMSI").alias("3GPP_IMSI").cast(StringType),
  col("3GPP-Charging-ID").alias("3GPP_Charging_ID").cast(StringType),
  col("3GPP-SGSN-Address").alias("3GPP_SGSN_Address").cast(StringType),
  col("3GPP-GGSN-Address").alias("3GPP_GGSN_Address").cast(StringType),
  col("3GPP-IMEISV").alias("3GPP_IMEISV").cast(StringType),
  col("3GPP-User-Location-Info").alias("3GPP_User_Location_Info").cast(StringType),
  col("Event-Timestamp").alias("Event_Timestamp").cast(StringType),
  col("Timestamp").alias("Timestamp_Unix").cast(StringType))
  .withColumn("year", regexp_extract(col("Fecha"), "([0-9]{4})$", 1))
  .withColumn("month", regexp_extract(col("Fecha"), "\\s([A-Za-z]+)\\s", 1))
  .withColumn("month", expr(s"CASE WHEN month = 'Jan' THEN '01' " +
  s"WHEN month = 'Feb' THEN '02' WHEN month = 'Mar' THEN '03' " +
  s"WHEN month = 'Apr' THEN '04' WHEN month = 'May' THEN '05' " +
  s"WHEN month = 'Jun' THEN '06' WHEN month = 'Jul' THEN '07' " +
  s"WHEN month = 'Aug' THEN '08' WHEN month = 'Sep' THEN '09' " +
  s"WHEN month = 'Oct' THEN '10' WHEN month = 'Nov' THEN '11' " +
  s"WHEN month = 'Dec' THEN '12' END"))
  .withColumn("day", regexp_extract(col("Fecha"), "\\s([0-9]{2})\\s", 1))
  .withColumn("hour", regexp_extract(col("Fecha"), "\\s([0-9]{2}):", 1))
  .withColumn("bigdata_close_date", to_date(lit(formattedDate), "yyyy-MM-dd"))
  .withColumn("bigdata_ctrl_id", formattedTimestamp.cast("long"))
  .filter($"Fecha".isNotNull)
  .filter(col("Fecha").rlike(pattern))
  .filter($"NAS_IP_Address".isNotNull)

//df_final.show(20, false)

//var output_delta = "abfss://ingestas@stbigdatadev02.dfs.core.windows.net/data/gestion_recursos/inventario_red/accounting_radius_movil/raw/"
//var output_delta = dbutils.widgets.get("output_delta")

// Guardar el DataFrame en formato Delta particionado
df_final.write
  .format("delta")
  .mode("append")  // Cambiar a "append" si no deseas sobrescribir
  .partitionBy("year", "month", "day", "hour")
  .save(output_file)

// COMMAND ----------

val files = dbutils.fs.ls(parsing_in)

// Elimina cada archivo individualmente
files.foreach(file => dbutils.fs.rm(file.path, true))
