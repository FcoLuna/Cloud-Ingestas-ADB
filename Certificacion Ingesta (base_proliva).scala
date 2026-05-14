// Databricks notebook source
// DBTITLE 1,validarEstructuras
import org.apache.spark.sql.{DataFrame, SparkSession}
import spark.implicits._

// Función para agregar un índice secuencial a un DataFrame
def addSequentialIndex(df: DataFrame, indexName: String): DataFrame = {
  val indexedDF = df.withColumn(indexName, monotonically_increasing_id())
  indexedDF
}
// Función para obtener el esquema de una tabla en el catálogo de Databricks
def getSchemaFromTable(spark: SparkSession, tableName: String): DataFrame = {
  val df = spark.table(tableName)
  val schema = df.schema
  val fields = schema.fields.map(field => (field.name, field.dataType.simpleString))
  val dfWithSchema = spark.createDataFrame(fields).toDF("nombre_col_azure", "tipo_col_azure")
  addSequentialIndex(dfWithSchema, "IndexAzure")
}

// Función para obtener el esquema de un archivo Parquet
def getSchemaFromParquet(spark: SparkSession, parquetDirectory: String): DataFrame = {
  val df = spark.read.parquet(parquetDirectory)
  val schema = df.schema
  val fields = schema.fields.map(field => (field.name, field.dataType.simpleString))
  val dfWithSchema = spark.createDataFrame(fields).toDF("nombre_col_parquet", "tipo_col_parquet")
  addSequentialIndex(dfWithSchema, "IndexParquet")
}

// Función principal que despliega los esquemas de ambas fuentes de datos en un solo DataFrame
def validarEstructuras(tablaAzure: String, parquetDirOnPrem: String): Unit = {
  getSchemaFromTable(spark, tablaAzure).createOrReplaceTempView("VW_TABLA_AZURE")
  getSchemaFromParquet(spark, parquetDirOnPrem).createOrReplaceTempView("VW_PARQUE_DIR_ON_PREM")

  // Usar `full_outer` join con índices para mantener el orden y completar los valores faltantes
val resultadoDF = spark.sql(
  s"""
     SELECT
       t.nombre_col_azure,
       t.tipo_col_azure,
       p.nombre_col_parquet,
       p.tipo_col_parquet,
       CASE WHEN t.nombre_col_azure =  p.nombre_col_parquet then 'OK' ELSE 'ERROR!!' END AS col_iguales,
       CASE WHEN t.tipo_col_azure =  p.tipo_col_parquet then 'OK' ELSE 'ERROR!!' END AS tipos_iguales
     FROM
       VW_TABLA_AZURE t
     FULL OUTER JOIN
       VW_PARQUE_DIR_ON_PREM p
     ON
       t.IndexAzure = p.IndexParquet
       order by t.IndexAzure, p.IndexParquet
   """.stripMargin)

resultadoDF.show(numRows = Int.MaxValue, truncate = false)
}

// COMMAND ----------

// DBTITLE 1,Validar estrcutura CSV
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

// La función para agregar un índice secuencial a un DataFrame
def addSequentialIndex(df: DataFrame, indexName: String): DataFrame = {
  df.withColumn(indexName, monotonically_increasing_id())
}

// Función para obtener el esquema de un archivo CSV
def getSchemaFromCSV(spark: SparkSession, csvFilePath: String): DataFrame = {
  val df = spark.read.option("header", "true").csv(csvFilePath)
  val schema = df.schema
  val fields = schema.fields.map(field => (field.name, field.dataType.simpleString))
  val dfWithSchema = spark.createDataFrame(fields).toDF("nombre_col_csv", "tipo_col_csv")
  addSequentialIndex(dfWithSchema, "IndexCSV")
}

// Función para obtener el esquema de una tabla en el catálogo de Databricks
def getSchemaFromTable(spark: SparkSession, tableName: String): DataFrame = {
  val df = spark.table(tableName)
  val schema = df.schema
  val fields = schema.fields.map(field => (field.name, field.dataType.simpleString))
  val dfWithSchema = spark.createDataFrame(fields).toDF("nombre_col_azure", "tipo_col_azure")
  addSequentialIndex(dfWithSchema, "IndexAzure")
}

// Función para obtener el esquema de un archivo Parquet
def getSchemaFromParquet(spark: SparkSession, parquetDirectory: String): DataFrame = {
  val df = spark.read.parquet(parquetDirectory)
  val schema = df.schema
  val fields = schema.fields.map(field => (field.name, field.dataType.simpleString))
  val dfWithSchema = spark.createDataFrame(fields).toDF("nombre_col_parquet", "tipo_col_parquet")
  addSequentialIndex(dfWithSchema, "IndexParquet")
}

// Función principal que despliega los esquemas de ambas fuentes de datos en un solo DataFrame
def validarEstructuras(tablaAzure: String, parquetDirOnPrem: String, csvFilePath: String): Unit = {
  getSchemaFromTable(spark, tablaAzure).createOrReplaceTempView("VW_TABLA_AZURE")
  getSchemaFromParquet(spark, parquetDirOnPrem).createOrReplaceTempView("VW_PARQUE_DIR_ON_PREM")
  getSchemaFromCSV(spark, csvFilePath).createOrReplaceTempView("VW_CSV")

  // Esta condición es aleatoria ya que tus condiciones de unión han de ser adecuadas a la lógica de tu aplicación
  val joinCondition = "0 = 0"

  val resultadoDF = spark.sql(
    s"""
       SELECT
         t.nombre_col_azure,
         t.tipo_col_azure,
         p.nombre_col_parquet,
         p.tipo_col_parquet,
         c.nombre_col_csv,
         c.tipo_col_csv,
         CASE 
           WHEN t.nombre_col_azure = p.nombre_col_parquet THEN 'OK' 
           ELSE 'ERROR!!' 
         END AS col_iguales_tabla_parquet,
         CASE 
           WHEN t.tipo_col_azure = p.tipo_col_parquet THEN 'OK' 
           ELSE 'ERROR!!' 
         END AS tipos_iguales_tabla_parquet,
         CASE 
           WHEN t.nombre_col_azure = c.nombre_col_csv THEN 'OK' 
           ELSE 'ERROR!!' 
         END AS col_iguales_tabla_csv,
         CASE 
           WHEN t.tipo_col_azure = c.tipo_col_csv THEN 'OK' 
           ELSE 'ERROR!!' 
         END AS tipos_iguales_tabla_csv
       FROM
         VW_TABLA_AZURE t
       FULL OUTER JOIN
         VW_PARQUE_DIR_ON_PREM p ON $joinCondition
       FULL OUTER JOIN
         VW_CSV c ON $joinCondition
     """.stripMargin)

  resultadoDF.show(numRows = Int.MaxValue, truncate = false)
}

// COMMAND ----------

// DBTITLE 1,validarCounts
import org.apache.spark.sql.{DataFrame, SparkSession}

def validarCounts(tablaAzure: String, parquetDirOnPrem: String, joinColumns: Seq[String], filtroFecha:String): Unit = {
  
  //spark.table(tablaAzure).createOrReplaceTempView("tabla_azure")
  // Leer el archivo parquet
  
  val dfOnPrem = spark.read.format("parquet").load(parquetDirOnPrem)
  dfOnPrem.createOrReplaceTempView("tablaOnPrem")

  //val dfOnADL = spark.read.format("delta").load(tablaAzure)
  val dfOnADL = spark.sql(s"select * from ${tablaAzure}")
  dfOnADL.createOrReplaceTempView("tablaOnADL")

  val dfOnPremFiltrado = spark.sql(s"select * from tablaOnPrem ${filtroFecha}")
  dfOnPremFiltrado.createOrReplaceTempView("tablaOnPremFiltrado")
  
  
  val dfAzureFiltrado = spark.sql(s"select * from tablaOnADL ${filtroFecha}")
  dfAzureFiltrado.createOrReplaceTempView("tablaAzureFitlrada")


  val joinCondition = joinColumns.map(col => s"tablaAzureFitlrada.$col = tablaOnPremFiltrado.$col").mkString(" AND ")

  
  println("CONT FILAS AZURE") 
  val query2 = s""" SELECT  COUNT(*) AS cont_filas_azure FROM tablaAzureFitlrada """
  spark.sql(query2).show(false) 
  println("CONT FILAS ON PREM") 
  val query3 = s""" SELECT  COUNT(*) AS cont_filas_delta FROM tablaOnPremFiltrado """
  spark.sql(query3).show(false) 
  /* 
  val query = s"""
      SELECT 
        SUM(CASE WHEN tablaAzureFitlrada.${joinColumns.head} IS NULL THEN 1 ELSE 0 END) AS solo_on_prem,
        SUM(CASE WHEN tablaOnPremFiltrado.${joinColumns.head} IS NULL THEN 1 ELSE 0 END) AS solo_en_azure,
        SUM(CASE WHEN tablaAzureFitlrada.${joinColumns.head} IS NOT NULL AND tablaOnPremFiltrado.${joinColumns.head} IS NOT NULL THEN 1 ELSE 0 END) AS en_ambos    
      FROM tablaAzureFitlrada
      FULL JOIN tablaOnPremFiltrado
      ON $joinCondition
  """ 
  println(query) 
  spark.sql(query).show(false)  */ 
}

// COMMAND ----------

// DBTITLE 1,Validar Count CSV
import org.apache.spark.sql.{DataFrame, SparkSession}

def validarCounts(tablaAzure: String, csvFilePath: String, joinColumns: Seq[String], filtroFecha: String): Unit = {
  
  // Leer el archivo CSV
  val dfCSV = spark.read.option("header", "true").csv(csvFilePath)
  dfCSV.createOrReplaceTempView("tablaCSV")

  // Cargar la tabla de Azure
  val dfOnADL = spark.sql(s"SELECT * FROM ${tablaAzure}")
  dfOnADL.createOrReplaceTempView("tablaOnADL")

  // Filtrar la tabla CSV basado en el filtro de fecha (supone que hay una columna de fecha)
  val dfCSVFiltrado = spark.sql(s"SELECT * FROM tablaCSV WHERE ${filtroFecha}")
  dfCSVFiltrado.createOrReplaceTempView("tablaCSVFiltrada")
  
  // Filtrar la tabla de Azure también
  val dfAzureFiltrado = spark.sql(s"SELECT * FROM tablaOnADL WHERE ${filtroFecha}")
  dfAzureFiltrado.createOrReplaceTempView("tablaAzureFiltrada")

  // Condiciones de unión basadas en los nombres de columnas proporcionados
  val joinCondition = joinColumns.map(col => s"tablaAzureFiltrada.$col = tablaCSVFiltrada.$col").mkString(" AND ")

  println("CONT FILAS AZURE") 
  val query2 = s""" SELECT COUNT(*) AS cont_filas_azure FROM tablaAzureFiltrada """
  spark.sql(query2).show(false) 
  
  println("CONT FILAS CSV") 
  val query3 = s""" SELECT COUNT(*) AS cont_filas_csv FROM tablaCSVFiltrada """
  spark.sql(query3).show(false)

  // Consulta de conteo de filas en ambas tablas con condiciones
  val query = s"""
      SELECT 
        SUM(CASE WHEN tablaAzureFiltrada.${joinColumns.head} IS NULL THEN 1 ELSE 0 END) AS solo_en_csv,
        SUM(CASE WHEN tablaCSVFiltrada.${joinColumns.head} IS NULL THEN 1 ELSE 0 END) AS solo_en_azure,
        SUM(CASE WHEN tablaAzureFiltrada.${joinColumns.head} IS NOT NULL AND tablaCSVFiltrada.${joinColumns.head} IS NOT NULL THEN 1 ELSE 0 END) AS en_ambos    
      FROM tablaAzureFiltrada
      FULL JOIN tablaCSVFiltrada
      ON $joinCondition
  """ 
  println(query) 
  spark.sql(query).show(false) 
}

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT DISTINCT (day) FROM `bi_fibra-e2e`.visualizacion.perf_hogar_fo_reinicios

// COMMAND ----------

// DBTITLE 1,Definir las variables
val tablaDelta = "`bi_ingestas`.raw_interacciones.param_canales_campanas"
var rutaOnPrem = "abfss://bigdataprd@stbigdataprd02.dfs.core.windows.net/data/interacciones/campanas/ma/param_canales_campanas/landing2024/raw.dgo.vw_param_canales_campanas.f.d.csv"
display(dbutils.fs.ls(rutaOnPrem))


val filtroFecha = "" // "WHERE fecha_periodo='202407'"

// COMMAND ----------

// DBTITLE 1,Certificar todo
println("validarEstructuras")
validarEstructuras(tablaDelta, rutaOnPrem)

println("validarCounts")
validarCounts(tablaDelta, rutaOnPrem, joinCols, filtroFecha)

// COMMAND ----------

// DBTITLE 1,Base Parquet
def getDirParquetOnPrem(Dir: String): String = {
  val basePath = "abfss://bigdataprd@stbigdataprd02.dfs.core.windows.net"
  s"$basePath$Dir"
}

// COMMAND ----------

// DBTITLE 1,Validación Q
def countsParticiones(tablaAzure: String, parquetDirOnPrem: String, Particiones: Seq[String], fechaColumna: Option[String] = None): Unit = {

println(s"""#################VALIDANDO Q "$tablaAzure" ###########################\n""")

val DirOnPrem = getDirParquetOnPrem(parquetDirOnPrem)

// Cambiar el formato de la fecha
val columns = Particiones.map(col) ++ fechaColumna.map(colum => date_format(col(colum), "yyyy-MM-dd").as(colum)).toSeq

// Leer y formatear las fechas en el DataFrame de parquet
val dfOnPrem = spark.read.parquet(DirOnPrem)
val dfOnPremFormatted = dfOnPrem.withColumn("fecha_solicitud", date_format(col("fecha_solicitud"), "yyyy-MM-dd"))

// Leer y formatear las fechas en la tabla de Azure
val dfAzure = spark.table(tablaAzure)
val dfAzureFormatted = dfAzure.withColumn("fecha_solicitud", date_format(col("fecha_solicitud"), "yyyy-MM-dd"))

// Agrupar y contar las particiones
val qOnPrem = dfOnPremFormatted.groupBy(columns: _*).agg(count("*").as("qOnPrem"))
val qfAzure = dfAzureFormatted.groupBy(columns: _*).agg(count("*").as("qAzure"))

qOnPrem.createOrReplaceTempView("qOnPrem")
qfAzure.createOrReplaceTempView("qfAzure")

val joinCondition = (Particiones.map(col => s"qfAzure.$col = qOnPrem.$col") ++ fechaColumna.map(col => s"qfAzure.$col = qOnPrem.$col")).mkString(" AND ")
val selectClause = (Particiones.map(col => s"nvl(qOnPrem.$col, qfAzure.$col) as $col") ++ fechaColumna.map(col => s"nvl(qOnPrem.$col, qfAzure.$col) as $col")).mkString(", ")

val query =
s"""
SELECT 
$selectClause,
qOnPrem.qOnPrem as Q_OnPrem,
qfAzure.qAzure as Q_Azure,
CASE WHEN qfAzure.qAzure = qOnPrem.qOnPrem THEN 'OK' ELSE 'ERROR' END as status
FROM qOnPrem
FULL JOIN qfAzure
ON $joinCondition
"""
spark.sql(query).show(numRows = Int.MaxValue, truncate = false)
}


// COMMAND ----------

// DBTITLE 1,Ejecución Individual
countsParticiones(
"bidesarrollo.interacciones.magento_solic_recambio_equipo",
"/modelos/interacciones/magento_solic_recambio_equipo/landing/",
Seq("fecha_solicitud"),
// Some("fecha_solicitud") // Descomenta esta línea si necesitas incluir la columna de fecha
)
