// Databricks notebook source
import java.sql.{Connection, DriverManager, CallableStatement}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.slf4j.{Logger, LoggerFactory}
import org.apache.spark.sql.functions.{col, concat, length, lit, when}
import java.util.Properties
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.util.Properties;

// COMMAND ----------

def agregarAlias(columnas: String): String = {
  
/**
 * Toma una cadena de columnas separadas por comas y les agrega el alias "TABLA.".
 * Si una columna contiene una expresión CAST, se asegura de que el alias "TABLA." se agregue correctamente dentro de la expresión CAST. 
 *
 * @param columnas Una cadena de columnas separadas por comas. Tambien acepta "*" 
 * @return Una cadena de columnas con el alias "TABLA." agregado a cada columna incluyendo los CAST.
 */

  columnas
    .split(",")
    .map(_.trim)
    .map { col =>
      if (col.toLowerCase.contains("cast")) {
        val pattern = """CAST\((\w+)\s+AS""".r
        val updatedCol = pattern.replaceAllIn(col, m => s"CAST(TABLA.${m.group(1)} AS")
        s"$updatedCol"
      } else {
        s"TABLA.$col"
      }
    }
    .mkString(", ")
}

// COMMAND ----------

val exa_server = dbutils.widgets.get("exa_server")
val exa_port = dbutils.widgets.get("exa_port")
val exa_servicename = dbutils.widgets.get("exa_servicename")

val exa_user = dbutils.widgets.get("exa_user")
val scope_secreto = dbutils.widgets.get("scope_secreto")
val exa_secreto = dbutils.widgets.get("exa_secreto")


val exa_pswd = dbutils.secrets.get(scope = s"$scope_secreto", key = s"$exa_secreto")

val esquema = dbutils.widgets.get("esquema")
val tabla = dbutils.widgets.get("tabla")
val column_partition = dbutils.widgets.get("column_partition")
val columnas_query = dbutils.widgets.get("columnas_query")

val mappers = dbutils.widgets.get("mappers")
val campo_filtro = dbutils.widgets.get("campo_filtro")
val filtro = dbutils.widgets.get("filtro")

val url = s"jdbc:oracle:thin:@(DESCRIPTION= (ADDRESS = (PROTOCOL = TCP)(HOST = $exa_server)(PORT = $exa_port)) (CONNECT_DATA = (SERVER = DEDICATED) (SERVICE_NAME = $exa_servicename)))"
val driver = "oracle.jdbc.driver.OracleDriver"

// -------------------------------------------------------------------------

val delimiter = dbutils.widgets.get("delimeter")
val header = dbutils.widgets.get("header")

val adls_container = dbutils.widgets.get("adls_container")



val dir_adls_rel = dbutils.widgets.get("dir_adls_rel")
val path_folder_salida = s"${adls_container}${dir_adls_rel}/stage"

// COMMAND ----------

if (column_partition == null) {
  val errorMessage = "La variable column_partition es nula. Deteniendo el flujo de ejecución."
  dbutils.notebook.exit(errorMessage)
}

// COMMAND ----------

val query_base = s"(SELECT ORA_HASH($column_partition, $mappers) AS PKOH, ${agregarAlias(columnas_query)} FROM $esquema.$tabla TABLA WHERE $campo_filtro = $filtro) subquery"
println(query_base)

val numPartitions = 64

// Lectura de los datos usando la consulta con ORA_HASH
val df_consulta = spark.read
    .format("jdbc")
    .option("driver", driver)
    .option("url", url)
    .option("user", exa_user)
    .option("password", exa_pswd)
    .option("dbtable", query_base)  // Usamos la query con ORA_HASH
    .option("numPartitions", numPartitions)
    .option("partitionColumn", "PKOH")  // Particionar por el hash generado
    .option("lowerBound", 0)
    .option("upperBound", numPartitions)  // Ajuste del valor upperBound
    .option("fetchsize", 50000)  // Ajusta según el tamaño de los datos
    .load()

val df_final = df_consulta.drop("PKOH")

df_final.write
    .format("csv")  // Cambiado a CSV
    .option("header", header)  // Añade una fila de encabezado
    .option("timestampFormat", "yyyy-MM-dd HH:mm:ss.SSSSS")
    .option("delimiter", delimiter)  // Definir el delimitador, en este caso una coma
    .mode("overwrite")  // Sobrescribe si el archivo ya existe
    .save(path_folder_salida)  // Guardar en la ruta especificada
