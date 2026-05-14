// Databricks notebook source
import java.sql.{Connection, DriverManager, CallableStatement}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{col, concat, length, lit, when}
import org.slf4j.{Logger, LoggerFactory}
import java.util.Properties
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.util.Properties;

// COMMAND ----------

val exa_server = "10.186.172.77" // smt-scan.tchile.local
val exa_port = "1521"
val exa_servicename = "explota"
val exa_user = "MIGRACION_BIGDATA"
val exa_pswd = "B1gd4t4.2024"
val driver = "oracle.jdbc.driver.OracleDriver"
val url = s"jdbc:oracle:thin:@(DESCRIPTION= (ADDRESS = (PROTOCOL = TCP)(HOST = $exa_server)(PORT = $exa_port)) (CONNECT_DATA = (SERVER = DEDICATED) (SERVICE_NAME = $exa_servicename)))"

// COMMAND ----------

val consulta =  """
    (SELECT * FROM MIGRACION_BIGDATA.INVENTARIO_RED_FO FETCH FIRST 1 ROWS ONLY)
"""


val df_prueba = spark.read
    .format("jdbc")
    .option("driver", driver)
    .option("url", url)
    .option("user", exa_user)
    .option("password", exa_pswd)
    .option("dbtable", consulta)
    .load()

display(df_prueba)


// COMMAND ----------

df_prueba.write.format("parquet").mode("overwrite").save("abfss://bigdata@stbigdatadev02.dfs.core.windows.net/data/test/bastian/archivo")

// COMMAND ----------

display(dbutils.fs.ls("abfss://bigdata@stbigdatadev02.dfs.core.windows.net/data/test/bastian/"))

// COMMAND ----------

val df_prueba2 = spark.read.format("parquet").load("abfss://bigdata@stbigdatadev02.dfs.core.windows.net/data/test/bastian/archivo")
display(df_prueba2)
