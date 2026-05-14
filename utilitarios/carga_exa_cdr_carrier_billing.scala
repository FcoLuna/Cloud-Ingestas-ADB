// Databricks notebook source
// MAGIC %md ###Librerias

// COMMAND ----------

dbutils.widgets.text("fecha","20250401")
//dbutils.widgets.text("mes_proceso","0")
//dbutils.widgets.text("tipo_ejecucion","N")
dbutils.widgets.text("adls_path","abfss://bigdataprd@stbigdataprd02.dfs.core.windows.net")
dbutils.widgets.text("catalogo","biproduccion")
//dbutils.widgets.text("nom_proc","CNF_PRD_FIJ_GESTION_PROXIMO_COBRO_INS")

// COMMAND ----------

import java.io.{BufferedReader, FileInputStream, InputStreamReader}
import java.sql.{Connection, DriverManager}
import java.text.SimpleDateFormat
import java.util.Calendar

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.SparkSession

// COMMAND ----------

// MAGIC %run ./funciones

// COMMAND ----------

// MAGIC %md ###Variables y Parametros

// COMMAND ----------

//PARAMETROS QUE SE PASAN DESDE ADF
val fecha    = dbutils.widgets.get("fecha").toInt
//val mes_proceso    = dbutils.widgets.get("mes_proceso").toInt
//val tipo_ejecucion = dbutils.widgets.get("tipo_ejecucion")
val adls_path      = dbutils.widgets.get("adls_path")
val catalogo       = dbutils.widgets.get("catalogo")
//val nombre_proceso = dbutils.widgets.get("nom_proc")

//VARIABLES GENERALES PARA EL PROCESO
val rutaConfig = getConfigPath
val format     = new SimpleDateFormat("yyyy-MM-dd")
val cal        = Calendar.getInstance()
val prop       = new java.util.Properties

// Carga la configuracion desde archivo config.properties
prop.load( new BufferedReader (new InputStreamReader (new FileInputStream (rutaConfig))))

//CONEXION A BASE DE DATOS
val driver        = prop.getProperty("database.driver")
val url           = prop.getProperty("database.url")
val schema        = prop.getProperty("database.schema.fin")
val exa_user      = prop.getProperty("database.user")
val secreto_scope = prop.getProperty("secreto.scope")
val secreto_key   = prop.getProperty("secreto.key")

val exa_pswd = dbutils.secrets.get(scope = s"$secreto_scope", key = s"$secreto_key")
val osuser   = prop.getProperty("database.osuser")

// COMMAND ----------

//TABLA DE ENTRADA
val tabla_in_cdr_carrier_billing = ".raw_finanzas.cdr_carrier_billing"


// COMMAND ----------

//CAMPOS PARA EL WHERE
val year = fecha.toString.substring(0,4)
val month = fecha.toString.substring(4,6)

var subpartition = spark.sql(s"select ('PART_P'|| cast(cast(MOD(year,2) as integer) as string) || '_MES'|| month) as trunc_1,('PART_P'|| cast(cast(MOD(substring(cast(add_months(to_date(year||month,'yyyyMM'),-14) as string),0,4),2) as integer) as string) || '_MES'|| substring(cast(add_months(to_date(year||month,'yyyyMM'),-14) as string),6,2)) as trunc_2 from biproduccion.raw_finanzas.cdr_carrier_billing where year=$year and month=$month limit 1")

//subpartition.collect.foreach(println)

val trunc_1 = subpartition.select("trunc_1").collect.map(_.toSeq)
val valor_1 = trunc_1.mkString("").toString().substring(9,22)

val trunc_2 = subpartition.select("trunc_2").collect.map(_.toSeq)
val valor_2 = trunc_2.mkString("").toString().substring(9,22)

// COMMAND ----------

// MAGIC %md
// MAGIC ###Lectura Tabla

// COMMAND ----------

var df_CDR_CB = spark.sql(s"select FECHA_TX, HORA_TX, MSISDN, ACCION, MERCHANT, ID_TX, IDTRANSACCIONTEF, MONTO, ELEGIBILIDAD, CARGO_O_ABONO, CODIGO_DE_ERROR, DESCRIPCION_ERROR from $catalogo$tabla_in_cdr_carrier_billing where year=$year and month=$month")

// COMMAND ----------

df_CDR_CB.count()

// COMMAND ----------

// MAGIC %md ###Trunca tabla en Exadata

// COMMAND ----------

var connection :Connection = null

prop.setProperty("driver",driver)
prop.setProperty("user",exa_user)
prop.setProperty("password",exa_pswd)
prop.setProperty("v$session.osuser", osuser)

// make the connection
Class.forName(driver)
connection = DriverManager.getConnection(url, prop)

val statement = connection.createStatement()
val resultSet0 = statement.executeUpdate(s"call $schema.DO_THE_TRUNCATE_PARTITION('SA_CDR_CARRIER_BILLING','SUBPARTITION','"+valor_1+"')")

// truncado de mes 14 anterior
val resultSet1 = statement.executeUpdate(s"call $schema.DO_THE_TRUNCATE_PARTITION('SA_CDR_CARRIER_BILLING','SUBPARTITION','"+valor_2+"')")

connection.close()

// COMMAND ----------

// MAGIC %md ###Carga Exadata

// COMMAND ----------

//val table = s"$schema.SA_CDR_CARRIER_BILLING"
val table = "FINANZAS.SA_CDR_CARRIER_BILLING"

//df_CDR_CB.repartition(1).write.mode("append").jdbc(url,table,prop)
df_CDR_CB.repartition(3).write.mode("append").jdbc(url,table,prop)
println("[INFO] Proceso Terminado")

