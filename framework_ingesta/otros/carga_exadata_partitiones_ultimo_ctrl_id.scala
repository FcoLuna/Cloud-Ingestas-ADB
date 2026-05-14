// Databricks notebook source
// MAGIC %md ###Librerias

// COMMAND ----------

import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import java.sql.{Connection, DriverManager}

// COMMAND ----------

// MAGIC %md ###Variables y Parametros

// COMMAND ----------

// dbutils.widgets.text("catalog","bi_ingestas")
// dbutils.widgets.text("schemaTable_unityCatalog","raw_interacciones.consultas_deuda_portout")
// dbutils.widgets.text("columns","*")
// dbutils.widgets.text("user_exadata","SRV_MIGRACION_BIGDATA")
// dbutils.widgets.text("kv_secret_name","sec-oracle-srv-migracion-bigdata")
// dbutils.widgets.text("schema_exadata","MIGRACION_BIGDATA")
// dbutils.widgets.text("table_exadata","CONSULTAS_DEUDA_PORTOUT")
// dbutils.widgets.text("schema_sp","MIGRACION_BIGDATA")
// dbutils.widgets.text("where_catalogo","")
// dbutils.widgets.text("cant_repartition","16")
// dbutils.widgets.text("partition_date_column","hora_envio_resp")

// COMMAND ----------

println("[INFO] Parametros=====")
//CATALOGO DEL CUAL SE TOMARÁN LOS DATOS A CARGAR
val catalog = dbutils.widgets.get("catalog")
//TABLA DEL CATALOGO LA CUAL SE TOMARÁN LOS DATOS A CARGAR
val table_unityCatalog = dbutils.widgets.get("schemaTable_unityCatalog")
// CONCATENACION DE CATALOGO + SCHEMATABLE
val catalogSchemaTable_ADB = catalog + "." + table_unityCatalog
//COLUMNAS QUE SE RESCATARAN DESDE LA TABLA DEL UNITY CATALOG (PUEDE SER * PARA TODO O SEPARADO POR COMAS - COLUMN_1, COLUMN_2, COLUMN_3)
val columns = dbutils.widgets.get("columns")
//USUARIO BASE DE DATOS
val user_exadata = dbutils.widgets.get("user_exadata")
//SECRETO BASE DE DATOS
val kv_secret_name = dbutils.widgets.get("kv_secret_name")
//ESQUEMA DE LA TABLA A CARGAR
val schema_exadata = dbutils.widgets.get("schema_exadata")
//NOMBRE TABLA A CARGAR
val table_exadata = dbutils.widgets.get("table_exadata")
//ESQUEMA DEL PROCEDIMIENTO ALMACENADO
val schema_sp = dbutils.widgets.get("schema_sp")
//CANTIDAD DE PARTICIONES, EN CASO DE NO AGREGAR LO DEJA POR DEFECTO EN 1
val cant_repartition = Option(dbutils.widgets.get("cant_repartition")).filterNot(_.isEmpty).getOrElse("1").toInt

val partition_date_column = dbutils.widgets.get("partition_date_column")

// COMMAND ----------

// MAGIC %md ###Conexión Exadata

// COMMAND ----------

// DBTITLE 1,Base Exadata
 println("[INFO] VARIABLES DE ENTRADA EXADATA=====")

 val server       = "smt-scan.tchile.local"
 val port         = "1521"
 val serviceName  = "explota"
 val user         = user_exadata
 val passw        = dbutils.secrets.get("secrets-ingestas",kv_secret_name)
 val driver       = "oracle.jdbc.driver.OracleDriver"
 val url          = "jdbc:oracle:thin:@(DESCRIPTION= (ADDRESS = (PROTOCOL = TCP)(HOST = smt-scan.tchile.local)(PORT = 1521)) (CONNECT_DATA = (SERVER = DEDICATED) (SERVICE_NAME = explota)))"
 val osuser       = "Ingestas"
 val schemaTable_exadata = schema_exadata + "." + table_exadata

 val prop = new java.util.Properties
 prop.setProperty("driver", driver)
 prop.setProperty("user", user)
 prop.setProperty("password", passw)
 prop.put("v$session.osuser",osuser)

// COMMAND ----------

// MAGIC %md ###Busca fechas de particion relacionadas al último BIGDATA_CTRL_ID de la tabla en Catalog

// COMMAND ----------

val df_particiones_exa = spark.sql(s"select distinct CAST(${partition_date_column} AS DATE) AS partition_date_column from ${catalogSchemaTable_ADB} where bigdata_ctrl_id = (select max(bigdata_ctrl_id) from ${catalogSchemaTable_ADB})")
// val df_particiones_exa = spark.sql(s"select distinct 'PART_'||date_format(${partition_date_column},'yyyyMMdd') from ${catalogSchemaTable_ADB} where bigdata_ctrl_id = (select max(bigdata_ctrl_id) from ${catalogSchemaTable_ADB})")

df_particiones_exa.createOrReplaceTempView("df_particiones_exa")

// COMMAND ----------

// MAGIC %md ###Trunca particiones en Exadata

// COMMAND ----------

var connection: Connection = null
Class.forName(driver)
connection = DriverManager.getConnection(url,prop)
val statement = connection.createStatement()

spark.sql(s"select 'PART_'||date_format(partition_date_column,'yyyyMMdd') from df_particiones_exa").collect().foreach { particion =>
  // println(particion)
  val query_truncate = s"BEGIN ${schema_sp}.DO_THE_TRUNCATE_PARTITION('"+schemaTable_exadata+"','PARTITION','"+particion.getString(0)+"'); END;"
  println("Query truncate: " + query_truncate)

  println("[INFO] Ejecutando query")
  val resultSet = statement.executeUpdate(query_truncate)
  println("[INFO] Particion truncada")
}

connection.close()

// COMMAND ----------

// MAGIC %md ###Obtener datos desde Unity Catalog y Carga Exadata

// COMMAND ----------

val df = spark.sql(s"""SELECT ${columns}  FROM ${catalogSchemaTable_ADB} t join df_particiones_exa p on t.bigdata_close_date = p.partition_date_column""")
//display(df)
//SE CUENTAN LOS VALORES DEL DATAFRAME
val df_count = df.count()
println("[INFO] CANTIDAD REGISTROS A ESCRIBIR: " + df_count)

//SI EL DF TIENE REGISTROS SE REALIZA LA CARGA, SINO SE AVISA QUE EL DF NO TIENE DATOS
if(df_count > 0) {
  try{
    println("[INFO] ESCRIBIENDO A EXADATA")

    df.
      repartition(cant_repartition).
      write.option("batchsize",10000).mode("append").
      jdbc(url,schemaTable_exadata,prop)

    println("[INFO] ESCRITURA A EXADATA REALIZADA SIN ERRORES")
  }catch{
    case e: Exception =>
      println("[ERROR] " + e)
      dbutils.notebook.exit("[ERROR] " + e)
  }
}else{
  val mensaje_error = "[ERROR] TABLA " + catalogSchemaTable_ADB + " NO CONTIENE DATOS"
  println(mensaje_error)
  dbutils.notebook.exit(mensaje_error)
}
