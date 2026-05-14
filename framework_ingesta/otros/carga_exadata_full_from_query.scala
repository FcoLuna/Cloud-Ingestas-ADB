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
// dbutils.widgets.text("schemaTable_unityCatalog","raw_interacciones.movimientos_puntos")
// dbutils.widgets.text("user_exadata","SRV_MIGRACION_BIGDATA")
// dbutils.widgets.text("kv_secret_name","sec-oracle-srv-migracion-bigdata")
// dbutils.widgets.text("schema_exadata","MIGRACION_BIGDATA")
// dbutils.widgets.text("table_exadata","movimientos_puntos")
// dbutils.widgets.text("schema_sp","MIGRACION_BIGDATA")
// dbutils.widgets.text("cant_partition_escritura","4")
// dbutils.widgets.text("query_catalog","select distinct cod_movimiento,cod_canal,des_evento,des_tipomovimiento,fec_movimiento,fec_vigencia,monto,obs_movimiento,rut_socio,saldo,usuario_unif from bi_ingestas.raw_interacciones.movimientos_puntos")

// COMMAND ----------

println("[INFO] Parametros=====")
//CATALOGO DEL CUAL SE TOMARÁN LOS DATOS A CARGAR
val catalog = dbutils.widgets.get("catalog")
//TABLA DEL CATALOGO LA CUAL SE TOMARÁN LOS DATOS A CARGAR
val table_unityCatalog = dbutils.widgets.get("schemaTable_unityCatalog")
// CONCATENACION DE CATALOGO + SCHEMATABLE
val catalogSchemaTable_ADB = catalog + "." + table_unityCatalog
//COLUMNAS QUE SE RESCATARAN DESDE LA TABLA DEL UNITY CATALOG (PUEDE SER * PARA TODO O SEPARADO POR COMAS - COLUMN_1, COLUMN_2, COLUMN_3)
// val columns = dbutils.widgets.get("columns")
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
val cant_partition_escritura = Option(dbutils.widgets.get("cant_partition_escritura")).filterNot(_.isEmpty).getOrElse("1").toInt
//Query de obtencion de datos desde Unity Catalog
val query_catalog = Option(dbutils.widgets.get("query_catalog")).filterNot(_.isEmpty).getOrElse(s"select * from ${catalogSchemaTable_ADB}")

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
prop.setProperty("batchsize", "10000")

// COMMAND ----------

// MAGIC %md ###Trunca tabla en Exadata

// COMMAND ----------

val query_truncate = s"call ${schema_sp}.DO_THE_TRUNCATE('"+schemaTable_exadata+"')"
println("Query truncate: " + query_truncate)

var connection: Connection = null
Class.forName(driver)
connection = DriverManager.getConnection(url,prop)
val statement = connection.createStatement()

println("[INFO] Ejecutando query")
val resultSet = statement.executeUpdate(query_truncate)
println("[INFO] Tabla truncada")

connection.close()

// COMMAND ----------

// MAGIC %md ###Obtener datos desde Unity Catalog y Carga Exadata

// COMMAND ----------

//SE GENERA DF CON DATOS DE LA TABLA DE ORIGEN
println("[INFO] QUERY A CONSULTAR: " + query_catalog)

val df = spark.sql(s"${query_catalog}")

//SE CUENTAN LOS VALORES DEL DATAFRAME
// val df_count = df.count()
// println("[INFO] CANTIDAD REGISTROS A ESCRIBIR: " + df_count)

//SI EL DF TIENE REGISTROS SE REALIZA LA CARGA, SINO SE AVISA QUE EL DF NO TIENE DATOS
if (!df.isEmpty) {
  try{
    println("[INFO] ESCRIBIENDO A EXADATA")

    df.
      write.mode("append").
      option("numPartitions", cant_partition_escritura).
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

