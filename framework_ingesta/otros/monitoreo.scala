// Databricks notebook source
// MAGIC %md
// MAGIC ##Librerias

// COMMAND ----------

import java.util.Calendar
import java.text.SimpleDateFormat

import java.sql.Connection
import java.sql.DriverManager
import oracle.jdbc.pool.OracleDataSource

// COMMAND ----------

// MAGIC %md
// MAGIC ##Variables

// COMMAND ----------

dbutils.widgets.text("nombre_proceso","AMDOCS_PRUEBA")

// COMMAND ----------

val nombre_proceso    = dbutils.widgets.get("nombre_proceso")

val existe            = "0"
var calendario        = Calendar.getInstance()
val format_date       = new SimpleDateFormat("yyyyMMdd")
val format_timestamp  = new SimpleDateFormat("yyyyMMdd HHmmss")
val fecha_hora_fin    = format_timestamp.format(calendario.getTime)
calendario.add(Calendar.HOUR_OF_DAY, -2) // 2 horas atras
val fecha_hora_inicio = format_timestamp.format(calendario.getTime)
calendario        = Calendar.getInstance()
calendario.add(Calendar.DATE, -1) // dia anterior
val fecha_proceso     = format_date.format(calendario.getTime) 
var hora_inicio_proceso = ""

// COMMAND ----------

// MAGIC %md
// MAGIC ##Codigo

// COMMAND ----------

// se generan las variables de conexion de la base de datos para cargar los datos del df2 a una tabla en exadata
  val url = "jdbc:oracle:thin:@(DESCRIPTION= (ADDRESS = (PROTOCOL = TCP)(HOST = smt-scan.tchile.local)(PORT = 1521)) (CONNECT_DATA = (SERVER = DEDICATED) (SERVICE_NAME = explota)))"
  val driver = "oracle.jdbc.driver.OracleDriver"

  val user = "SRV_AMDOCS_FIJO_BIGDATA"
  val passw = "f1j0_4md0cs_2021"
  val osuser = "SRV_AMDOCS_FIJO_BIGDATA"
  //val secrets = new SecretsAmdocs()
  //val exa_user = "SRV_AMDOCS_FIJO_BIGDATA"
  //val secreto = "sec-oracle-srv-amdocs-fijo-bigdata"
  //val exa_pswd        = secrets.getSecret(secreto)

  //val exadata_connection = new ExadataConnection(exa_user,exa_pswd)
  //val instance_connection = exadata_connection.getExadataInstanceConnection()

  //val call: CallableStatement = instance_connection.prepareCall(s"select MAX(TO_CHAR(HORA_INICIO_PROCESO,'YYYYMMDD HHMISS')) from CONFORMADO_EXP.VW_CS_DETALLE_MONITOREO_E2E WHERE NOMBRE_PROCESO = '${nombre_proceso}' AND TO_CHAR(TRUNC(FECHA_PROCESO),'YYYYMMDD') = '${fecha_proceso}'")
  //call.executeUpdate()
  //instance_connection.close()


  var prop = new java.util.Properties
  prop.setProperty("driver", driver)
  prop.setProperty("user", user)
  prop.setProperty("password", passw)
  prop.setProperty("v$session.osuser", osuser)

  var connection: Connection = null
  //println(driver)
  Class.forName(driver)
  //connection = DriverManager.getConnection(url, user, passw)
  connection = DriverManager.getConnection(url, prop)
  val statement = connection.createStatement()

  var consulta_data = statement.executeQuery(s"SELECT MAX(TO_CHAR(HORA_INICIO_PROCESO,'YYYYMMDD HH24MISS')) as HORA_INICIO_PROCESO from CONFORMADO_EXP.VW_CS_DETALLE_MONITOREO_E2E WHERE NOMBRE_PROCESO = '${nombre_proceso}' AND TO_CHAR(TRUNC(FECHA_PROCESO),'YYYYMMDD') = '${fecha_proceso}'")

  while ( consulta_data.next() ) {
    hora_inicio_proceso = consulta_data.getString("HORA_INICIO_PROCESO")
    println("hora inicio proceso = " + hora_inicio_proceso )
  }
  

  if ( hora_inicio_proceso == null ) {
    //hace el insert
    val insert_data_monitoreo = statement.executeUpdate(s"call CAPSEM.USP_TABLA_CONTROL_INS('" + nombre_proceso + "','" + fecha_proceso + "','" + fecha_hora_inicio + "')")
    println("insert monitoreo")
    //hace el update
    val update_data_monitoreo = statement.executeUpdate(s"call CAPSEM.USP_TABLA_CONTROL_UPD('" + nombre_proceso + "',0,'" + fecha_proceso + "','" + fecha_hora_inicio + "','" + fecha_hora_fin + "',0,'Proceso Finalizado')")
    println("update monitoreo")
  } else {
    // hace el update
    val update_data_monitoreo = statement.executeUpdate(s"call CAPSEM.USP_TABLA_CONTROL_UPD('" + nombre_proceso + "',0,'" + fecha_proceso + "','" + hora_inicio_proceso + "','" + fecha_hora_fin + "',0,'Proceso Finalizado')")
    println("update monitoreo")
  }

  connection.close()
