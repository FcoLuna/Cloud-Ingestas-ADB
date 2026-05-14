// Databricks notebook source
import java.sql._
import java.util.Properties

// COMMAND ----------

// MAGIC %run ../utilidades/parse_json

// COMMAND ----------

val obj_parse_son: parse_json.type = parse_json

// COMMAND ----------

object sql_utils {
  private var registeredDrivers: List[String] = Nil

  def registerDriver(driverClass: String) = {
        try {
            if (driverClass != null) {
                if (!registeredDrivers.contains(driverClass)) {
                    java.lang.Class.forName(driverClass).newInstance;
                    registeredDrivers = driverClass :: registeredDrivers;
                }
            }
        } catch {
            case c: ClassNotFoundException => throw new Exception("ERROR registrando el driver " + driverClass + " ClassNotFoundException:", c)
            case t: Throwable => throw new Exception("ERROR registrando el driver " + driverClass + " exception " + t.getClass.getCanonicalName + ":", t)
        }
    }

    def connect_direct(username: String, password: String, jdbcUrl: String): Connection = {
        println("Conectando a Base de Datos")
        val driver: String = "oracle.jdbc.driver.OracleDriver"
        try {
            Class.forName(driver)
            val connection: Connection = DriverManager.getConnection(jdbcUrl, username, password)
            println("Conexion a Base de Datos exitosa")
            connection
        } catch {
            case e: Exception => {
                println("ERROR conectando a la Base de Datos " + jdbcUrl)
                throw new Exception("ERROR conectando a la Base de Datos " + jdbcUrl, e)
            }
        } finally {
        }
    }

    /**
     *
     * @param jdbcUrl JDBC
     * @param props
     * @return Соnexion
     */
    def connect(jdbcUrl: String, props: Properties): Connection = {
        println("Conectando a Base de Datos")
        try {
            val connection: Connection = DriverManager.getConnection(jdbcUrl, props)
            println("Conexion a Base de Datos exitosa")
            connection
        } catch {
            case e: java.sql.SQLException => {
                e.printStackTrace()
                throw new Exception("ERROR conectando a la Base de Datos " + jdbcUrl, e)
            }
            case e: Exception => {
                e.printStackTrace()
                throw new Exception("ERROR conectando a la Base de Datos " + jdbcUrl, e)
            }
        }
    }

    def close(c: Connection, s: CallableStatement, r: ResultSet = null): Unit = {
        if (r != null) try {
            r.close()
        } catch {
            case e: SQLException => {
                println("ERROR: Ocurrio un error cerrando ResultSet." + e)
                e.printStackTrace()
            }
        }
        if (s != null) try {
            s.close()
        } catch {
            case e: SQLException => {
                println("ERROR: Ocurrio un error cerrando CallableStatement." + e)
                e.printStackTrace()
            }
        }
        if (c != null) try {
            c.close()
            println(s"••• Desconexion a Base de Datos exitosa")
        } catch {
            case e: SQLException => {
                println("ERROR: Ocurrio un error cerrando la conexion a la BD." + e)
                e.printStackTrace()
            }
        }
    }
    
    def close(c: Connection, s: CallableStatement): Unit = {
        if (s != null) try {
            s.close()
            println(s" CallableStatement cerrado exitosa")
        } catch {
            case e: SQLException => {
                println("ERROR: Ocurrio un error cerrando la conexion CallableStatement." + e)
                e.printStackTrace()
            }
        }
        if (c != null) try {
            c.close()
            println(s"••• Desconexion a Base de Datos exitosa")
        } catch {
            case e: SQLException => {
                println("ERROR: Ocurrio un error cerrando la conexion a la BD." + e)
                e.printStackTrace()
            }
        }
    }

}
