// Databricks notebook source
import java.util.Properties
import java.sql.Array

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._ 
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

import java.sql.{Connection, ResultSet, SQLException}
import java.util.Properties


// COMMAND ----------

// MAGIC %run ../mvp_ofertas/utilidades/utilidades

// COMMAND ----------

// MAGIC %run ../mvp_ofertas/utilidades/parse_json

// COMMAND ----------

// MAGIC %run ../mvp_ofertas/utilidades/sql_utils

// COMMAND ----------

// MAGIC %run ../mvp_ofertas/utilidades/date_utils

// COMMAND ----------

println("Parametros de Entrada:")
dbutils.widgets.text("catalogo", "")
val catalogo = dbutils.widgets.get("catalogo")

dbutils.widgets.text("dir_adls", "")
val dir_adls = dbutils.widgets.get("dir_adls")

dbutils.widgets.text("accion", "")
val accion = dbutils.widgets.get("accion")

dbutils.widgets.text("nombre_proceso", "")
val nombre_proceso = dbutils.widgets.get("nombre_proceso")

// COMMAND ----------

val objUtilidades: utilidades.type = utilidades
val objParseJson: parse_json.type = parse_json
val sqlUtils: sql_utils.type = sql_utils
val dateUtils: date_utils.type = date_utils

var ret_val: Int = 0
var errMsg: String = _
var connectionProperties = new Properties()

// COMMAND ----------

println("--------------------------------------")
println("El proceso ha comenzado correctamente!")
println("--------------------------------------")
ret_val = 0

var result: String = null

// Archivo de configuracion
val json_config_file = "file:" + objUtilidades.getPath("/mvp_ofertas/config/json_config_file.json")
val path_catalogo_oferta_ingst_in = catalogo+"."+objParseJson.obtenerValorJSON(json_config_file, "cat_delta", "catalogo_oferta_ingst_in")
val jdbcUrl       = objParseJson.obtenerValorJSON(json_config_file, "jdbc", "url")
val username      = objParseJson.obtenerValorJSON(json_config_file, "jdbc", "username")
val scope_azure   = objParseJson.obtenerValorJSON(json_config_file, "jdbc","scope_azure")
var secret_azure  = objParseJson.obtenerValorJSON(json_config_file, "jdbc","secret_azure")
var pass_explota  = objUtilidades.getSecret(scope_azure,secret_azure)
val driver        = objParseJson.obtenerValorJSON(json_config_file, "jdbc", "driver")
val dbtable       = objParseJson.obtenerValorJSON(json_config_file, "jdbc", "catalog_table_exadata")
val numPartitions = objParseJson.obtenerValorJSON(json_config_file, "jdbc", "numPartitions")
val path_file_ok  = dir_adls+objParseJson.obtenerValorJSON(json_config_file, "abfss", "path_file_ok")

connectionProperties.put("user", username)
connectionProperties.put("password", pass_explota)
connectionProperties.put("driver", driver)
connectionProperties.put("jdbcUrl", jdbcUrl)
connectionProperties.put("charSet", "UTF-8")
connectionProperties.put("fetchsize", "1000")
connectionProperties.put("numPartitions", numPartitions)
connectionProperties.put("v$session.osuser", "spark")


// COMMAND ----------

        val latestPartition = spark.sql(s"show partitions " + path_catalogo_oferta_ingst_in).
                        orderBy(col("year").desc, col("month").desc).
                        limit(1).collect()(0)
        println("path_catalogo_oferta_ingst_in: " + path_catalogo_oferta_ingst_in)
        val DF_RENTABILIZACION_MOVIL = spark.read.table(path_catalogo_oferta_ingst_in).
                                         filter("year=" + latestPartition.getString(0) + 
                                           " and month=" + latestPartition.getString(1) + 
                                           " and day=" + latestPartition.getString(2))

// COMMAND ----------

    var nRecords: Long = 0
    var DF_RENTABILIZACION_MOVIL: DataFrame = _
    
    try {
        println(s"[INFO] APP_NAME: ${nombre_proceso}\nPATH_TABLE_IN: ${path_catalogo_oferta_ingst_in}\nACCION: ${accion}")

        var df_result: DataFrame = null

        println(s"[INFO] Obteniendo datos desde ${path_catalogo_oferta_ingst_in}")

        val latestPartition = spark.sql(s"show partitions " + path_catalogo_oferta_ingst_in).
                                    orderBy(col("year").desc, col("month").desc).
                                    limit(1).collect()(0)

        println("path_catalogo_oferta_ingst_in: " + path_catalogo_oferta_ingst_in)
        DF_RENTABILIZACION_MOVIL = spark.read.table(path_catalogo_oferta_ingst_in).
                                         filter("year=" + latestPartition.getString(0) + 
                                           " and month=" + latestPartition.getString(1) + 
                                           " and day=" + latestPartition.getString(2))
 
        val DF_RENTABILIZACION_MOVIL_RENAME = DF_RENTABILIZACION_MOVIL.
                                        withColumn("CODIGO_PRODUCTO", substring(col("CODIGO_PRODUCTO"), 1, 30)).
                                        withColumn("DESCRIPCION_CANAL", substring(col("DESCRIPCION_CANAL"), 1, 150)).
                                        withColumn("FECHA_PROCESAMIENTO", lit(dateUtils.getCurrentTime).cast("timestamp")).
                                        withColumn("ACCION", lit(accion)).
                                        drop("year", "month", "day").
                                        select($"TIPO_MERCADO", 
                                               $"FECHA_PROCESAMIENTO".cast("date"), 
                                               $"FECHA_INICIO".cast("date"), 
                                               $"FECHA_FIN".cast("date"), 
                                               $"NOMBRE_MACRO_CAMPANA", 
                                               $"NOMBRE_MICRO_CAMPANA", 
                                               $"ID_PRIORIDAD", 
                                               $"NOMBRE_OFERTA", 
                                               $"ID_OFERTA", 
                                               $"ID_OFERTA_DEPENDENCIA", 
                                               $"NOMBRE_CRM", 
                                               $"CODIGO_OFERTA", 
                                               $"CODIGO_PRODUCTO", 
                                               $"CODIGO_PROMOCION", 
                                               $"DESCRIPCION_CODIGO_REFERENCIA", 
                                               $"UNIDAD_MEDIDA_DESCUENTO", 
                                               $"DESCUENTO_OFERTA", 
                                               $"DURACION_DESCUENTO".cast("int"), 
                                               $"DESCRIPCION_OFERTA", 
                                               $"PRECIO_OFERTA".cast("int"), 
                                               $"PRECIO_OFERTA_DESCUENTO".cast("int"), 
                                               $"URL_FORMULARIO", 
                                               $"DETALLE_TEMATICOS1_OFERTA", 
                                               $"DETALLE_TEMATICOS2_OFERTA", 
                                               $"DETALLE_TEMATICOS3_OFERTA", 
                                               $"DETALLE_TEMATICOS4_OFERTA", 
                                               $"DESCRIPCION_CANAL", 
                                               $"ID_CANAL_VECTOR", 
                                               $"ACCION")  

        // Grabar en la BD
        println(s"Iniciando Carga en BD")
        DF_RENTABILIZACION_MOVIL_RENAME.write.mode(SaveMode.Append).jdbc(jdbcUrl, dbtable, connectionProperties)

        nRecords = DF_RENTABILIZACION_MOVIL_RENAME.count()

        // Escribir el DataFrame como un archivo CSV en la ruta ABFS con extensión .ok
        if(nRecords > 0){
        println(s"[INFO] Carga en ${dbtable} Exitosa.")
        } else {
          ret_val = 1
          println(s"[ERROR] Carga en ${dbtable} con cero registros.")
        }

    } catch {
        case e: Exception => {
            ret_val = 1
            errMsg = s"${ret_val}|Ocurrio una Exception al carga en ${dbtable}.|${e}"
            println(s"[ERROR] - ${errMsg}")
            println(errMsg)
            throw new Exception(s"ERROR:" + e)
            e.printStackTrace()
        }
    }

