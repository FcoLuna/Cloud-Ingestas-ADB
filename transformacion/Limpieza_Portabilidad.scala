// Databricks notebook source
import java.util.concurrent.TimeUnit
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types._

import java.util.{Calendar, TimeZone, Date}
import org.apache.spark.sql.types.DateType
import java.time.format.DateTimeFormatter
import java.time.LocalDateTime
import java.text.SimpleDateFormat

// COMMAND ----------

//var catalogNamet = dbutils.widgets.text("catalog_name", "bi_ingestas", "Nombre_catalogo")
// Obtener el nombre del catálogo y el nombre de la tabla
val catalogName = dbutils.widgets.get("catalog_name")

// COMMAND ----------


println(catalogName)

val sql_del = s"""
DELETE FROM ${catalogName}.raw_interacciones.detalle_solicitud_portabilidad 
WHERE
id_operador like '%PORTABILIDAD%' 
OR id_operador like '%ID Operador%'
OR hora_envio_solicitud like '%Hora Envio Solicitud%'
OR nt like '%NT%'
OR tipo_de_servicio_donante like '%Tipo de Servicio%'
OR tipo_de_servicio_receptor like '%Tipo de Servicio%'
OR comuna like '%Comuna%'
OR localidad like '%Localidad%'
OR canal_de_atencion like '%Canal de atención%'
OR modalidad like '%Modalidad%'
OR rut_del_cliente like '%RUT del Cliente%'
OR id_portabilidad like '%ID Portabilidad%'
OR receptor like '%Receptor%'
OR donante like '%Donante%' 
OR estado like '%Estado%'
"""

// COMMAND ----------

// Ejecutar la consulta SQL
val result = spark.sql(sql_del)
// Mostrar un mensaje de confirmación
println("Delete operation completed.")
// Mostrar los resultados
result.show()
