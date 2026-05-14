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

//var catalogName = dbutils.widgets.text("catalog_name", "bi_ingestas", "Nombre_catalogo")
// Obtener el nombre del catálogo y el nombre de la tabla
val catalogName = dbutils.widgets.get("catalog_name")

// COMMAND ----------


println(catalogName)

val sql_del = s"""
WITH duplicados AS (
      SELECT
      id_operador, hora_envio_solicitud, nt, tipo_de_servicio_donante, tipo_de_servicio_receptor, region, comuna, localidad, canal_de_atencion, modalidad, rut_del_cliente, id_portabilidad, receptor, donante, estado, bigdata_close_date, bigdata_ctrl_id, year, month, day,
      ROW_NUMBER() OVER (PARTITION BY
      id_operador, hora_envio_solicitud, nt, tipo_de_servicio_donante, tipo_de_servicio_receptor, region, comuna, localidad, canal_de_atencion, modalidad, rut_del_cliente, id_portabilidad, receptor, donante, estado
      ORDER BY hora_envio_solicitud asc) AS rn
      FROM ${catalogName}.raw_interacciones.detalle_solicitud_portabilidad
 
)
DELETE FROM ${catalogName}.raw_interacciones.detalle_solicitud_portabilidad
WHERE hora_envio_solicitud IN (
    SELECT hora_envio_solicitud
    FROM duplicados
    WHERE rn > 1
);

"""

// COMMAND ----------

// Ejecutar la consulta SQL
val result = spark.sql(sql_del)
// Mostrar un mensaje de confirmación
println("Delete operation completed.")
// Mostrar los resultados
result.show()
