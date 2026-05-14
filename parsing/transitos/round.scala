// Databricks notebook source
// MAGIC %md
// MAGIC # Variables

// COMMAND ----------

val contenedor = dbutils.widgets.get("adls_container")
val ruta = dbutils.widgets.get("ruta_stage")
val nombreArchivo = dbutils.widgets.get("nombre_archivo")
val encoding = dbutils.widgets.get("encoding")

// COMMAND ----------

// MAGIC %md
// MAGIC # Lógica

// COMMAND ----------

import org.apache.spark.sql.functions._

// Leer el archivo txt de la ruta final
val txtDF = spark.read.option("encoding", encoding).text(ruta)

// Convertir el DataFrame a un array de strings
val txtArray = txtDF.collect().map(_.getString(0))

// Modificar las columnas correspondientes a los elementos número 13, 21 y 22
val modifiedArray = txtArray.map { row =>
  val columns = row.split(";")
  if (columns.length > 12) {
    columns(12) = columns(12).replace(",", ".")
  }
  if (columns.length > 20) {
    columns(20) = columns(20).replace(",", ".")
  }
  if (columns.length > 21) {
    columns(21) = columns(21).replace(",", ".")
  }
  columns.mkString(";")
}

// Convertir el array modificado de nuevo a un DataFrame
val finalDF_ = spark.createDataFrame(modifiedArray.map(Tuple1(_))).toDF("value")

// Renombrar las columnas society y purchase_order_document_number
val finalDF = finalDF_.withColumn("value",regexp_replace($"value","society","sociedad"))
.withColumn("value",regexp_replace($"value","purchase_order_document_number","numero_documento_compras"))
.withColumn("value",regexp_replace($"value","purchase_order_document_number_position","numero_posicion_documento_compras"))
.withColumn("value",regexp_replace($"value","commercial_document_number","numero_documento_comercial"))
.withColumn("value",regexp_replace($"value","commercial_position_document_number","numero_posicion_documento_comercial"))
.withColumn("value",regexp_replace($"value","goods_issue_date","fecha_salida_mercancias"))
.withColumn("value",regexp_replace($"value","sku","numero_material"))
.withColumn("value",regexp_replace($"value","batch","lote"))
.withColumn("value",regexp_replace($"value","original_center","centro_origen"))
.withColumn("value",regexp_replace($"value","original_location","almacen_origen"))
.withColumn("value",regexp_replace($"value","destination_center","centro_destino"))
.withColumn("value",regexp_replace($"value","destination_location","almacen_destino"))
.withColumn("value",regexp_replace($"value","order_quantity","cantidad_pedido"))
.withColumn("value",regexp_replace($"value","base_unit_measure","unidad_medida_base"))
.withColumn("value",regexp_replace($"value","execution_time","hora_ejecucion"))
.withColumn("value",regexp_replace($"value","execution_date","fecha_ejecucion"))
.withColumn("value",regexp_replace($"value","files_data_source","files_data_source"))
.withColumn("value",regexp_replace($"value","system_data_source","system_data_source"))
.withColumn("value",regexp_replace($"value","etl_info","etl_info"))
.withColumn("value",regexp_replace($"value","Texto_Breve_Material","texto_breve_material"))
.withColumn("value",regexp_replace($"value","Cantidad_Por_Entregar","cantidad_por_entregar"))
.withColumn("value",regexp_replace($"value","Importe","importe"))
.withColumn("value",regexp_replace($"value","Usuario","usuario"))
.withColumn("value",regexp_replace($"value","Referencia","referencia"))
.withColumn("value",regexp_replace($"value","Organización_de_compras","organizacion_de_compras"))
.withColumn("value",regexp_replace($"value","Grupo_de_compras","grupo_de_compras"))
.withColumn("value",regexp_replace($"value","Grupo_de_artículos","grupo_de_articulos"))

// Mostrar el DataFrame modificado
// display(finalDF)

finalDF.coalesce(1)
  .write.format("text").mode("overwrite")
  .save(ruta + "/tmp/")

// COMMAND ----------

// Ruta del archivo txt generado en la celda anterior
val rutaArchivoAnterior = ruta + "/tmp"

// Listar los archivos en la ruta anterior
val archivos = dbutils.fs.ls(rutaArchivoAnterior).filter(_.name.startsWith("part-"))

// Nueva ruta con el nuevo nombre
val nuevaRutaArchivo = ruta + "/" + nombreArchivo

// Mover y renombrar el archivo
if (archivos.nonEmpty) {
  dbutils.fs.mv(archivos(0).path, nuevaRutaArchivo)
  dbutils.fs.rm(rutaArchivoAnterior, true)
}

// COMMAND ----------

//val finalDF = spark.read.format("csv")
//  .option("header", "true")
//  .option("delimiter", ";")
//  .load(rutaFinal)
//  .withColumn("order_quantity", 
//    when($"order_quantity".contains(","), 
//          regexp_replace($"order_quantity", ",", ".").cast("double").cast("decimal(10,4)")
//        ).otherwise($"order_quantity")
//  )
//  .withColumn("Cantidad_Por_Entregar", regexp_replace($"Cantidad_Por_Entregar", ",", "."))
//  .withColumn("Importe", regexp_replace($"Importe", ",", "."))
//
//
//// Convertir el DataFrame a un formato adecuado para guardar como .txt
//val finalDFAsString = finalDF.selectExpr("concat_ws(';', value) as value")

//finalDFAsString.write
//  .format("text")
//  .mode("overwrite")
//  .save(rutaFinal)
