// Databricks notebook source
// MAGIC %md
// MAGIC ### Variables DEV

// COMMAND ----------

/*
 * Variables DEV
val ruta = "abfss://bigdataprd@stbigdataprd02.dfs.core.windows.net/data/gestion_recursos/logistica/sap/stock_series_convergente/stage"
val nombreArchivo = "0404_OWS_Stk_Ser_20250219.txt"
val encoding = "US-ASCII"
val numeroCampo = 4 */

// COMMAND ----------

// MAGIC %md
// MAGIC ### Variables PRD

// COMMAND ----------

/*
 * Variables PRD */
val contenedor = dbutils.widgets.get("adls_container")
val ruta = dbutils.widgets.get("ruta_stage")
val nombreArchivo = dbutils.widgets.get("nombre_archivo")
val encoding = dbutils.widgets.get("encoding")
val numeroCampo = dbutils.widgets.get("numero_campo").toInt

// COMMAND ----------

// MAGIC %md
// MAGIC ### Lógica

// COMMAND ----------

import org.apache.spark.sql.functions._

// Leer el archivo txt de la ruta final
val txtDF = spark.read.option("encoding", encoding).text(ruta)

// Convertir el DataFrame a un array de strings
val txtArray = txtDF.collect().map(_.getString(0))

// Modificar las columnas correspondientes a los elementos número 4
val modifiedArray = txtArray.map { row =>
  val columns = row.split(";")
  if (columns.length > numeroCampo) {
    columns(numeroCampo) = columns(numeroCampo).replaceFirst("^0+(?!$)", "")
  }
  columns.mkString(";")
}

// Dividir el array modificado en chunks más pequeños
val chunkSize = 100000000 // Ajustar el tamaño del chunk según sea necesario
val chunks = modifiedArray.grouped(chunkSize).toArray

// Convertir cada chunk a un DataFrame y unirlos
val finalDF = chunks.map { chunk =>
  spark.createDataFrame(chunk.map(Tuple1(_))).toDF("value")
}.reduce(_ union _)

// Mostrar el DataFrame modificado
// display(finalDF)

// COMMAND ----------

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
