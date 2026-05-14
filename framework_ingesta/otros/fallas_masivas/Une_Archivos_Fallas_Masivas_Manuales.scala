// Databricks notebook source
// Une archivos de fallas masivas son muchos y los deja en uno
// René Lobera

dbutils.widgets.text("adls_container","abfss://ingestas@stbigdatadev02.dfs.core.windows.net")
val adls_container = dbutils.widgets.get("adls_container")

dbutils.widgets.text("dir_adls_rel","/data/gestion_recursos/operacion_red/fatem/fallas_masivas_finaliz_manuales_fo")
val dir_adls_rel = dbutils.widgets.get("dir_adls_rel")

dbutils.widgets.text("catalogo","bi_ingestas")
val catalogo = dbutils.widgets.get("catalogo")

val directoryName = adls_container+dir_adls_rel+"/stage2"
val outputFileName = adls_container+dir_adls_rel+"/stage/"

//val directoryName = "abfss://ingestas@stbigdatadev02.dfs.core.windows.net/data/gestion_recursos/operacion_red/fatem/fallas_masivas_nuevas_react_fo/stage2"
                     
//val outputFileName = "abfss://ingestas@stbigdatadev02.dfs.core.windows.net/data/gestion_recursos/operacion_red/fatem/fallas_masivas_nuevas_react_fo/stage/"

// Obtener la lista de archivos en el directorio
val files = dbutils.fs.ls(directoryName).filter(_.isFile).map(_.path)
var file_name=""
var file_size=0

if (files.nonEmpty) {
// Leer y combinar los archivos
  var isFirstFile = true
  var nombre_archivo=""
  val combinedData = files.flatMap { file =>
    val data = spark.read.text(file).collect().map(_.getString(0).replaceAll("[\\r\\n]", "").replaceAll("[^\\x20-\\x7E]", ""))
    var filteredData= Array.empty[String]
    if ( directoryName.contains("/data/gestion_recursos/operacion_red/fatem/fallas_masivas_nuevas_manuales_fo") )
      filteredData = data.filter(line => line.startsWith("20") || line.startsWith("INICIO"))
    else if (directoryName.contains("/data/gestion_recursos/operacion_red/fatem/fallas_masivas_finaliz_manuales_fo"))
    {
      filteredData = data
      println("filtro 2")
    }
    else
    {
      filteredData = data
      println("sin filtro")
    }
    if (isFirstFile) {
      isFirstFile = false
      nombre_archivo=file
      filteredData
    } else {
      filteredData.tail // Omitir la primera línea (encabezado) en archivos posteriores
    }
  }.toList

  val nombre_archivo2=nombre_archivo.split("/").last
  //file_name = nombre_archivo2.substring(0, nombre_archivo2.lastIndexOf('_')) + ".csv"
  file_name = nombre_archivo2
  dbutils.fs.put(outputFileName+file_name, combinedData.mkString("\n"), true)
  file_size = combinedData.mkString("\n").getBytes("UTF-8").length
  
  files.foreach(file => dbutils.fs.rm(file, true))
}
else
  println("Sin Archivos")

dbutils.notebook.exit(s"$file_name,$file_size")

