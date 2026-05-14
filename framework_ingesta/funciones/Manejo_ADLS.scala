// Databricks notebook source
// MAGIC %md
// MAGIC ### Obtener tamaño carpeta (usado para original file size)

// COMMAND ----------


/* import org.apache.spark.sql.{DataFrame, SparkSession}
import com.databricks.backend.daemon.dbutils.FileInfo
import spark.implicits._
    
def get_folder_size(folderPath: String): String = {
    var fileInfos = dbutils.fs.ls(folderPath)
    val fileAttributesSeq = fileInfos.map(file => (file.size))
    val df: DataFrame = fileAttributesSeq.toDF()
    val size = df.agg(sum("value")).first.get(0).toString()
    return size

} */
/*
val path_contenedor_dev = "abfss://bigdatadev@stbigdatadev02.dfs.core.windows.net"
val path_lnp = s"${path_contenedor_dev}/modelos/interacciones/lista_no_publicidad/conformado" // OK DELTA 
get_folder_size(path_lnp) */
/* 
import com.databricks.backend.daemon.dbutils.FileInfo

def getFolderSize(folderPath: String): String = {
  dbutils.fs.ls(folderPath).map { fileInfo: FileInfo =>
    if (fileInfo.isDir) getFolderSize(fileInfo.path)
    else fileInfo.size
  }.sum
}

// Uso de la función
val path_contenedor_dev = "abfss://bigdatadev@stbigdatadev02.dfs.core.windows.net"
val path_lnp = s"${path_contenedor_dev}/modelos/interacciones/lista_no_publicidad/conformado" // OK DELTA 
val totalSize = getFolderSize(path_lnp)

println(s"El tamaño total de la carpeta es: $totalSize bytes") */

// COMMAND ----------

/* val text = "0/a/b"
text.split("/")(-1) */

// COMMAND ----------

import com.databricks.backend.daemon.dbutils.FileInfo

def get_folder_size(folderPath: String): String = {
  def getFolderSize(folderPath: String): Long = {
    dbutils.fs.ls(folderPath).map { item =>
      if (item.isDir) getFolderSize(item.path) else item.size
    }.sum
  }

  val totalSize = getFolderSize(folderPath)
  s"$totalSize"
}

/* val path_contenedor_dev = "abfss://bigdatadev@stbigdatadev02.dfs.core.windows.net"
val path_lnp = s"${path_contenedor_dev}/modelos/interacciones/lista_no_publicidad/conformado" // OK DELTA 
val totalSize = get_folder_size(path_lnp) 
println(totalSize) */


// COMMAND ----------



// COMMAND ----------

// MAGIC %md
// MAGIC ###Probar existencia de archivos en ADLS

// COMMAND ----------

def exists_file(filepath: String): Boolean = {
  try
  {
    dbutils.fs.ls(filepath)
    return true
  }
  catch
  {
    case e: Exception => {
      if (e.getMessage.contains("The specified path does not exist.")) {
        return false
      } else {
        e.printStackTrace
        return false
      }
    }
  }
}

// COMMAND ----------

// MAGIC %md
// MAGIC ### Creación de archivos

// COMMAND ----------

def createNewFile(filepath: String, contents: String): Boolean = {
  try{
    if(exists_file(filepath)){
      return false
      }
    else{
      dbutils.fs.put(filepath, contents)
      return true
      }
  }
  catch {
    case e: Exception => e.printStackTrace
      return false
  }
}

// COMMAND ----------

// MAGIC %md
// MAGIC ### Eliminación de archivo

// COMMAND ----------

def delete(filepath: String, recurse:Boolean=true) : Boolean = {
  try
  {
    dbutils.fs.rm(filepath, recurse=recurse)
    return true
  }
  catch
    { case e: Exception => e.printStackTrace
      return false
    }
}

// COMMAND ----------

// MAGIC %md
// MAGIC ### Crear y leer archivos txt

// COMMAND ----------

def makeTxtFile(pathtxt: String, content: String): Boolean ={
  try{
    dbutils.fs.put(pathtxt, content)
    return true
  }
  catch {
    case e: Exception => e.printStackTrace
      return false
  }
}

def readTxtFile(pathtxt: String): String= {
  try{
    return dbutils.fs.head(pathtxt)
  }
  catch {
    case e: Exception => e.printStackTrace
      return null
  }
}

// COMMAND ----------

// MAGIC %md
// MAGIC ### Creación directorios

// COMMAND ----------

def mkdir(dirPath: String) : Boolean = {
  try{
    dbutils.fs.mkdirs(dirPath)
    return true
  }
  catch {
    case e: Exception => e.printStackTrace
      return false
  }
}

// COMMAND ----------

// MAGIC %md
// MAGIC ### Listar archivos

// COMMAND ----------

def listFiles (abfsPath: String) : Seq[com.databricks.backend.daemon.dbutils.FileInfo] = {
  try
  {
    return dbutils.fs.ls(abfsPath)
  }
  catch
  {
    case e: Exception => e.printStackTrace
      return null
  }
}

// COMMAND ----------

// MAGIC %md
// MAGIC ### Obtener tamaño de archivos

// COMMAND ----------

def sizeFile (abfsPath: String) : Long = {
  return listFiles(abfsPath).map(_.size).sum
}

// COMMAND ----------

// MAGIC %md
// MAGIC ### Tamaño y cantidad de archivos recientemente generados

// COMMAND ----------

def size_and_count_file (abfsPath: String) : Seq[Long] = {
  val list = listFiles(abfsPath)
  val last = list.map(_.modificationTime).max
  val filtered = list.filter(_.modificationTime == last)

  return Seq(filtered.map(_.size).sum, filtered.size)
}

// COMMAND ----------

// MAGIC %md
// MAGIC ### Mover archivos

// COMMAND ----------

def moverArchivoAbfs (pathOrigen: String, pathDestino: String, recurse:Boolean=true) : Boolean = {
  try
  {
    dbutils.fs.mv(pathOrigen, pathDestino, recurse=recurse)
    return true
  }
  catch
  {
    case e: Exception => e.printStackTrace
      return false
  }
}

// COMMAND ----------

// MAGIC %md
// MAGIC ### Lectura de archivos

// COMMAND ----------

def openFile(filepath: String): String = {
  try {
    val df = spark.read.textFile(filepath)
    val text = df.collect().mkString("\n")
    
    return text
  } catch {
    case e: Exception =>
      e.printStackTrace()
      return ""
  }
}

// COMMAND ----------

// MAGIC %md
// MAGIC ### Contar archivos de forma recursiva

// COMMAND ----------

def countFiles(path_files: String): Long = {
  var n_count = 0L

  for (i <- listFiles(path_files)) {
    if (i.isFile) {
      n_count += 1
    } else {
      n_count += countFiles(i.path)
    }
  }
  return n_count
}

// COMMAND ----------

// MAGIC %md
// MAGIC ### Eliminación archivos dentro de una carpeta

// COMMAND ----------

// actualizada
def delete_files(filepath: String, recurse:Boolean=true) : Boolean = {

  val file_names = listFiles(filepath)

  try{

    for (i <- file_names){

      var file_name = i.name
      dbutils.fs.rm(filepath + file_name, recurse=recurse)    
    }
    return true
  }
  catch
    { case e: Exception => e.printStackTrace
      return false
    }
}
