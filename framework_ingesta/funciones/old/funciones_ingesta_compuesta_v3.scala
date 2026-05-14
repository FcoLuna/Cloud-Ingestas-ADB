// Databricks notebook source
import java.text.SimpleDateFormat
import org.apache.hadoop.fs.FileStatus
import java.time.format.DateTimeFormatter
import java.time.LocalDateTime
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import java.time.LocalDate
import java.time.format.DateTimeFormatter

// COMMAND ----------

// MAGIC %run /Shared/framework_ingesta/funciones/Manejo_ADLS

// COMMAND ----------

// INGESTA SIMPLE V3

def obtenerParticiondesdeArchivo(mensaje_nifi: String, formato_entrada: String, formato_salida: String): String = {
      var contador = 0
      for (i <- 0 until mensaje_nifi.split("_").size) {
        if (mensaje_nifi.split("_")(i).replaceAll("[^0-9]", "") != "") {
          contador = contador + 1
        }
      }
      var datex = ""
      if (contador > 1) {
        datex = mensaje_nifi.split("_")(mensaje_nifi.split("_").length - 1).replace("[^0-9]", "").substring(0, 8)
      } else if (contador == 1) {
        datex = mensaje_nifi.replaceAll("[^0-9]", "").substring(0, formato_entrada.length())
      }
      val inputFormat = new SimpleDateFormat(formato_entrada)
      val outputFormat = new SimpleDateFormat(formato_salida)
      return outputFormat.format(inputFormat.parse(datex))
    }

// COMMAND ----------

// INGESTA SIMPLE V3 (ACTUALIZADO)
def parquetPutName(status: Seq[com.databricks.backend.daemon.dbutils.FileInfo], dataType: String, plataforma: String, categoria: String, loadType: String, periodicity: String): Unit = {
  val current = DateTimeFormatter.ofPattern("yyyyMMddHHmmss").format(LocalDateTime.now)
  for (i <- 0 to status.length-1) {
    val fileStatus = status(i)
    if (fileStatus.isDir) {
      val subStatus = listFiles(fileStatus.path)
      println("directory:" + fileStatus.path)
      parquetPutName(subStatus, dataType, plataforma, categoria, loadType, periodicity)
    } else if (fileStatus.path.toString.contains("_SUCCESS") != true && fileStatus.path.toString.contains("part-")) {
      val new_path = fileStatus.path.toString.substring(0, fileStatus.path.toString.indexOf("part")) + dataType + "." + plataforma + "." + categoria + "." + loadType + "." + periodicity + "." + current + ".N" + i + ".snappy.parquet"
      println("file:" + fileStatus.path.toString)
      println("filesub" + new_path)
      moverArchivoAbfs(fileStatus.path.toString, new_path)
    }

  }
}

// COMMAND ----------

// INGESTA SIMPLE V3 (ACTUALIZADO)
def numPartitionsCalc(tmp: String): Int = {
 
  var dataSize = (listFiles(tmp).map(_.size).sum)
  var numPartitions = 0
  if (dataSize < 10737418240L) {
    // println(dataSize.getClass.getSimpleName)
    numPartitions = scala.math.ceil(dataSize / 1073741824L).toInt
    numPartitions = if (numPartitions == 0) 1 else numPartitions
  } else {
    numPartitions = scala.math.ceil(dataSize / 10737418240L).toInt
    numPartitions = if (numPartitions == 0) 1 else numPartitions
  }
  return numPartitions
}

// COMMAND ----------

/* FUNCION QUE RECORRE EL LISTADO DE DIRECTORIOS A CARGAR EN HDFS, ELIMINANDO RUTAS QUE YA EXISTAN */
def deletePartitions(path_data2_seq: Seq[String]): Unit = {
  for (i <- 0 until path_data2_seq.length) {
    delete(path_data2_seq(i).toString, true)
  }
}
