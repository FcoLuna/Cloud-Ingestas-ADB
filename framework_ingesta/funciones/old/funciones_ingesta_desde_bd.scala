// Databricks notebook source
import java.text.SimpleDateFormat
import org.apache.hadoop.fs.FileStatus
import java.time.format.DateTimeFormatter
import java.time.LocalDateTime
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem

// COMMAND ----------

// MAGIC %run /Shared/framework_ingesta/funciones/Manejo_ADLS_v2

// COMMAND ----------

  /*FUNCION QUE TOMA LOS ARCHIVOS PARQUET GENERADOS Y LES MODIFICA EL NOMBRE SEGUN NOMENCLATURA DE ARQUITECTURA
    * EJ: MODIFICA LOS ARCHIVOS PARQUET GENERADOS COMO PART-00001.SNAPPY A raw.interacciones.sap.i.d.20200801205689.snappy
    * RECIBE UN ARREGLO CON EL LISTADO DE ARCHIVOS,DIRECTORIOS Y SUBDIRECTORIOS EXISTENTES DONDE SE REALIZA LA INGESTA EN HDFS
    * ITERANDO DE FORMA RECURSIVA BUSCA CUALQUIER ELEMENTO QUE SEA UN ARCHIVO Y QUE SU NOMBRE COMIENCE CON "part-*", LUEGO HACE UN MV
    * A LA MISMA RUTA CAMBIANDOLE EL NOMBRE CON LOS PARAMETROS INDICADOS
    */

def parquetPutName(status: Seq[com.databricks.backend.daemon.dbutils.FileInfo], dataType: String, plataforma: String, categoria: String, loadType: String, periodicity: String): Unit = {
  val current = DateTimeFormatter.ofPattern("yyyyMMddHHmmss").format(LocalDateTime.now)
  for (i <- 0 to status.length-1) {
    val fileStatus = status(i)
    println ("PRUEBAAAAA fileStatus " + fileStatus)
    if (fileStatus.isDir) {
      val subStatus = listFiles(fileStatus.path)
      println("directory:" + fileStatus.path)
      parquetPutName(subStatus, dataType, plataforma, categoria, loadType, periodicity)
    } else if (fileStatus.path.toString.contains("_SUCCESS") != true && fileStatus.path.toString.contains("part-")) {
      val new_path = fileStatus.path.toString.substring(0, fileStatus.path.toString.indexOf("part")) + dataType + "." + plataforma + "." + categoria + "." + loadType + "." + periodicity + "." + current + ".N" + i + ".snappy.parquet"
      println ("PRUEBAAAAA new_path " + new_path)
      println("file:" + fileStatus.path.toString)
      println("filesub" + new_path)
      moverArchivoAbfs(fileStatus.path.toString, new_path)
    }

  }
}
// def parquetPutName(status: Array[FileStatus]): Unit = {
//     val current = DateTimeFormatter.ofPattern("yyyyMMddHHmmss").format(LocalDateTime.now)
//     for (i <- status.indices) {
//     val fileStatus = status(i)
//     if (fileStatus.isDirectory) {
//         val conf = new Configuration();
//         val filesystem = FileSystem.get(fileStatus.getPath().toUri(), conf)
//         val subStatus = filesystem.listStatus(fileStatus.getPath())
//         println("directory:" + fileStatus.getPath())
//         parquetPutName(subStatus)
//     } else if (fileStatus.getPath.toString.contains("_SUCCESS") != true && fileStatus.getPath.toString.contains("part-")) {
//         println("     file:" + fileStatus.getPath().toString)
//         println("     filesub" + fileStatus.getPath.toString.substring(0, fileStatus.getPath.toString.indexOf("part")) + dataType + "." + plataforma + "." + categoria + "." + loadType + "." + periodicity + "." + current + ".N" + i + ".snappy.parquet")
//         fs.rename(new Path(fileStatus.getPath.toString), new Path(fileStatus.getPath.toString.substring(0, fileStatus.getPath.toString.indexOf("part")) + dataType + "." + plataforma + "." + categoria + "." + loadType + "." + periodicity + "." + current + ".N" + i + ".snappy.parquet"))
//     }
//     }
// }

// COMMAND ----------

 /*
 * FUNCION QUE CALCULA CUANTOS ARCHIVOS PARQUET DEBEN GENERARSE COMO SALIDA GENERANDO ARCHIVOS DE TAMAÑO SUPERIOR A 128 MB
 * (PESO MINIMO RECOMENDADO PARA EL CLUSTER).
 * SE ESCRIBE LA DATA A UN DIRECTORIO TEMPORAL, SE OBTIENE EL PESO TOTAL EN BYTES DE LA DATA ESCRITA, SE APLICA LA FUNCION MATH.CEIL
 * LA CUAL RECIBE EL PESO Y CALCULA CELDAS CERCANAS O SUPERIORES A 128 MB, OBTENIENDO COMO SALIDA LA CANTIDAD DE CELDAS A GENERAR.
 * ESTO SE ALMACENA EN LA VARIABLE numPartitions LA CUAL INDICA CUANTOS ARCHIVOS DE SALIDA SE CREARAN CON EL PESO MINIMO.
 */

def numPartitionsCalc(tmp: String): Int = {
    var dataSize = (listFiles(tmp).map(_.size).sum)
    var numPartitions = 0
    numPartitions = scala.math.ceil(dataSize / 1073741824L).toInt
    numPartitions = if (numPartitions == 0) 1 else numPartitions
    return numPartitions
  }

// COMMAND ----------

  //Falta Actualizar
  
  /* FUNCION QUE RECORRE EL LISTADO DE DIRECTORIOS A CARGAR EN HDFS, ELIMINANDO RUTAS QUE YA EXISTAN */
def deletePartitions(path_data2_seq: Seq[String]): Unit = {
    for (i <- 0 until path_data2_seq.length) {
    dbutils.fs.rm(path_data2_seq(i).toString())
    }
    }

// COMMAND ----------

// # Eliminar un archivo

// dbutils.fs.rm("/FileStore/mydata/myfile.csv")

// # Eliminar un directorio y todo su contenido (de forma recursiva)

// dbutils.fs.rm("/FileStore/mydata/", recurse=True)
