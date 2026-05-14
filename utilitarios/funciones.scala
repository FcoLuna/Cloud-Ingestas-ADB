// Databricks notebook source
import java.io.{BufferedReader, FileInputStream, InputStreamReader}
import java.text.SimpleDateFormat
import java.util.Calendar

// COMMAND ----------

// Función que busca el path del archivo de configuración
def getConfigPath: String = {

  // Get path of current file
  var path = dbutils.notebook().getContext().notebookPath.get

  // Split the string by the backslash character
  val parts = path.split("/")

  // Take the first three segments and join them back with the backslash
  // Note: The first element in the array will be an empty string because the path starts with a backslash
  val beforeThirdSlash = if (parts.length > 4) parts.take(4).mkString("/") else path
  
  val rutaConfig = "/Workspace" + beforeThirdSlash + "/utilitarios/config.properties"
  return rutaConfig
}

//PROCEDIMIENTO QUE VALIDA SI UN DIRECTORIO EXISTE
def testDirExist(path: String): Boolean = {
  try {
    // Verificar si el listado de la ruta no está vacío y el primer elemento es un directorio
    dbutils.fs.ls(path).nonEmpty && dbutils.fs.ls(path)(0).isDir
  } catch {
    // Capturar la excepción en caso de que la ruta no exista y devolver false
    case e: Exception => 
      false
  }
}
//PROCEDIMIENTO QUE RESTA DIAS A LA FECHA ACTUAL (POR DEFECTO DEBE VENIR 1)
def getFecha_dia (diaproceso: Int, format:String, cal: Calendar) : String = {
  cal.add(Calendar.DATE,diaproceso)
  val format = new SimpleDateFormat("yyyy-MM-dd")
  val fecha = format.format(cal.getTime)
  fecha
}

//PROCEDIMIENTO QUE RESTA MESES A LA FECHA ACTUAL (POR DEFECTO DEBE VENIR 0)
def getFecha_mes (mesproceso: Int, format:String, cal: Calendar) : String = {
  cal.add(Calendar.MONTH,mesproceso)
  val format = new SimpleDateFormat("yyyy-MM-dd")
  val fecha = format.format(cal.getTime)
  fecha
}

//PROCEDIMIENTO QUE BORRA PARTICIONES EN HDFS
def deletePartitions(path_data2_seq: Seq[String]): Unit = {
  for (i <- 0 until path_data2_seq.length) {
    dbutils.fs.rm(path_data2_seq(i).toString(), true)
  }
}

//PROCEDIMIENTO QUE BUSCA FECHAS VALUDAS
def filtro_fecha (yearmonth: Int): Int = {
  val dataSize = 201912
  var fecha = 0
  if (yearmonth > dataSize) {
    fecha = yearmonth
  } else {
    fecha = 202001
  }
  fecha
}
