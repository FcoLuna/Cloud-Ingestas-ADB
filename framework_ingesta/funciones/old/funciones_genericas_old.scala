// Databricks notebook source
import java.text.SimpleDateFormat
import org.apache.hadoop.fs.FileStatus
import java.time.format.DateTimeFormatter
import java.time.LocalDateTime
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.spark.sql.DataFrame
import io.delta.tables._
import org.apache.spark.sql.DataFrame

// COMMAND ----------

// MAGIC %md
// MAGIC ###Probar existencia de archivos en ADLS

// COMMAND ----------

// actualizado
def exists_file(filepath: String): Boolean = {
  try
  {
    dbutils.fs.ls(filepath)
    return true
  }
  catch
  {
    // case e: FileNotFoundException   => println("Couldn't find that file.")
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
// MAGIC ### Probar creación de archivos

// COMMAND ----------

// Actualizado
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
// MAGIC ### Probar listar archivos

// COMMAND ----------

// actualizada
def listFiles (abfsPath: String) : Seq[com.databricks.backend.daemon.dbutils.FileInfo] = {
  try
  {
    // return dbutils.fs.ls(abfsPath).map(_.name)
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
// MAGIC ### Probar eliminación de archivo

// COMMAND ----------

// actualizada
def delete(filepath: String, recurse:Boolean=true) : Boolean = {

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

// COMMAND ----------

// MAGIC %md
// MAGIC ### Crear y leer archivos txt

// COMMAND ----------

// nueva
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
// dbutils.fs.put("abfss://contenedor-test@stbigdatadev02.dfs.core.windows.net/data/interacciones/asignacion_ofertas/usuario_mkt/activacion_disney_fija/last_ingest_time.txt", "prueba")

// nueva
def readTxtFile(pathtxt: String): String= {
  try{
    return dbutils.fs.head(pathtxt)
  }
  catch {
    case e: Exception => e.printStackTrace
      return null
  }
}
// dbutils.fs.head("abfss://contenedor-test@stbigdatadev02.dfs.core.windows.net/data/interacciones/asignacion_ofertas/usuario_mkt/activacion_disney_fija/last_ingest_time.txt")

// COMMAND ----------

// MAGIC %md
// MAGIC ### Probar creación directorios

// COMMAND ----------

// actualizada
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
// MAGIC ### Obtener tamaño de archivos

// COMMAND ----------

// Nuevo
def sizeFile (abfsPath: String) : Long = {
  return listFiles(abfsPath).map(_.size).sum
}

// COMMAND ----------

// MAGIC %md
// MAGIC ### Probar mover archivos

// COMMAND ----------

// actualizada
def moverArchivoAbfs (pathOrigen: String, pathDestino: String, recurse:Boolean=true) : Boolean = {
  try
  {
    //abfs.rename(new Path(pathOrigen), new Path(pathDestino))
    dbutils.fs.mv(pathOrigen, pathDestino, recurse=recurse)
    //delete(pathOrigen)
    //dbutils.fs.cp(pathOrigen, pathDestino, recurse=true)
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
// MAGIC ### Probar lectura de archivos

// COMMAND ----------

// falta
def openFile(filepath: String): String = {
  try {
    // Leer el archivo como un DataFrame
    val df = spark.read.textFile(filepath)

    // Convertir el DataFrame a una cadena
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
// MAGIC ### Función que busca archivos que comiencen con "raw.." o "noraw.." dependiendo del dataType

// COMMAND ----------

// actualizada
def findFile(list_files: Seq[com.databricks.backend.daemon.dbutils.FileInfo], dataType: String): String = {
  var result: String = null
  for (file <- list_files) {
    if (file.name.startsWith(dataType)){
      result = file.name
      return result
    }
  }
  return result
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
// MAGIC ### Probar guardado de DF a archivo de texto

// COMMAND ----------

// falta probar
def guardarArchivoTexto (dataframe: DataFrame, pathDestino: String) : Boolean = {
  try
  {
    dataframe.write.save(pathDestino)
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
// MAGIC ### Probar guardado de DF a JSON

// COMMAND ----------

// falta probar
def guardarArchivoJSON (dataframe: DataFrame, pathDestino: String) : Boolean = {
  try
  {
    dataframe.write.format("json").save(pathDestino)
  //      dataframe.toJSON.saveAsTextFile(pathDestino)
 
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
// MAGIC ### Obtener particiones desde archivo

// COMMAND ----------

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

// MAGIC %md
// MAGIC ### Poner nombre al archivo parquet

// COMMAND ----------

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

// MAGIC %md
// MAGIC ### Calcular particiones

// COMMAND ----------

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

// DUDA SI BORRAR ESTA FUNCIÓN
// def numPartitionsCalc(tmp: String): Int = {
//     var dataSize = (listFiles(tmp).map(_.size).sum)
//     var numPartitions = 0
//     numPartitions = scala.math.ceil(dataSize / 1073741824L).toInt
//     numPartitions = if (numPartitions == 0) 1 else numPartitions
//     return numPartitions
//   }

// COMMAND ----------

// MAGIC %md
// MAGIC ### Eliminar particiones

// COMMAND ----------

def deletePartitions(path_data2_seq: Seq[String]): Unit = {
    for (i <- 0 until path_data2_seq.length) {
    dbutils.fs.rm(path_data2_seq(i).toString())
    }
  }

// COMMAND ----------

// MAGIC %md
// MAGIC ### Definir schema Json para DF

// COMMAND ----------

//Función para definir schema del dataframe a procesar 
def defineSchemaJson(pathJson: String, name_schema: String, path_df: String): DataFrame = {

  val dfConverted = spark.read.option("multiline", "true").json(pathJson + name_schema).drop("type")
  val flatList = dfConverted.select(explode($"fields").as("fieldsFlat")).collect().toList.flatMap(row => row.toSeq)
  val df = spark.read.format("parquet").load(path_df)

  var df2 = df  // Dataframe

  for (elem <- flatList) {
    val row = elem.asInstanceOf[Row]
    val columnName = row.getString(0)
    val tipo = row.getString(2)

    // El DF debe tener la misma estructura que el json, luego se procede a cambiar el tipo de la columna
    df2 = df2.withColumn(columnName, col(columnName).cast(tipo))
  
  // println(s"columnName: $columnName, Tipo: $tipo")
  // df2.printSchema()
  }
  return df2
}

// COMMAND ----------

// MAGIC %md
// MAGIC ### MERGE DATA

// COMMAND ----------

def mergeData(destination_path: String, merge_string: String, df: DataFrame): Boolean = {

  try{
    println("Modo Merge")
    // Crea las DeltaTable a partir de las rutas de Delta Lake
    val deltaTable = DeltaTable.forPath(spark, destination_path)
    
    // Realiza la operación MERGE INTO
    deltaTable.alias("a")
            .merge(df.alias("b"),  merge_string)
            .whenMatched().updateAll()
            .whenNotMatched().insertAll()
            .execute()
          
    true
            
  }catch{
   case e: Exception =>
      println("Modo Write")
      df.write.format("delta")
              .mode("overwrite")
              .option("mergeSchema", "true")
              .option("overwriteSchema", "true")
              .save(destination_path)

      false 
  }
}

// COMMAND ----------

// MAGIC %md
// MAGIC ### Obtener Año/Mes/Dia de un nombre de arhivo

// COMMAND ----------


