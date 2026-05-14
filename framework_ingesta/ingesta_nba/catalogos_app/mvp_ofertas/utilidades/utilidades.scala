// Databricks notebook source
import org.apache.hadoop.fs.Path
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.SparkSession

import scala.collection.mutable
import scala.io._
import scala.sys.process._

// COMMAND ----------

object utilidades {
    var exitFn: () => Unit = () => System.exit(-1)
    
    def printHeader(problem: String) = println(("*" * (problem.length + 2)) + s"\n* $problem\n" + ("*" * (problem.length + 2)))
    
  /** 
    * Funcion para construir ruta absoluta de archivos necesarios en el proceso (sql, config)
    * @param relative_path Ruta relativa con el archivo a leer
    * @return String con la ruta absoluta
  */
  def getPath(relative_path : String): String = {

    dbutils.notebook.getContext.notebookPath match {
      case Some(path) =>
        // Extraer la ruta hasta el ultimo '/' 
        val lastSlashIndex = path.lastIndexOf('/') 
        // Extraer la ruta hasta el penultimo '/'
        val secondLastSlashIndex = path.substring(0, lastSlashIndex).lastIndexOf('/')
        val basePath = path.substring(0, secondLastSlashIndex)
        // Concatenar con la subruta hacia el directorio relativo
        s"/Workspace$basePath$relative_path"
      case None =>
        throw new IllegalStateException("No se pudo obtener la ruta del notebook")
    }
  }   

    def printTopic(s: String) {
//        println(s"\n${DateUtils.getTodayTimeStamp} - ${s}")
        //println(s"\n$s")
        println(genUnderlines(s))
    }
    
    def genUnderlines(s: String) = s.map(_ => "-").mkString
    
    def isEmpty(x: String) = x == null || x.trim.isEmpty
    
    def ReadCSVProvideSchema(spark: SparkSession, path: String, delimiter: String = ",", header: String = "false", csvSchema: StructType): DataFrame = {
        
        if (header == "false") {
            spark.read
              .format("com.databricks.spark.csv")
              .option("sep", delimiter)
              .schema(csvSchema)
              .load(path)
        } else {
            spark.read
              .format("com.databricks.spark.csv")
              .option("sep", delimiter)
              .option("header", header)
              .schema(csvSchema)
              .load(path)
        }
    }
    
    def Df_From_X(sparkSession: SparkSession, sFormat: String, sSchema: StructType, sPath: String): DataFrame = {
        var df: DataFrame = null
        val formats = List("parquet", "csv", "txt", "json", "jdbc", "orc", "avro", "bz2")
        if (isEmpty(sFormat)) {
            df = sparkSession.read.format("parquet").load(sPath)
            
        } else if ((sFormat == "csv") || (sFormat == "json")) {
            df = sparkSession.read.format(sFormat)
              .option("header", "true")
              .schema(sSchema)
              .load(sPath)
        } else {
            if (sSchema == null)
                df = sparkSession.read.load(sPath)
            else
                df = sparkSession.read.schema(sSchema).load(sPath)
        }
        df.show(10, false)
        df
    }
    
    // Reading ORC
    def Df_From_Orc(path_orc: String, sparkSession: SparkSession): DataFrame = {
        val df = sparkSession.read.format("orc").load(path_orc)
        df.show(10, false)
        df
    }
    
    // Writing DataFrame to ORC
    def Orc_From_Df(df: DataFrame, path_orc: String): Unit = {
        df.write.format("orc")
          .mode("overwrite")
          .option("compression", "snappy")
          .save(path_orc)
    }
    
    def Df_From_Csv(sparkSession: SparkSession, csvFile: String): DataFrame = {
        val df = sparkSession.read.format("csv")
          .option("inferSchema", "true")
          .option("header", "true")
          .load(csvFile)
        df
    }
    
    def getLastFile(path_hdfs: String, filter: String = "parquet"): String = {
        printHeader(s"Buscando informacion en $path_hdfs - filter: $filter")
        val lsFilePath = ((Seq("hadoop", "fs", "-ls", "-R", path_hdfs) #| Seq("grep", filter) #| Seq("sort", "-k6,7") #| Seq("tail", "-1") #| Seq("awk", "{print $8}")).!!).replace("\n", "")
        lsFilePath
    }
    
    def getLastDir(path_hdfs: String): String = {
        printHeader(s"Buscando informacion en $path_hdfs")
        val lsFilePath = ((Seq("hadoop", "fs", "-ls", "-R", path_hdfs) #| Seq("sort", "-k6,7") #| Seq("tail", "-1") #| Seq("awk", "{print $8}")).!!).replace("\n", "")
        lsFilePath
    }
    
    def moverArchivo(path_hdfs: String, dest: String): Int = {
        printHeader(s"Creando archivo $dest en $path_hdfs")
        try {
            val lsFilePath = ((Seq("hadoop", "fs", "-ls", "-R", path_hdfs) #| Seq("awk", "{print $8}")).!!).replace("\n", " ")
            val tempFileList = lsFilePath.split(" ").filter(x => (x.contains(".parquet")))
            
            for (filename <- tempFileList) {
                println(s"\nfilename: $filename\n")
                val cmd = "hadoop fs -mv " + filename + " " + path_hdfs + "/" + dest
                val output = cmd !
            }
        } catch {
            case e: Exception => return 1
        }
        return 0
    }
    
    def removeFiles(sfile: String) = {
        printHeader(s"Borrando $sfile")
        var cmd = 0
        try {
            cmd = Seq("hdfs", "dfs", "-rm", sfile).!
            printTopic(s"Resultado del borrado: $cmd")
        } catch {
            case e: Exception => {
                printTopic(s"Ocurrio un error al borrar el archivo $sfile\n" + e)
            }
        }
    }
    
    def runShellCommand(command: String): Int = {
        command !;
    }
    
    def runShellCommand(commandSeq: Seq[String]): Int = {
        commandSeq !;
    }
    
    // Metodo para crear esquema
    def createSchema(paths: String): org.apache.spark.sql.types.StructType = {
        val fields = paths.split(",")
        var schema_out = new StructType()
        for (i <- 0 until fields.size) {
            schema_out = schema_out.add(StructField(fields(i), StringType, true))
        }
        schema_out
    }
    
    // metodo para leer contenido de un archivo de texto
    def leerArchivoTXT(filename: String): String = {
        return Source.fromFile(filename).getLines.mkString
    }
    
    def printErrorAndExit(str: String): Unit = {
        println("[ERROR] " + str)
        exitFn()
    }
    
    def printWarning(str: String) = println("[WARN] " + str)
    
    def printAndExit(s: String): Unit = {
        println(s)
        System.exit(1)
        throw new RuntimeException("Unreachable!")
    }
    /*
    def numPartitionsCalc(fs: org.apache.hadoop.fs.FileSystem, tmp: String): Int = {
        var dataSize = fs.getContentSummary(new Path(tmp)).getLength
        var numPartitions = 0
        numPartitions = Math.ceil(dataSize / 1073741824L).toInt
        numPartitions = if (numPartitions == 0) 1 else numPartitions
        return numPartitions
    } ***/
    def Esnulo(svalue: String): Boolean = {
        var bretval = true
        if (svalue != null && (!svalue.equals(""))) {
            bretval = false
        }
        bretval
    }
    
    /*
    Leer un archivo con las siguiente estructura
    AAAA,AAA
    5,GGGFF
     */
    def readFile2Map4String(cls: Class[_], file: String): mutable.HashMap[String, String] = {
        val path = cls.getClassLoader.getResource(file).getPath
        val lines = Source.fromFile(path).getLines()
        val map = new mutable.HashMap[String, String]()
        lines.foreach(elem => {
            val arr = elem.split(",")
            map.put(arr(0), arr(1))
        })
        map
    }
    
    /*
    Leer un archivo con las siguiente estructura desde hdfs
    AAAA,AAA
    5,GGGFF
     */
     /*
    def readFile2Map4StringFromSpark(spark: SparkSession, fileName: String): mutable.HashMap[String, String] = {
        val map = new mutable.HashMap[String, String]()
        val path = s"hdfs://bdpcluster/ori/map/${fileName}"
        val lines: Array[String] = spark.textFile(path).collect()
        lines.foreach(elem => {
            val arr = elem.split(",")
            map.put(arr(0), arr(1))
        })
        map
    }*/

    def getSecret(scope: String, secretKey: String): String = {   
		/**    * Obtiene el password del secreto del key vault de Azure.    *    
				* @param scope Scope de Azure.    
				* @param secretKey Clave del secreto en Azure.    
				* @return Contraseña del secreto.    
		*/  

		val password = dbutils.secrets.get(scope, secretKey)   
		password 
	}
}
