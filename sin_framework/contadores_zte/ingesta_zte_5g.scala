// Databricks notebook source
spark.conf.set("spark.databricks.delta.optimizeWrite.enabled", "true")
spark.conf.set("spark.databricks.delta.autoCompact.enabled", "true")
spark.conf.set("spark.sql.files.maxPartitionBytes", "64m") 
spark.conf.set("spark.sql.files.openCostInBytes", "4m")

spark.conf.set("spark.sql.shuffle.partitions", "200")
spark.conf.set("spark.databricks.delta.concurrentWrites", "true")

// COMMAND ----------

import scala.util.matching.Regex
import org.apache.spark.sql.types._
import scala.util.Try

// COMMAND ----------

//dbutils.widgets.text("dir_adls_rel", "data/trafico/senalizacion/contadores_zte")
//dbutils.widgets.text("catalogo", "bi_ingestas")
//dbutils.widgets.text("esquema", "raw_trafico") 
//dbutils.widgets.text("adls_path", "abfss://ingestas@stbigdatadev02.dfs.core.windows.net")

// COMMAND ----------

val adls_path = dbutils.widgets.get("adls_path")
val dir_adls_rel = dbutils.widgets.get("dir_adls_rel")
val catalogo = dbutils.widgets.get("catalogo")
val esquema = dbutils.widgets.get("esquema")

// COMMAND ----------

val expectedSchemaCELLPRACH = StructType(Seq(
    StructField("COLLECTTIME", LongType, true),
    StructField("SBNID", StringType, true),
    StructField("MEID", IntegerType, true),
    StructField("MENAME", StringType, true),
    StructField("eNBID", IntegerType, true),
    StructField("ENODEBNAME", StringType, true),
    StructField("CellID", IntegerType, true),
    StructField("CellNAME", StringType, true),
    StructField("C373444750", IntegerType, true),
    StructField("C373444751", IntegerType, true),
    StructField("C373444752", IntegerType, true),
    StructField("C373444753", IntegerType, true),
    StructField("C373444754", IntegerType, true),
    StructField("C373444757", IntegerType, true),
    StructField("C373444758", IntegerType, true),
    StructField("C373444759", IntegerType, true),
    StructField("C373444798", IntegerType, true),
    StructField("C373444799", IntegerType, true),
    StructField("C373444880", IntegerType, true),
    StructField("C373444881", IntegerType, true),
    StructField("C373444887", IntegerType, true),
    StructField("C373444888", IntegerType, true),
    StructField("C373444889", IntegerType, true),
    StructField("C373444890", IntegerType, true)
))

// COMMAND ----------

val stagePath = s"$adls_path/$dir_adls_rel/stage_5G"

// Listar los archivos CSV en la carpeta usando dbutils
val files = dbutils.fs.ls(stagePath)
  .filter(file => file.name.endsWith(".csv"))
  .map(file => file.path.toString)

// COMMAND ----------

// Agrupar los archivos por familia
val filesByFamily = files.groupBy { filePath =>
  val filename = filePath.replace("_ITBBU", "") // Quitar "_ITBBU" del nombre
  try {
    filename.split("UMEID_")(1).split("_")(0) // Extraer la familia
  } catch {
    case e: Exception =>
      println(s"Error al procesar el archivo $filePath: ${e.getMessage}")
      "UNKNOWN" // Marca archivos problemáticos con una familia "UNKNOWN"
  }
}

// COMMAND ----------

filesByFamily.foreach { case (family, filePaths) =>
  println(s"Procesando familia: $family")

  // Leer y combinar los archivos de la misma familia ignorando los problemáticos
  val combinedDF = filePaths.flatMap { filePath =>
    Try {
      // Intentar leer el archivo
      spark.read.option("header", true).option("inferSchema", true).csv(filePath)
    }.toOption.orElse {
      // Registrar y omitir si ocurre un error
      println(s"Ignorando archivo con error: $filePath")
      None
    }
  } match {
    case Nil =>
      println(s"No se encontraron archivos válidos para la familia $family.")
      None
    case validFiles =>
      // Combinar los archivos válidos
      Some(validFiles.reduce(_ union _)
        .withColumn("bigdata_close_date", current_date())
        .withColumn("bigdata_ctrl_id", expr("unix_timestamp()").cast("long"))
        .withColumn("year", substring(col("COLLECTTIME"), 1, 4))
        .withColumn("month", substring(col("COLLECTTIME"), 5, 2))
        .withColumn("day", substring(col("COLLECTTIME"), 7, 2))
      )
  }

  // Escribir en Delta si hay datos válidos
  combinedDF.foreach { df =>
    val technology = "5G"
    val fullPath = s"$adls_path/$dir_adls_rel/${technology}_$family/raw"
    val tabla_salida = catalogo + "." + esquema + ".ZTE_" + technology + "_" + family

    println(s"Escribiendo datos en la ruta: $fullPath y tabla: $tabla_salida")

    Try {
      if (!spark.catalog.tableExists(tabla_salida)) {
        df.write
          .mode("overwrite")
          .format("delta")
          .option("path", fullPath)
          .partitionBy("year", "month", "day")
          .saveAsTable(tabla_salida)
      } else {
        df.write
          .mode("append")
          .format("delta")
          .option("path", fullPath)
          .insertInto(tabla_salida)
      }
    }.recover {
      case e: Exception =>
        println(s"Error al escribir en la tabla $tabla_salida: ${e.getMessage}")
    }
  }
}

// COMMAND ----------

  val delete5G = s"$adls_path/$dir_adls_rel/stage_5G"
  println(delete5G)
  dbutils.fs.rm(delete5G, true)
