// Databricks notebook source
spark.conf.set("spark.databricks.delta.optimizeWrite.enabled", "true")
spark.conf.set("spark.databricks.delta.autoCompact.enabled", "true")
spark.conf.set("spark.sql.files.maxPartitionBytes", "64m") 
spark.conf.set("spark.sql.files.openCostInBytes", "4m")

spark.conf.set("spark.sql.shuffle.partitions", "200")
spark.conf.set("spark.databricks.delta.concurrentWrites", "true")

// COMMAND ----------

import scala.util.matching.Regex
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

val stagePath = s"$adls_path/$dir_adls_rel/stage_PLAT"

// Listar los archivos CSV en la carpeta usando dbutils
val files = dbutils.fs.ls(stagePath)
  .filter(file => file.name.endsWith(".csv")) 
  .map(file => file.path)

// COMMAND ----------

val filesByFamily = files.groupBy { filePath =>
  val filename = filePath.replace("_ITBBU", "").split("UMEID_")(1)
  filename.split("_")(0) 
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
    val technology = "PLAT"
    val fullPath = s"$adls_path/$dir_adls_rel/${technology}_$family/raw"
    val tabla_salida = catalogo + "." + esquema + ".ZTE_" + technology + "_" + family

    println(s"Escribiendo datos en la ruta: $fullPath y tabla: $tabla_salida")

    Try {
      if (!spark.catalog.tableExists(tabla_salida)) {
        df.repartition(10)
          .write
          .mode("overwrite")
          .format("delta")
          .option("path", fullPath)
          .partitionBy("year", "month", "day")
          .saveAsTable(tabla_salida)
      } else {
        df.repartition(10)
          .write
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

  val deletePLAT = s"$adls_path/$dir_adls_rel/stage_PLAT"
  println(deletePLAT)
  dbutils.fs.rm(deletePLAT, true)
