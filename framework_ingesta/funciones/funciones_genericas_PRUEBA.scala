// Databricks notebook source
// MAGIC %md
// MAGIC ### Librerías

// COMMAND ----------

import org.apache.spark.sql.DataFrame
import java.text.SimpleDateFormat
import io.delta.tables._

// Añadido por bug de no encuentra Calendar camilo 11-01
import java.util.Calendar

// COMMAND ----------

// MAGIC %md
// MAGIC ### Importar Funciones Manejo ADLS

// COMMAND ----------

// MAGIC %run /Shared/framework_ingesta/funciones/Manejo_ADLS

// COMMAND ----------

// MAGIC %md
// MAGIC ### Obtener particiones desde archivo

// COMMAND ----------

def obtenerParticiondesdeArchivo(nombre: String, formato_entrada: String, formato_salida: String): String = {
    var contador = 0
    for (i <- 0 until nombre.split("_").size) {
    if (nombre.split("_")(i).replaceAll("[^0-9]", "") != "") {
        contador = contador + 1
    }

    }
    var datex = ""
    if (contador > 1) {
    datex = nombre.split("_")(nombre.split("_").length - 1).replace("[^0-9]", "").substring(0, 8)
    } else if (contador == 1) {
    datex = nombre.replaceAll("[^0-9]", "").substring(0, formato_entrada.length())
    }
    val inputFormat = new SimpleDateFormat(formato_entrada)
    val outputFormat = new SimpleDateFormat(formato_salida)
    return outputFormat.format(inputFormat.parse(datex))
}

// COMMAND ----------

// MAGIC %md
// MAGIC ### Calcular particiones

// COMMAND ----------

def numPartitionsCalc(tmp: String): Int = {
  var dataSize = sizeFile(tmp)
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

// MAGIC %md
// MAGIC ### Definir schema Json para DF

// COMMAND ----------

def defineSchemaJson(pathJson: String, path_df: String): DataFrame = {

  val dfConverted = spark.read.option("multiline", "true").json(pathJson).drop("type")
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
// MAGIC ### Merge Data

// COMMAND ----------

def mergeData(destination_path: String, merge_string: String, df: DataFrame, partition: Boolean, columns: Seq[String]): Boolean = {

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
      if (partition) {
        println("Modo Write")
        df.write.format("delta")
                .mode("overwrite")
                .partitionBy(columns: _*)
                .option("overwriteSchema", "true")
                .save(destination_path)
      } else {
        println("Modo Write")
        df.write.format("delta")
                .mode("overwrite")
                .option("overwriteSchema", "true")
                .save(destination_path)
      }

      false 
  }
}

// COMMAND ----------

// MAGIC %md
// MAGIC ### Listar particiones

// COMMAND ----------

def listPartitions(schema_hql: String): (Seq[String], String) = {
  
  println("[INFO] prueba 1")
  println("schema_hql\n "+schema_hql)
  println("schema_hql.indexOf()n)   "+schema_hql.indexOf(")\n"))
  println("schema_hql.indexOf(() + 2, schema_hql.indexOf()n)    "+schema_hql.indexOf("(") + 2, schema_hql.indexOf(")\n"))
  val arreglo_string = schema_hql.substring(schema_hql.indexOf("(") + 2, schema_hql.indexOf(")\n")).split("\n")
  var select_line = "select "
  var columns = Seq.empty[String]
  var variable = ""
  val format = new SimpleDateFormat("yyyy-MM-dd")
  val partition_value = format.format(Calendar.getInstance().getTime())

  println("arreglo_string   " + arreglo_string)
  println("[INFO] prueba 2")
  /* SE RECORRE LA LISTA CON LOS CAMPOS ,A LA VARIABLE SELECT_LINE SE LE CONCATENAN TODOS LOS CAMPOS FORMANDO UNA QUERY*/
  for (campo <- arreglo_string if campo.trim() != "") {
    variable = campo.trim().substring(0, campo.trim().indexOf(" "))
    println("[INFO] " + variable)
    select_line = select_line.concat(variable + ",")
  }
  
  println("select_line   " + select_line)
  println("[INFO] prueba 3")
  if (schema_hql.toLowerCase().contains("partitioned by")) {
      // EVALUA SI LA COLUMNA DE PARTICION ESTA VACIA, GENERANDO UNA COLUMNA DE PARTICION NUEVA CON LA FECHA DEL DIA Y AGREGANDOLA AL DATAFRAME. EN CASO QUE LA COLUMNA DE PARTICION Y LA COLUMNA AUXILIAR ESTAN VACIAS, SE OBTIENE EL TIMESTAMP Y SE FORMATEA A YYYY-MM-DD UTILIZANDO ESTA FECHA COMO COLUMNA DE PARTICION
      
      if (partition_date_column == "" && auxiliar_partition_value != "") {
        df2 = df2.withColumn("bigdata_close_date", lit(auxiliar_partition_value) cast "date")
        partition_date_column = "bigdata_close_date"
        formato_partition_column = "yyyy-MM-dd"
      } else if (partition_date_column == "" && auxiliar_partition_value == "") {
        val format = new SimpleDateFormat("yyyy-MM-dd")
        bigdata_close_date = format.format(Calendar.getInstance().getTime())
        df2 = df2.withColumn("bigdata_close_date", lit(bigdata_close_date) cast "date")
        partition_date_column = "bigdata_close_date"
        formato_partition_column = "yyyy-MM-dd"
      }

      //SE CREA UNA COLUMNA DE PARTICION TEMPORAL, TOMANDO LA COLUMNA DE PARTICION Y CAMBIANDO EL FORMATO DE FECHA DE ENTRADA AL DE SALIDA
      df2 = df2.withColumn("bigdata_close_date", from_unixtime(unix_timestamp(col(partition_date_column), formato_partition_column), formato_partition_column_out) cast "date")
      
      //GENERA UN ARREGLO CON LAS DISTINTAS FECHAS EXISTENTES EN EL DATAFRAME
      val dates = df2.select("bigdata_close_date").distinct().rdd.map(r => r(0)).collect()
        
      //SE OBTIENE EL NOMBRE DE LA COLUMNA O COLUMNAS DE PARTICION DESDE EL HQL. ej: (partition_date,year,month,day)
      val find_text_inicial = schema_hql.toLowerCase().indexOf("partitioned by") + 15
      val find_text_final = find_text_inicial + schema_hql.substring(find_text_inicial).indexOf(")")
      val partition_name = schema_hql.substring(find_text_inicial, find_text_final).replace("(", "").replace("string", "").trim()

      //SE SEPARA LA COLUMNA DE PARTICION POR "," EN CASO QUE SEA MAS DE UNA COLUMNA
      var splits = partition_name.split(",").size

      // SE RECORRE LA CANTIDAD DE COLUMNAS DE PARTICION,
      // SE MODIFICA EL SELECT AGREGANDO EL NOMBRE DE LA COLUMNA Y EL VALOR DE LA FECHA,
      // SE GENERA LA RUTA HDFS CON LOS NOMBRES DE LAS COLUMNAS DE PARTICION Y LOS VALORES DE LA FECHA
      // SE AGREGA EL NOMBRE DE LA COLUMNA A UNA LISTA
      
      println("[INFO] prueba 4")
      for (x <- 0 to splits - 1 by 1) {
        if (x != splits - 1 && splits > 1) {
          select_line = select_line.concat("'" + partition_value.split("-")(x) + "' as " + partition_name.split(",")(x).trim() + " ,")
          columns = columns :+ partition_name.split(",")(x).trim()

        } else if (x == splits - 1 && splits > 1) {
          select_line = select_line.concat("'" + partition_value.split("-")(x) + "' as " + partition_name.split(",")(x).trim() + " from ing_schema")
          columns = columns :+ partition_name.split(",")(x).trim()

        } else if (x == splits - 1 && splits == 1) {
          select_line = select_line.concat("'" + partition_value.split("-")(0) + "' as " + partition_name + " from ing_schema")
          columns = columns :+ partition_name.split(",")(x).trim()
        }
      }
      
      println("select_line   " + select_line)
  }else{
      select_line = select_line.substring(0, select_line.length() - 1).concat(" from ing_schema")
  }

  return (columns, select_line)
}
