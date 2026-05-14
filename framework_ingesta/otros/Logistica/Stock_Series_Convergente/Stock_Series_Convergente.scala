// Databricks notebook source
// MAGIC %md
// MAGIC # Funciones

// COMMAND ----------

// MAGIC %run ../function

// COMMAND ----------

// MAGIC %md
// MAGIC # Exadata

// COMMAND ----------

// MAGIC %run ../exadata

// COMMAND ----------

val tableOwner = "GESTION_RECURSOS"
val tableName = "LGT_MOVIMIENTOS_2"
val table = tableOwner + "." + tableName

// COMMAND ----------

// MAGIC %md
// MAGIC # Valores externos y rutas

// COMMAND ----------

/**
* Variables para testing 
*/

// val contenedor = "abfss://bigdatadev@stbigdatadev02.dfs.core.windows.net"
// val daysAgo = 1
// val daysAhead = 0
// val repartition = 9

println("[INFO] Obteniendo variables desde Data Factory")
val daysAgo = dbutils.widgets.get("dias_atras").toInt
val daysAhead = dbutils.widgets.get("dias_limite").toInt
val repartition = dbutils.widgets.get("sesiones").toInt

val starDate = getDate(-daysAgo, "yyyy-MM-dd")
val endDate = getDate(-daysAhead, "yyyy-MM-dd")

println("[INFO] Rutas necesarias")
val contenedor = "abfss://bigdataprd@stbigdataprd02.dfs.core.windows.net"
val targetPath = contenedor + "/data/gestion_recursos/logistica/sap/movimientos2/raw"

// COMMAND ----------

// MAGIC %md
// MAGIC # Campos y tipos de datos

// COMMAND ----------

val targetDF = spark.read.format("delta").load(targetPath)
  .filter(to_date($"fecha_ejec", "yyyyMMdd") >= starDate && to_date($"fecha_ejec", "yyyyMMdd") <= endDate)
  .select(
    $"sociedad",
    $"numero_documento_material",
    $"ano_documento_material".cast("integer").as("ano_documento_material"),
    $"posicion_documento_material".cast("integer").as("posicion_documento_material"),
    $"numero_de_serie",
    $"mat",
    $"ind_dh",
    to_date($"fecha_ejec", "yyyyMMdd").as("fecha_ejec"),
    to_date($"fecha_ejec", "yyyyMMdd").as("partition_date")
  )




// COMMAND ----------

// MAGIC %md
// MAGIC # Comprobación de datos
// MAGIC La siguiente celda mantener comentada

// COMMAND ----------

// display(targetDF.groupBy("partition_date").count.orderBy($"partition_date".desc))

// COMMAND ----------

// MAGIC %md
// MAGIC # Limpieza en Exadata (para reprocesos)

// COMMAND ----------

// Se crea una lista de particiones destinadas a ser eliminadas
val partitions = "(SELECT * FROM (WITH DATA AS (select table_name,partition_name,to_date(trim('''' from regexp_substr(extractvalue(dbms_xmlgen.getxmltype" +
  "('select high_value from all_tab_partitions where table_name='''|| table_name|| ''' and table_owner = '''|| table_owner|| ''' " +
  "and partition_name = '''|| partition_name|| ''''),'//text()'),'''.*?''')),'syyyy-mm-dd hh24:mi:ss') - 1 date_on_partition " +
  "FROM all_tab_partitions WHERE table_name = '" + tableName + "' AND table_owner = '" + tableOwner + "')" +
  "SELECT partition_name FROM DATA WHERE date_on_partition >= DATE '" + starDate.substring(0, 4) + "-" + starDate.substring(5, 7) + "-" + starDate.substring(8, 10) + "' " +
  "AND date_on_partition <= DATE '" + endDate.substring(0, 4) + "-" + endDate.substring(5, 7) + "-" + endDate.substring(8, 10) + "') A)"


Class.forName(jdbcDriver)
  val db = DriverManager.getConnection(jdbcUrl, jdbcUser, jdbcPass)
  val st = db.createStatement()
  try {
    val partitionList = spark.read.format("jdbc")
      .option("url", jdbcUrl)
      .option("driver", jdbcDriver)
      .option("dbTable", partitions)
      .option("user", jdbcUser)
      .option("password", jdbcPass)
      .load()
      .collect()
    for (partition <- partitionList) {
      st.executeUpdate("call " + tableOwner + ".DO_THE_TRUNCATE_PARTITION('" + table + "','PARTITION','" + partition.getString(0) + "')")
    }
  } catch {
    case e: Exception =>
      println("[ERROR TRUNCATE] " + e)
  }


// COMMAND ----------

// MAGIC %md
// MAGIC # Insertando datos a Exadata

// COMMAND ----------

targetDF.repartition(repartition)
  .write.mode("append")
  .jdbc(jdbcUrl, table, utils.jdbcProperties(jdbcDriver, jdbcUser, jdbcPass))
