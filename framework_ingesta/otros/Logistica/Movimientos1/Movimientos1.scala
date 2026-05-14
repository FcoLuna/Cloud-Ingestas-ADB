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
val tableName = "LGT_MOVIMIENTOS_1"
val table = tableOwner + "." + tableName

val prop = new java.util.Properties
prop.setProperty("driver", jdbcDriver)
prop.setProperty("user", jdbcUser)
prop.setProperty("password", jdbcPass)
prop.put("v$session.osuser",osuser)
var connection: Connection = null

// COMMAND ----------

// MAGIC %md
// MAGIC # Valores externos y rutas

// COMMAND ----------

/**
* Variables para testing 
*/

// val contenedor = "abfss://bigdatadev@stbigdatadev02.dfs.core.windows.net"
// val daysAgo = 19
// val daysAhead = 0

println("[INFO] Obteniendo variables desde Data Factory")
val daysAgo = dbutils.widgets.get("dias_atras").toInt
val daysAhead = dbutils.widgets.get("dias_limite").toInt
val repartition = dbutils.widgets.get("sesiones").toInt

val starDate = getDate(-daysAgo, "yyyy-MM-dd")
val endDate = getDate(-daysAhead, "yyyy-MM-dd")

println("[INFO] Rutas necesarias")
val contenedor = "abfss://bigdataprd@stbigdataprd02.dfs.core.windows.net"
val targetPath = contenedor + "/data/gestion_recursos/logistica/sap/movimientos1/raw"

// COMMAND ----------

// getDate(-1, "yyyy-MM-dd")
// getDate(-0, "yyyy-MM-dd")

// COMMAND ----------

// MAGIC %md
// MAGIC # Campos y tipos de datos

// COMMAND ----------

val targetDF = spark.read.format("delta").load(targetPath)
  .filter(to_date($"fecha_ejec", "yyyyMMdd") >= starDate && to_date($"fecha_ejec", "yyyyMMdd") <= endDate)
  .select(
    $"sociedad",
    $"num_de_doc_mat",
    $"ano_doc_mat".cast("integer").as("ano_doc_mat"),
    to_date($"fecha_contab", "yyyyMMdd").as("fecha_contab"),
    $"pos_en_doc_mat".cast("integer").as("pos_en_doc_mat"),
    $"centro",
    $"alm_origen",
    $"alm_destino",
    $"mat",
    $"clase_mov",
    $"lote",
    $"coste_vta".cast("decimal(20,4)").as("coste_vta"),
    $"moneda",
    $"un_med",
    $"clase_valor",
    $"num_aduana",
    to_date($"fecha_desadunaje", "yyyyMMdd").as("fecha_desadunaje"),
    $"cod_aduana",
    $"orderid",
    $"area_func",
    $"centro_coste",
    $"orden",
    $"cliente",
    $"cta_contable",
    $"cta_mayor",
    $"cant".cast("decimal(20,4)").as("cant"),
    $"puesto_desc",
    $"ind_d_h",
    $"pos_creada_aut",
    $"doc_anul",
    $"ejerc_doc_anul".cast("integer").as("ejerc_doc_anul"),
    $"usuario_creac_modif",
    to_date($"fecha_creac_modif", "yyyyMMdd").as("fecha_creac_modif"),
    $"hora_creac_modif",
    $"proveedor",
    $"ped_cliente",
    $"pos_ped_cliente".cast("integer").as("pos_ped_cliente"),
    $"pos_presupuestaria",
    $"fondo",
    $"centro_gestor",
    $"doc_compras",
    $"clase_documento",
    $"referencia",
    $"txt_cabec",
    $"tipo_de_stock",
    $"stock_especial",
    to_date($"fecha_ejec", "yyyyMMdd").as("fecha_ejec"),
    $"elemento_pep".as("elemento_rep"),
    to_date($"fecha_ejec", "yyyyMMdd").as("partition_date")
  )




// COMMAND ----------

// MAGIC %md
// MAGIC # Comprobación de datos
// MAGIC La siguiente celda mantener comentada

// COMMAND ----------

display(targetDF.groupBy("partition_date").count.orderBy($"partition_date".desc))

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
      st.executeUpdate("call " + tableOwner + ".DO_THE_TRUNCATE_PARTITION('" + table + "','" + partition.getString(0) + "')")
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
