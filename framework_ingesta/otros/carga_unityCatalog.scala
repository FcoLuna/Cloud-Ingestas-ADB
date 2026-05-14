// Databricks notebook source
//Proceso permite consumir datos desde una tabla de unity catalog, filtrarla, y almacenar ese df en una nueva tabla del catalogo.
//Por ejemplo procesos cascada pago, carga archivos desde ftp y los almacena en tabla CONVERGENTE, luego se filtra esta tabla y se crea la tabla FIJO y MOVIL

// COMMAND ----------

// MAGIC %md ###Librerias

// COMMAND ----------

import io.delta.tables._
import org.apache.spark.sql.functions._

// COMMAND ----------

// MAGIC %md ###Parametros

// COMMAND ----------

//Inputs
// catalogo a consumir
val catalog = dbutils.widgets.get("catalog")
// schema de tabla a consumir
val schemaInput = dbutils.widgets.get("schemaInput")
// nombre tabla a consumir
val tableInput = dbutils.widgets.get("tableInput")
// concatenacion para crear nombre completo de tabla a consumir
val catalogSchemaTable_ADB = catalog + "." + schemaInput + "." + tableInput
// columnas a seleccionar de tabla input (* para todas o seleccionadas por coma, ej: col1, col2, col3)
val columsInput = dbutils.widgets.get("columsInput")
// condicion para filtrar datos (escribir en lenguaje sql)
val whereInput = Option(dbutils.widgets.get("whereInput")).filterNot(_.isEmpty).getOrElse("")

//Output
// esquema y nombre tabla destino
val schemaTableOutput = dbutils.widgets.get("schemaTableOutput")
// concatenacion para obtener nombre completa de tabla destino
val catalogSchemaTableOtput = catalog + "." + schemaTableOutput
// url de repositorio
val adls_path = dbutils.widgets.get("adls_path")
// ruta donde se almacenarán los datos en adls
val pathOutput = adls_path + dbutils.widgets.get("pathOutput")
// particiciones (year, month, day u otro)
val partitionByOutput = dbutils.widgets.get("partitionByOutput")
// modo de escritura, por defecto overwrite
val writeMode = Option(dbutils.widgets.get("writeMode")).filterNot(_.isEmpty).getOrElse("overwrite")

// entrega true si la tabla existe en el catalogo o false en caso contrario
val tableExists = spark.catalog.tableExists(catalogSchemaTableOtput)


// COMMAND ----------

// MAGIC %md ###Extracción de datos y creacion de tabla

// COMMAND ----------

//SE GENERA DF CON DATOS DE LA TABLA DE ORIGEN
var query = ""
if(whereInput == ""){
  query = "SELECT " + columsInput + " FROM " + catalogSchemaTable_ADB
}else{
  query = "SELECT " + columsInput + " FROM " + catalogSchemaTable_ADB + " WHERE " + whereInput
}

println("[INFO] QUERY A CONSULTAR: " + query)

val df = spark.sql(query)

// display(df)

val df_count = df.count()

if(df_count > 0) {
  try{

    // if(tableExists){
    // println("[INFO] LIMPIANDO TABLA CATALOGO")

    //   // Crear una instancia de DeltaTable
    //   val deltaTable = DeltaTable.forPath(spark, pathOutput)
    //   // Eliminar registros
    //   deltaTable.delete(whereInput)
    //   // Vaciar archivos no referenciados (opcional)
    //   deltaTable.vacuum()

    // }else{println("[INFO] TABLA NO EXISTE EN EL CATALOGO")}

    println("[INFO] ESCRIBIENDO EN ADLS Y CREANDO TABLA EN CATALOGO")

    df
      .write.format("delta")
      .mode(writeMode)
      .option("path", pathOutput)
      .partitionBy("year","month")
      .option("partitionOverwriteMode", "dynamic")
      .saveAsTable(catalogSchemaTableOtput)

    println("[INFO] PROCESO TERMINADO SIN ERRORES")
  }catch{
    case e: Exception =>
      println("[ERROR] " + e)
  }
}else{
  println("[ERROR] TABLA " + catalogSchemaTable_ADB + " NO CONTIENE DATOS")
}



