// Databricks notebook source
// MAGIC %md
// MAGIC ### Levantamiento de librerías
// MAGIC

// COMMAND ----------

// Importa las bibliotecas necesarias para Spark y funciones
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DateType
import java.time.LocalDate
import java.time.format.DateTimeFormatter
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.types.{StructType, StructField, StringType}
import org.apache.spark.sql.Column


// COMMAND ----------

// MAGIC %md
// MAGIC ### Variables de ejecución

// COMMAND ----------


// Configuración de la sesión Spark
val spark = SparkSession.builder.appName("Parsing Sprinklr Feedback").getOrCreate()
//spark.sparkContext.setLogLevel("INFO")


var parsing_in = dbutils.widgets.get("parsing_in")
var parsing_out = dbutils.widgets.get("parsing_out")
var filename = dbutils.widgets.get("nombre_archivo")



// COMMAND ----------

// MAGIC %md
// MAGIC ### Estructura de SPRINKLR_FEEDBACK

// COMMAND ----------

//Estructura basada en tabla interacciones.sprinklr_feedback
val sprinklrFeedbackStruct = Map(
  "SXB_CONVERGENCIA" -> 1,
  "SXB_REFERIDO" -> 1,
  "DIR_NRO" -> 10,
  "DIR_PISO" -> 10,
  "OFER_P1_PRECIO" -> 10,
  "OFER_P1_DELTA" -> 10,
  "OFER_P2_PRECIO" -> 10,
  "OFER_P2_DELTA" -> 10,
  "OFER_P3_PRECIO" -> 10,
  "OFER_P3_DELTA" -> 10,
  "OFER_P4_PRECIO" -> 10,
  "OFER_P4_DELTA" -> 10,
  "OFER_P5_PRECIO" -> 10,
  "OFER_P5_DELTA" -> 10,
  "VENTA_SISTEMA_ID" -> 10,
  "CLI_ANTIGUEDAD" -> 10,
  "CALL_FAMILIA_ID" -> 10,
  "CALL_SISTEMA_ID" -> 10,
  "CLI_ULT_BOLETA_MONTO" -> 10,
  "CLI_CICLO_COD" -> 10,
  "CLI_LINEA_ANTIGUEDAD" -> 10,
  "C_DEPTO_ACTUALIZADO" -> 100,
  "C_DIR_CIUDAD_ACTUALIZADO" -> 100,
  "C_DIR_REGION_ACTUALIZADO" -> 100,
  "C_CONTACTO_REFERENTE" -> 100,
  "ID_CONVERSACION" -> 100,
  "NUMERO_CASO" -> 100,
  "DIR_COMUNA" -> 100,
  "DIR_CIUDAD" -> 100,
  "NBA_MACRO_CAMPANA" -> 100,
  "NBA_MICRO_CAMPANA" -> 100,
  "DISPOSICION_LLAMADA" -> 100,
  "CLI_NOMBRE" -> 100,
  "PROD_VIG_FIN_PROMO" -> 120,
  "CLI_ID" -> 15,
  "CLI_RUT_CON_DV" -> 15,
  "C_DIRECCION_CF_ACTUALIZADA" -> 150,
  "C_CALLE_ACTUALIZADA" -> 150,
  "C_COMUNA_ACTUALIZADA" -> 150,
  "ID_AGENTE" -> 150,
  "DIRECCION" -> 150,
  "DIR_CALLE" -> 150,
  "NBA_ID_OFERTA" -> 16,
  "TELEFONO_CLIENTE" -> 1700,
  "MONTH" -> 2,
  "CALL_FEC_GEST_INI" -> 20,
  "VENTA_PROD_COD" -> 20,
  "CALL_BATCH_ID" -> 20,
  "OFER_P5_COD" -> 20,
  "ACC_ID" -> 20,
  "OFER_P4_COD" -> 20,
  "ACC_LINEA_ID" -> 20,
  "OFER_P3_COD" -> 20,
  "ACC_HORARIO" -> 20,
  "OFER_P2_COD" -> 20,
  "CLI_SEGMENTO" -> 20,
  "OFER_P1_COD" -> 20,
  "CLI_CONTAC_ALTER_MOVIL" -> 20,
  "CLI_GSE" -> 20,
  "CLI_CONTAC_ALTER_FIJO" -> 20,
  "CLI_CONTAC_ALTER" -> 20,
  "CLI_IND_PORTADO" -> 20,
  "CALL_PAIS_ID" -> 20,
  "CALL_FEC_GEST_FIN" -> 20,
  "NOMBRE_SEGMENTO" -> 200,
  "NOMBRE_CAMPANA" -> 200,
  "ESTADO_FINALIZA_CONVERSACION" -> 200,
  "SUBDISPOSICION_LLAMADA" -> 200,
  "OPERADOR" -> 200,
  "SXB_GENERICO3" -> 200,
  "SXB_GENERICO2" -> 200,
  "SXB_GENERICO1" -> 200,
  "SXB_OBSERVACIONES" -> 200,
  "GENERICO_10" -> 200,
  "GENERICO_09" -> 200,
  "GENERICO_08" -> 200,
  "GENERICO_07" -> 200,
  "GENERICO_05" -> 200,
  "GENERICO_04" -> 200,
  "GENERICO_03" -> 200,
  "GENERICO_02" -> 200,
  "GENERICO_01" -> 200,
  "SAS_MA_NOMBRE_FLUJO" -> 200,
  "OFER_P5_DESC" -> 200,
  "OFER_P4_DESC" -> 200,
  "OFER_P3_DESC" -> 200,
  "OFER_P2_DESC" -> 200,
  "OFER_P1_DESC" -> 200,
  "GENERICO_06" -> 200,
  "ACC_RESP_2" -> 200,
  "ACC_PREG_2" -> 200,
  "ACC_RESP_1" -> 200,
  "ACC_PREG_1" -> 200,
  "ACC_ARG3_ID" -> 200,
  "ACC_ARG2_ID" -> 200,
  "ACC_ARG1_ID" -> 200,
  "C_REFERENCIA_CREADA_POR" -> 255,
  "C_REFERIDO_POR" -> 255,
  "ID_SEGMENTO" -> 255,
  "TIPO_DESCONEXION" -> 255,
  "MOTIVO_DESCONEXION" -> 255,
  "TIPO_PERFIL_MARCADOR" -> 255,
  "NOMBRE_AGENTE" -> 255,
  "CALL_ARCHIVO" -> 30,
  "RUT_AGENTE" -> 300,
  "HABILIDADES_AGENTE" -> 300,
  "C_DESCRIPCION_DEL_TRABAJO" -> 300,
  "YEAR" -> 4,
  "CALL_CANAL_ID" -> 4,
  "CUARTIL" -> 40,
  "C_FORMULARIO_RELLENADO" -> 50,
  "BASE_ID" -> 50,
  "C_PISO_ACTUALIZADA" -> 50,
  "C_REFERENCIA_REALIZADA" -> 50,
  "IDENTIFICADOR_LLAMADA" -> 50,
  "C_NRO_ACTUALIZADA" -> 50,
  "BPO_ID" -> 50,
  "SITE_ID" -> 50,
  "CALL_NEGOCIO_ID" -> 50,
  "CALL_REG_ID" -> 50,
  "C_TIPO_CASO" -> 50,
  "C_BAM" -> 50,
  "C_ALTA_SIN_EQUIPO" -> 50,
  "C_ALTA_CON_EQUIPO" -> 50,
  "VENTA_OOSS_COD" -> 50,
  "DIR_DEPTO" -> 50,
  "CLI_TIPO_TRAFICO" -> 50,
  "PROD_VIG_DESC" -> 50,
  "PROD_VIG_PRECIO" -> 50,
  "RESPUESTA_GEN_4" -> 500,
  "RESPUESTA_GEN_3" -> 500,
  "RESPUESTA_GEN_2" -> 500,
  "RESPUESTA_GEN_10" -> 500,
  "RESPUESTA_GEN_1" -> 500,
  "RESPUESTA_GEN_9" -> 500,
  "RESPUESTA_GEN_8" -> 500,
  "RESPUESTA_GEN_7" -> 500,
  "RESPUESTA_GEN_6" -> 500,
  "RESPUESTA_GEN_5" -> 500,
  "GRUPO_DE_AGENTES" -> 500,
  "ACC_CAMPANA_ID" -> 60,
  "DIR_REGION" -> 70
)


// COMMAND ----------




// Define las rutas de entrada y salida en Azure Data Lake
//val parsing_in = "abfss://ingestas@stbigdatadev02.dfs.core.windows.net/data/interacciones/campanas/sprinklr/sprinklr_feedback/parsing_in/"
//val parsing_out = "abfss://ingestas@stbigdatadev02.dfs.core.windows.net/data/interacciones/campanas/sprinklr/sprinklr_feedback/parsing_out/"

// Nombre del archivo de entrada
//val filename = "Sprinklr_Feedback_20241108.csv"

// Función para limpiar columnas String
def cleanStringColumn(colName: String): Column = {
  val trimmedColumn = trim(col(colName))
  when(trimmedColumn === "" || trimmedColumn === "null" || col(colName).isNull, null)
    .otherwise(trimmedColumn)
    .alias(colName)
}

try {
  // Formateo de la fecha
  val datePart = filename.split("_")(2).substring(0, 8)
  val formatter = DateTimeFormatter.ofPattern("yyyyMMdd")
  val currentDate = LocalDate.parse(datePart, formatter)

  val datePattern = "^\\d{4}-\\d{2}-\\d{2}$"
  val filterNumbers = "^-?\\d+(\\.\\d+)?$"

  // Lectura del archivo con delimitador |
  val df1 = spark.read.option("header", "true").option("inferSchema", "false").option("delimiter", "|").csv(parsing_in+filename)
  val df2 = df1.select(df1.columns.map(c => when(trim(col(c)) === "" || col(c) === "null", null).otherwise(col(c)).alias(c)): _*)
    .withColumn("NBA_FECHA_PROCESAMIENTO", date_format(to_date(col("NBA_FECHA_PROCESAMIENTO"), "ddMMMyyyy:HH:mm:ss"), "yyyy-MM-dd"))
    .filter(col("FECHA").isNotNull)
    .filter(col("FECHA").rlike(datePattern))
    .filter(col("FECHA").cast(DateType).isNotNull)
    .filter(col("ACC_SCORE").isNull || col("ACC_SCORE").rlike(filterNumbers))
    .dropDuplicates()

  val df_cleaned_nuevo = df2.select(
    df1.columns.map { colName =>
      df1.schema(colName).dataType match {
        case _: org.apache.spark.sql.types.StringType => cleanStringColumn(colName)
        case _ => col(colName)
      }
    }: _*
  )

  val df_filled = df_cleaned_nuevo.na.fill("null_placeholder")

  val dfNewWithoutDuplicates_reverse = df_filled.columns.foldLeft(df_filled) { (tempDf, colName) =>
    val columnType = tempDf.schema(colName).dataType
    if (columnType.isInstanceOf[org.apache.spark.sql.types.StringType]) {
      tempDf.withColumn(colName, when(col(colName) === "null_placeholder", lit(null)).otherwise(col(colName)))
    } else {
      tempDf
    }
  }

  val folder_name = filename.stripSuffix(".csv")

  //Se ajusta estructura a largos definidos en exadata

  val dfNewWithoutDuplicates_length = sprinklrFeedbackStruct.foldLeft(dfNewWithoutDuplicates_reverse) {
  case (df, (colName, maxLength)) =>
    if (df.columns.contains(colName))
      df.withColumn(colName, substring(col(colName), 1, maxLength))
    else
      df
}



  // Guarda el archivo en la carpeta de salida
  //dfNewWithoutDuplicates_reverse.repartition(1).write.option("header", "true").option("delimiter", "|").mode("append").csv(parsing_out)
  dfNewWithoutDuplicates_length.repartition(1).write.option("header", "true").option("delimiter", "|").mode("append").csv(parsing_out)

// Listar los archivos en el directorio y Filtrar el archivo que comienza con "part-"
val files = dbutils.fs.ls(parsing_out)
val oldFilePathOption = files.find(file => file.name.startsWith("part-"))

// Si encontramos el archivo, renombrarlo
oldFilePathOption match {
  case Some(oldFile) =>
    val oldFilePath = oldFile.path
    val newFilePath = parsing_out + filename
    // Renombrar el archivo
    dbutils.fs.mv(oldFilePath, newFilePath)
    println(s"El archivo ha sido renombrado a: $filename")

  case None =>
    println("No se encontró ningún archivo que cumpla con el criterio.")
}

} catch {
  case e: Exception =>
    println("[ERROR] " + e)
    throw e
}

println("[INFO] PROCESO TERMINADO")


// COMMAND ----------

val files = dbutils.fs.ls(parsing_in)

// Elimina cada archivo individualmente
files.foreach(file => dbutils.fs.rm(file.path, true))

