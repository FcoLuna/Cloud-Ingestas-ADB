// Databricks notebook source
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

import java.sql.{Connection, ResultSet, SQLException}
import java.util.Properties

//val spark = SparkSession.builder.appName("CatalogoApp").getOrCreate()

// COMMAND ----------

// MAGIC %run ../utilidades/utilidades

// COMMAND ----------

// MAGIC %run ../utilidades/parse_json

// COMMAND ----------

// MAGIC %run ../utilidades/sql_utils

// COMMAND ----------

// MAGIC %run ../utilidades/date_utils

// COMMAND ----------

object catalogo 
{
    var schema: org.apache.spark.sql.types.StructType = _
    var ret_val = 0
    var nRecords: Long = 0
    var appName, strSQL, ABFSS_PATH_IN, FILENAME, ACCION, dbtable, methodName, errMsg: String = _
    val objUtilidades: utilidades.type = utilidades
    val objParseJson:  parse_json.type = parse_json
    val sqlUtils: sql_utils.type = sql_utils
    val dateUtils: date_utils.type = date_utils

    var conn: Connection = _
    var cstmt: java.sql.CallableStatement = _           
    var rs: ResultSet = _
    var DF_RENTABILIZACION_MOVIL: DataFrame = _
    var errFlag = false

    def Procesar(args: scala.Array[String], stringBuilder: java.lang.StringBuilder, json_config_file: String, properties: Properties, jdbcUrl: String): Int = 
    {
//        this.methodName = new Object() {}.getClass().getEnclosingMethod().getName()
//        objUtilidades.printHeader(s"${this.methodName}")
        try {
            this.appName = args(0).split(":")(0).toUpperCase().trim()
            val ABFSS_PATH_IN = args(1).split(":")(0).trim()
            val ACCION = args(2).split(":")(0).trim().toUpperCase()

            objUtilidades.printTopic(s"[INFO] AppName: ${this.appName}\nABFSS_PATH_IN: ${this.ABFSS_PATH_IN}\nACCION: ${this.ACCION}")

            Ejecutar(json_config_file, properties, jdbcUrl)
            objUtilidades.printTopic(s"[INFO] this.ret_val: " + this.ret_val)
            objUtilidades.printTopic(s"[INFO] Total registros procesados: " + this.nRecords)
            spark.catalog.clearCache()
        } catch {
            case e: Exception => {
                this.ret_val = 1
                println(s"Procesar() - Ocurrio un Error, en Procesamiento del archivo de MVP. ${e}")
                objUtilidades.printTopic(s"[ERROR] - Procesar() - Ocurrio un Error,en Procesamiento del archivo de MVP. ${e}")
                e.printStackTrace()
            }
        }
        this.ret_val
    }

    def Ejecutar(json_config_file: String, properties: Properties, jdbcUrl: String): Unit = {
//        this.methodName = new Object() {}.getClass().getEnclosingMethod().getName()
        this.ret_val = 0
        this.nRecords = 0
//        objUtilidades.printHeader(s"${this.methodName}")
        /*
        val producto_actualSchema = StructType(StructField("ID_PLAN_ACTUAL", StringType) ::
                                               StructField("DESCRIPCION_PLAN_ACTUAL", StringType) ::
                                               StructField("talla_plan_actual", StringType) :: Nil)
        
        val producto_ofertaSchema = StructType(StructField("CODIGO_OFERTA", StringType) ::
                                               StructField("CODIGO_PRODUCTO", StringType) ::
                                               StructField("codigo_promocion", StringType) ::
                                               StructField("tipo_producto", StringType) ::
                                               StructField("descripcion_oferta", StringType) ::
                                               StructField("cargo_fijo_plan_oferta", StringType) :: Nil)
        
        val oferta_listSchema = StructType(StructField("TIPO_MERCADO", StringType) ::
                                           StructField("ID_OFERTA", StringType) ::
                                           StructField("NOMBRE_MICRO_CAMPANA", StringType) ::
                                           StructField("producto_actual", producto_actualSchema) ::
                                           StructField("producto_oferta", producto_ofertaSchema) :: Nil)
        
        val structureSchema = new StructType().add("TIPO_MERCADO", StringType).
                                               add("ID_OFERTA", StringType).
                                               add("properties", new StructType().
                                               add("ID_PLAN_ACTUAL", IntegerType).
                                               add("DESCRIPCION_PLAN_ACTUAL", StringType))
        
        this.schema = StructType(StructField("tipo_mercado", StringType) ::
                                 StructField("fecha_procesamiento", StringType) ::
                                 StructField("fecha_inicio", DateType) ::
                                 StructField("fecha_fin", DateType) ::
                                 StructField("nombre_macro_campana", StringType) ::
                                 StructField("nombre_micro_campana", StringType) ::
                                 StructField("id_prioridad", IntegerType) ::
                                 StructField("nombre_oferta", StringType) ::
                                 StructField("id_oferta", StringType) ::
                                 StructField("id_oferta_dependencia", StringType) ::
                                 StructField("nombre_crm", StringType) ::
                                 StructField("codigo_oferta", StringType) ::
                                 StructField("codigo_producto", StringType) ::
                                 StructField("codigo_promocion", StringType) ::
                                 StructField("descripcion_codigo_referencia", StringType) ::
                                 StructField("unidad_medida_descuento", StringType) ::
                                 StructField("descuento_oferta", StringType) ::
                                 StructField("duracion_descuento", IntegerType) ::
                                 StructField("descripcion_oferta", StringType) ::
                                 StructField("precio_oferta", IntegerType) ::
                                 StructField("precio_oferta_descuento", IntegerType) ::
                                 StructField("url_formulario", StringType) ::
                                 StructField("detalle_tematicos1_oferta", StringType) ::
                                 StructField("detalle_tematicos2_oferta", StringType) ::
                                 StructField("detalle_tematicos3_oferta", StringType) ::
                                 StructField("detalle_tematicos4_oferta", StringType) ::
                                 StructField("descripcion_canal", StringType) ::
                                 StructField("id_canal_vector", IntegerType) :: Nil) */

        try {
            
            DF_RENTABILIZACION_MOVIL = get_Rentabilizacion_Movil(json_config_file)
            var DF_RENTABILIZACION_MOVIL_RENAME = DF_RENTABILIZACION_MOVIL.
                                                       withColumn("CODIGO_PRODUCTO", substring(col("CODIGO_PRODUCTO"), 1, 30)).
                                                       withColumn("DESCRIPCION_CANAL", substring(col("DESCRIPCION_CANAL"), 1, 150)).
                                                       withColumn("FECHA_PROCESAMIENTO", lit(dateUtils.getCurrentTime).cast("timestamp")).
                                                       withColumn("ACCION", lit(ACCION)).
                                        drop("year", "month", "day").
                                        select(
                                               $"TIPO_MERCADO", 
                                               $"FECHA_PROCESAMIENTO".cast("date"), 
                                               $"FECHA_INICIO".cast("date"), 
                                               $"FECHA_FIN".cast("date"), 
                                               $"NOMBRE_MACRO_CAMPANA", 
                                               $"NOMBRE_MICRO_CAMPANA", 
                                               $"ID_PRIORIDAD", 
                                               $"NOMBRE_OFERTA", 
                                               $"ID_OFERTA", 
                                               $"ID_OFERTA_DEPENDENCIA", 
                                               $"NOMBRE_CRM", 
                                               $"CODIGO_OFERTA", 
                                               $"CODIGO_PRODUCTO", 
                                               $"CODIGO_PROMOCION", 
                                               $"DESCRIPCION_CODIGO_REFERENCIA", 
                                               $"UNIDAD_MEDIDA_DESCUENTO", 
                                               $"DESCUENTO_OFERTA", 
                                               $"DURACION_DESCUENTO".cast("int"), 
                                               $"DESCRIPCION_OFERTA", 
                                               $"PRECIO_OFERTA".cast("int"), 
                                               $"PRECIO_OFERTA_DESCUENTO".cast("int"), 
                                               $"URL_FORMULARIO", 
                                               $"DETALLE_TEMATICOS1_OFERTA", 
                                               $"DETALLE_TEMATICOS2_OFERTA", 
                                               $"DETALLE_TEMATICOS3_OFERTA", 
                                               $"DETALLE_TEMATICOS4_OFERTA", 
                                               $"DESCRIPCION_CANAL", 
                                               $"ID_CANAL_VECTOR", 
                                               $"ACCION")  

            DF_RENTABILIZACION_MOVIL_RENAME.printSchema()
            DF_RENTABILIZACION_MOVIL_RENAME.show(20, false)
            
            // Grabar en la BD
            Grabar(DF_RENTABILIZACION_MOVIL_RENAME, properties, jdbcUrl)

//      this.dbtable = objParseJson.obtenerValorJSON(json_config_file, "jdbc", "catalog_table_exadata")
        this.dbtable = "MARVALRE.MH_NBA_INPUT_CATALOGO_OFERTAS"

        println("catalog_table_exadata: " + dbtable)
            
        // Grabar en la tabla de Log
//            this.ret_val = DM_LOG_MKTHUB(FILENAME.toUpperCase, this.nRecords, this.dbtable, this.ret_val, "Proceso Finalizado Correctamente")
        } catch {
            case e: Exception => {
                this.ret_val = 1
//                this.errMsg = s"${this.ret_val}|${this.methodName} Ocurrio un Error, ejecutando creacion de MVP.|${e}"
                this.errMsg = s"${this.ret_val}|Ejecutar() Ocurrio un Error, ejecutando creacion de MVP.|${e}"
                objUtilidades.printTopic(s"[ERROR] - ${this.errMsg}")
                println(this.errMsg)
                throw new Exception(s"ERROR:" + e)
            }
        }
    }

    // Get Rentabilizacion Movil
    def get_Rentabilizacion_Movil(json_config_file: String): DataFrame = {
//        this.methodName = new Object() {}.getClass().getEnclosingMethod().getName()
        var df_result: DataFrame = null
/*        objUtilidades.printHeader(s"${this.methodName}")
        objUtilidades.printTopic(s"[INFO] Obteniendo datos desde ${this.ABFSS_PATH_IN}")   // /${this.FILENAME}")
*/        this.ret_val = 0 
        try {
            print("L")
            /*
            df_result = spark.read.schema(schema).
              format("csv").
              option("header", "true").
              option("delimiter", "\t").
              option("quote", "\"").
              option("encoding", "ISO-8859-1").
              load(s"${this.ABFSS_PATH_IN}/${this.FILENAME}").cache()
              */

//          val path_catalogo_oferta_ingst_in = objParseJson.obtenerValorJSON(json_config_file, "catalogos", "catalogo_oferta_ingst_in")
            val path_catalogo_oferta_ingst_in = "bi_ingestas.raw_interacciones.catalogo_oferta_ingst"
            println("path_catalogo_oferta_ingst_in: " + path_catalogo_oferta_ingst_in)

            df_result = spark.read.table(path_catalogo_oferta_ingst_in)//.filter($"salida_oferta"===partition_oferta_oferta_full_price)

            df_result.printSchema()
            df_result.show(20, false)
        } catch {
            case e: Exception => {
                this.ret_val = 1
                
//this.errMsg = s"${this.ret_val}|${this.methodName} Ocurrio un error al obtener los datos de ${this.ABFSS_PATH_IN}/${this.FILENAME}.|${e}"
                this.errMsg = s"${this.ret_val}|get_Rentabilizacion_Movil() Ocurrio un error al obtener los datos de ${this.ABFSS_PATH_IN}/$FILENAME}.|${e}"

                objUtilidades.printTopic(s"[ERROR] - ${this.errMsg}")
                println(this.errMsg)
                throw new Exception(s"ERROR:" + e)
                //e.printStackTrace()
            }
        }
        df_result
    }
    

    // Graba en la base datos
    def Grabar(df_out: DataFrame, properties: Properties, jdbcUrl: String): Int = {
//        this.methodName = new Object() {}.getClass().getEnclosingMethod().getName()
//        val methodName: String = new Object() {}.getClass().getEnclosingMethod().getName()
//        objUtilidades.printHeader(s"${this.methodName} Iniciando Carga en BD")
        this.ret_val = 0

//      this.dbtable = objParseJson.obtenerValorJSON(json_config_file, "jdbc", "catalog_table_exadata")
        this.dbtable = "MARVALRE.MH_NBA_INPUT_CATALOGO_OFERTAS"

        objUtilidades.printTopic(s"[INFO] Dbtable: ${this.dbtable}")
        try {
            //df_out.write.mode(SaveMode.Append).partitionBy("fecha_procesamiento").jdbc(jdbcUrl, dbtable, properties)
            df_out.write.mode(SaveMode.Append).jdbc(jdbcUrl, this.dbtable, properties)
            objUtilidades.printTopic(s"[INFO] ${this.methodName} Carga en ${this.dbtable} Exitosa.")
            this.nRecords = df_out.count()
            
            // Grabar en la tabla de Log
            this.ret_val = DM_LOG_MKTHUB(this.ACCION, this.nRecords, this.dbtable, this.ret_val, "Proceso Finalizado Correctamente")
        } catch {
            case e: Exception => {
                this.ret_val = 1
//                this.errMsg = s"${this.ret_val}|Ocurrio una Exception en ${this.methodName} ejecutando la carga en ${this.dbtable}.|${e}"
                this.errMsg = s"${this.ret_val}|Ocurrio una Exception en Grabar() ejecutando la carga en ${this.dbtable}.|${e}"
                objUtilidades.printTopic(s"[ERROR] - ${this.errMsg}")
                println(this.errMsg)
                throw new Exception(s"ERROR:" + e)
                //e.printStackTrace()
                
            }
        }
        this.ret_val
    }
    def DM_LOG_MKTHUB(filename: String, nRecords: Long, dbtable: String, ret_val: Int, message: String): Int = {
        // Implement the logic to log the process
        0 // Placeholder
    }

   // */
}
