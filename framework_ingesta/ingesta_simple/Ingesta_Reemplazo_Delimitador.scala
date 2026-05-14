// Databricks notebook source
spark.conf.set("spark.sql.session.timeZone", "America/Santiago")

// COMMAND ----------


import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions.expr
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.functions.split
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import io.delta.tables._
import org.apache.spark.sql.Row

import java.util.{Calendar, TimeZone, Date}
import org.apache.spark.sql.types.DateType
import java.time.format.DateTimeFormatter
import java.time.LocalDateTime
import java.text.SimpleDateFormat

// COMMAND ----------

var dir_stage = ""
var nombre_archivo_origen = ""
var delimitador_origen = ""
var delimitador_reemplazo = ""
var status_ejecucion = 0  
var status_error = ""
var desc_status_ejecucion = ""
var path_stage_archivo = ""

// COMMAND ----------

if(status_ejecucion == 0){
   //try {
    //DIRECTORIO STAGE INGESTA ARCHIVOS
    dir_stage = dbutils.widgets.get("dir_stage")
    //NOMBRE ARCHIVO ORIGEN
    nombre_archivo_origen = dbutils.widgets.get("nombre_archivo_origen")
    //DELIMITADOR DE ORIGEN
    delimitador_origen = dbutils.widgets.get("delimitador_origen")
    //DELIMITADOR DE REEMPLAZO
    delimitador_reemplazo = dbutils.widgets.get("delimitador_reemplazo")

   // } catch {
   //     case e: Exception =>
   //       status_ejecucion = 1
   //       desc_status_ejecucion = "[ERROR] " + e
   //       status_error = e
   //       println("[ERROR] " + e)
   // } 
}

// COMMAND ----------

if(status_ejecucion == 0){
    //try {
      path_stage_archivo = dir_stage + "/" + nombre_archivo_origen  

    //} catch {
    //    case e: Exception =>
    //      status_ejecucion = 1
    //      desc_status_ejecucion = "[ERROR] " + e
    //      status_error = e
    //      println("[ERROR] " + e)
    //} 
//} else {
//  println("Skipped")
}

// COMMAND ----------

println(s"dir_stage: $dir_stage") 
println(s"nombre_archivo_origen: $nombre_archivo_origen") 
println(s"path_stage_archivo: $path_stage_archivo") 
println(s"delimitador_origen: $delimitador_origen") 
println(s"delimitador_reemplazo: $delimitador_reemplazo") 



// COMMAND ----------

// ESQUEMA ARCHIVO ORIGEN
val schema = StructType(Array(
StructField("NUM_ABONADO", StringType, true),
StructField("NUM_OS", StringType, true),
StructField("FECHA", StringType, true),
StructField("USUARIO", StringType, true),
StructField("DESCRIPCION", StringType, true),
StructField("DES_OFICINA", StringType, true),
StructField("FECHA_FICHA", StringType, true),
StructField("RES_FICHA", StringType, true),
StructField("COD_CLIENTE", StringType, true),
StructField("NUM_CELULAR", StringType, true),
StructField("COD_PLANTARI", StringType, true),
StructField("DES_PLANTARIF", StringType, true),
StructField("COD_CARGOBASICO", StringType, true),
StructField("DES_CARGOBASICO", StringType, true),
StructField("IMP_CARGOBASICO", StringType, true),
StructField("NUM_SERIE", StringType, true),
StructField("COD_TIPCONTRATO", StringType, true),
StructField("DES_TIPCONTRATO", StringType, true),
StructField("NUM_IDENT", StringType, true),
StructField("DES_CATEGORIA", StringType, true),
StructField("DES_VALOR", StringType, true),
StructField("FEC_ALTA_EQ", StringType, true),
StructField("COD_ARTICULO_EQ", StringType, true),
StructField("DES_ARTICULO_EQ", StringType, true),
StructField("IND_PROCEQUI_EQ", StringType, true),
StructField("COD_TECNOLOGIA_EQ", StringType, true),
StructField("DES_MODVENTA_EQ", StringType, true),
StructField("DES_FABRICANTE_EQ", StringType, true),
StructField("DES_USO_EQ", StringType, true),
StructField("COD_CAUSA_EQ", StringType, true),
StructField("ANTIGUEDAD", StringType, true),
StructField("ANTIGUEDAD_EN_MESES", StringType, true),
StructField("FEC_ALTA_EQ_ANT", StringType, true),
StructField("IND_PROCEQUI_EQ_ANT", StringType, true),
StructField("COD_TECNOLOGIA_EQ_ANT", StringType, true),
StructField("DES_MODVENTA_EQ_ANT", StringType, true),
StructField("DES_ARTICULO_EQ_ANT", StringType, true),
StructField("DES_FABRICANTE_EQ_ANT", StringType, true),
StructField("NUM_SERIE_EQ_ANT", StringType, true),
StructField("DES_USO_EQ_ANT", StringType, true),
StructField("TRAMO_ANTIGUED", StringType, true),
StructField("VALIDA_SERIE", StringType, true),
StructField("COD_TIPCONTRATO_ANT", StringType, true),
StructField("DES_TIPCONTRATO_ANT", StringType, true),
StructField("TIPO", StringType, true),
StructField("DES_CAUSA", StringType, true),
StructField("SEGMENTO", StringType, true),
StructField("FLUJO_RENUEVA", StringType, true),
StructField("NUM_SIMULACION", StringType, true),
StructField("IND_DEV_EQUIPO", StringType, true),
StructField("IND_AMISTAR", StringType, true),
StructField("TIP_SIMULACION", StringType, true),
StructField("IND_ESTADOEQUI", StringType, true),
StructField("IND_DEVCARGADOR", StringType, true),
StructField("COD_ARTICULONEW", StringType, true),
StructField("VAL_LISTAEQUIPO", StringType, true),
StructField("NUM_SALDOPUNTOS", StringType, true),
StructField("NUM_DSCTO_PROMOCION", StringType, true),
StructField("NUM_DSCTO_POLITICA", StringType, true),
StructField("NUM_DSCTO_ADICIONAL", StringType, true),
StructField("COD_PLANTARIF", StringType, true),
StructField("COD_PLANTARIFNUE", StringType, true),
StructField("COD_SSNEM", StringType, true),
StructField("COD_SERVSUPLEM", StringType, true),
StructField("IND_ESTADOSIMU", StringType, true),
StructField("DET_ANULACION", StringType, true),
StructField("FEC_SIMULACION", StringType, true),
StructField("FEC_HASTA", StringType, true),
StructField("COD_OFICINA_ORIG", StringType, true),
StructField("COD_OFICINA_EJE_CAMB", StringType, true),
StructField("COD_USUARIO", StringType, true),
StructField("COD_USUARIO_RECAMBIO", StringType, true),
StructField("COD_USUARIO_DSCTO", StringType, true),
StructField("COD_RAZON_CONTENCION", StringType, true),
StructField("DET_RAZON_CONTENCION", StringType, true),
StructField("PROMO_APLICADA", StringType, true),
StructField("NUM_OSCAMBIOPLAN", StringType, true),
StructField("NUM_OSCAMBIOEQUI", StringType, true),
StructField("NUM_VENTA", StringType, true),
StructField("COD_CONCEPTO_9217", StringType, true),
StructField("COD_CONCEPTO_9235", StringType, true),
StructField("COD_CONCEPTO_9221", StringType, true),
StructField("VAL_DESCUENTO_9218", StringType, true),
StructField("VAL_DESCUENTO_9236", StringType, true),
StructField("VAL_DESCUENTO_9222", StringType, true),
StructField("PERIODO", StringType, true),
StructField("FECHA_2", StringType, true),
StructField("CANAL", StringType, true),
StructField("RES_CARTA", StringType, true),
StructField("FECHA_CARTA", StringType, true),
StructField("TIPO_RET", StringType, true),
StructField("TIPO_PLAN", StringType, true),
StructField("DISTRIBUIDOR", StringType, true),
StructField("TIPO_ARTICULO", StringType, true),
StructField("TIPO_EQUIPO", StringType, true),
StructField("CAN_FECHA", StringType, true),
StructField("CAN_PUNTOS_BONIFICADOS", StringType, true),
StructField("CAN_SALDO", StringType, true),
StructField("CAN_VALOR_EQUIPO", StringType, true),
StructField("CAN_VALOR_PRODUCTO", StringType, true),
StructField("CAN_PUNTOS_COMPRADOS", StringType, true),
StructField("MODALIDAD", StringType, true),
StructField("PMP", StringType, true),
StructField("COD_SISCEL_9217", StringType, true),
StructField("COD_SISCEL_9218", StringType, true),
StructField("CARGO_HABILITACION", StringType, true),
StructField("DSCTO_HABILITACION", StringType, true),
StructField("PAGADO_POR_HABILITACION", StringType, true),
StructField("PUNTO_COMPRADOS", StringType, true),
StructField("FECHA_REC", StringType, true),
StructField("IDTABLA", StringType, true),
StructField("SEGM_PROP", StringType, true),
StructField("COD_ARTICULO_EQ_ANT", StringType, true),
StructField("COD_OPERADOR", StringType, true),
StructField("NOM_OPERADOR", StringType, true),
StructField("RAZONANTIGUEDAD", StringType, true),
StructField("FEC_ALTA_1", StringType, true),
StructField("FEC_ALTA_2", StringType, true),
StructField("FEC_ALTA_3", StringType, true),
StructField("FEC_ALTA_0", StringType, true),
StructField("MODELO_HOMOLOGADO_ARPU", StringType, true),
StructField("ACCION_COMERCIAL", StringType, true),
StructField("VALOR_PAGADO_CLIENTE", StringType, true),
StructField("VIR", StringType, true),
StructField("DESCUENTO_VIR", StringType, true),
StructField("PCOSTO", StringType, true),
StructField("FULLFILMENT", StringType, true),
StructField("SUBSIDIO", StringType, true),
StructField("TRIANGULADO", StringType, true),
StructField("FEC_PROCESO_PMP", StringType, true),
StructField("SIMCARD", StringType, true),
StructField("MARCA_PLAN", StringType, true),
StructField("NUM_SERIE_EQ", StringType, true),
StructField("SUBSIDIO_CON_ACCION_COMERCIAL", StringType, true),
StructField("COD_CATEGORIA_PLAN", StringType, true),
StructField("COD_CATEGORIA_PLAN_ANT", StringType, true),
StructField("TIPO_EQUIPO_2", StringType, true),
StructField("TIER", StringType, true),
StructField("COD_CATEGORIA_PLAN_NUE", StringType, true),
StructField("COD_CONCEPTO_99999", StringType, true),
StructField("VAL_DESCUENTO_99999", StringType, true),
StructField("CREDITO", StringType, true),
StructField("CUOTAS", StringType, true),
StructField("COD_CONCEPTOART", StringType, true),
StructField("COD_CONCEPTODTO", StringType, true),
StructField("IMP_CARGO", StringType, true),
StructField("COD_CONCEPTO_DTO", StringType, true),
StructField("VAL_DTO", StringType, true),
StructField("TIP_DTO", StringType, true),
StructField("NUM_CELULAR_2", StringType, true),
StructField("SEGM_PROF", StringType, true),
StructField("PROMOCION", StringType, true),
StructField("IMP_ABONO", StringType, true),
StructField("IND_ABONO", StringType, true),
StructField("MONTO_VENTA", StringType, true),
StructField("COD_SAP_EQ", StringType, true),
StructField("COD_SAP_EQ_ANT", StringType, true),
StructField("PMP_EQ_ANT", StringType, true),
StructField("SUBSIDIO_MODELO", StringType, true),
StructField("NUM_UNIDADES", StringType, true),
StructField("IMP_ABONO_CHEQUE", StringType, true),
StructField("IMP_ABONO_EFECTIVO", StringType, true),
StructField("IMP_ABONO_T_DEBITO", StringType, true),
StructField("IMP_ABONO_T_CREDITO", StringType, true),
StructField("ABONO_TC_NUM_CUOTAS", StringType, true),
StructField("ABONO_COD_TIPTARJETA", StringType, true),
StructField("ABONO_TIPTARJETA", StringType, true),
StructField("PRODUCT_DESC", StringType, true),
StructField("SUBSCRIBER_KEY", StringType, true),
StructField("ORDER_ACTION_KEY", StringType, true),
StructField("SISTEMA", StringType, true),
StructField("DES_PAGO", StringType, true),
StructField("IMP_PAGO", StringType, true),
StructField("MODELO_VENTA", StringType, true),
StructField("NUM_CUOTAS", StringType, true),
StructField("FINANCING_PERIOD", StringType, true),
StructField("PIE", StringType, true),
StructField("CUSTOMER_KEY", StringType, true),
StructField("LOTE_EQUIPO", StringType, true),
StructField("PRODUCT_DESC_PLN_OUT", StringType, true),
StructField("FLAG", StringType, true),
StructField("REPLACEMENT_EQUIP_IND", StringType, true))
)
 
// leer dataframe y asignarle el esquema
//val df = spark.read.schema(schema).option("header", "false").option("inferSchema", "false").option("delimiter", "\t").csv(PathInput)
var df = spark.read.schema(schema).option("header", "false").option("inferSchema", "false").option("delimiter", delimitador_origen).csv(path_stage_archivo)
//Borra Archivo Origen 
//dbutils.fs.rm(path_stage_archivo, true) 
// guardar la data separada con otro delimitador
df.repartition(1).write.option("header", "true").mode("overwrite").option("delimiter",delimitador_reemplazo).csv(path_stage_archivo)
 
// buscar el archivo que se guardó con el part- etc.."
//val files = dbutils.fs.ls(PathOutput)
//val oldFilePathOption = files.find(file => file.name.startsWith("part-"))
 
// renombrarlo
//oldFilePathOption match {
//  case Some(oldFile) =>
//    val oldFilePath = oldFile.path
//    val newFilePath = PathOutput + filename
//    dbutils.fs.mv(oldFilePath, newFilePath)
    println(s"El archivo ha sido generado con delimitadores y cabecera: $nombre_archivo_origen")
 
  //case None =>
  //  println("No se encontró ningún archivo que cumpla con el criterio.")
//}
 
df.show(10, false)
