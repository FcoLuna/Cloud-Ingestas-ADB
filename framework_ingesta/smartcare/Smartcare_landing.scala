// Databricks notebook source
dbutils.widgets.text("n_stage","1")
val numero_stage = dbutils.widgets.get("n_stage")

// COMMAND ----------

spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")

// COMMAND ----------

import org.apache.spark.sql.functions._
import org.apache.hadoop.fs.{FileSystem, Path}
import java.net.URI
import java.nio.file.Paths
import org.apache.spark.sql.DataFrame

import org.apache.spark.sql.SparkSession
import com.databricks.dbutils_v1.DBUtilsHolder.dbutils

// COMMAND ----------

var list : Seq[com.databricks.backend.daemon.dbutils.FileInfo] = null
// Ruta del directorio de origen y destino
val origen = "abfss://ingestas@stbigdatadev02.dfs.core.windows.net/data/trafico/trafico_detalle/smartcare/stage"
val path_origen = origen + numero_stage
val path_destino = "abfss://ingestas@stbigdatadev02.dfs.core.windows.net/data/trafico/trafico_detalle/smartcare/landing"

// COMMAND ----------

// Función para determinar la carpeta de destino según el nombre del archivo
def obtenerCarpetaDestino(nombreArchivo: String): String = {
  val nombreMinusculas = nombreArchivo.toLowerCase
  
  if (nombreMinusculas.contains("cdr_aiu_moc")) {
    "cdr_aiu_moc"
  } else if (nombreMinusculas.contains("cdr_aiu_mtc")) {
    "cdr_aiu_mtc"
  } else if (nombreMinusculas.contains("cdr_ims_call_delay")) {
    "cdr_ims_call_delay"
  } else if (nombreMinusculas.contains("cdr_ims_inf_call_sip")) {
    "cdr_ims_inf_call_sip"
  } else if (nombreMinusculas.contains("cdr_ims_mo_call_leg_sip")) {
    "cdr_ims_mo_call_leg_sip"
  } else if (nombreMinusculas.contains("cdr_ims_mt_call_leg_sip")) {
    "cdr_ims_mt_call_leg_sip"
  } else if (nombreMinusculas.contains("cdr_volte_voice_quality")) {
    "cdr_volte_voice_quality"
  } else if (nombreMinusculas.contains("dns_detail_ufdr_dns")) {
    "dns"
  } else if (nombreMinusculas.contains("email_detail_ufdr_email")) {
    "email"
  } else if (nombreMinusculas.contains("emailfail_detail_ufdr_email")) {
    "emailfail"
  } else if (nombreMinusculas.contains("fileaccess_detail_ufdr_fileaccess")) {
    "fileaccess"
  } else if (nombreMinusculas.contains("ftp_detail_ufdr_ftp")) {
    "ftp"
  } else if (nombreMinusculas.contains("detail_cdr_gbiups")) {
    "gbiups"
  } else if (nombreMinusculas.contains("im_detail_ufdr_im")) {
    "im"
  } else if (nombreMinusculas.contains("detail_cdr_iupsrelease")) {
    "iupsrelease"
  } else if (nombreMinusculas.contains("other_detail_ufdr_other")) {
    "other"
  } else if (nombreMinusculas.contains("detail_cdr_s1mme")) {
    "s1mme"
  } else if (nombreMinusculas.contains("detail_cdr_s6a")) {
    "s6a"
  } else if (nombreMinusculas.contains("stream_detail_ufdr_streaming")) {
    "stream"
  }else if (nombreMinusculas.contains("streamfail_detail_ufdr_streaming")) {
    "streamfail"
  } else if (nombreMinusculas.contains("tdr_ims_hss_trans")) {
    "tdr_ims_hss_trans"
  }else if (nombreMinusculas.contains("tdr_ims_inf_ip_message_sip")) {
    "tdr_ims_inf_ip_message_sip"
  } else if (nombreMinusculas.contains("tdr_ims_inf_miscellaneous_sip")) {
    "tdr_ims_inf_miscellaneous_sip"
  }else if (nombreMinusculas.contains("tdr_ims_inf_register_sip")) {
    "tdr_ims_inf_register_sip"
  } else if (nombreMinusculas.contains("tdr_ims_ip_message_sip")) {
    "tdr_ims_ip_message_sip"
  }else if (nombreMinusculas.contains("tdr_ims_miscellaneous_sip")) {
    "tdr_ims_miscellaneous_sip"
  } else if (nombreMinusculas.contains("tdr_ims_register_sip")) {
    "tdr_ims_register_sip"
  }else if (nombreMinusculas.contains("tdr_map_sms")) {
    "tdr_map_sms"
  } else if (nombreMinusculas.contains("tdr_volte_badipmos_info")) {
    "tdr_volte_badipmos_info"
  } else if (nombreMinusculas.contains("voip_detail_ufdr_voip")) {
    "voip"
  } else if (nombreMinusculas.contains("web_detail_ufdr_http_browsing")) {
    "web"
  } else if (nombreMinusculas.contains("webfail_detail_ufdr_http_browsing")) {
    "webfail"
  } else if (nombreMinusculas.contains("email")) {
    "email"
  } else {
    "sin_suscripcion" // Si no hay coincidencias, asignamos una carpeta por defecto
  }
}

// COMMAND ----------


val files = dbutils.fs.ls(path_origen).filter(file => file.name.endsWith(".gz"))

files.foreach { file =>
  // Obtenemos el nombre de la carpeta de destino según el nombre del archivo
  val nombreCarpeta = obtenerCarpetaDestino(file.name)
  
  // Ruta destino (creando una carpeta para cada suscripción dentro de /landing)
  val destino = s"$path_destino/$nombreCarpeta/${file.name}"

  // Verificar si la carpeta de destino existe, si no la crea
  val destinoCarpeta = s"$path_destino/$nombreCarpeta"
  
  // Verificar si el directorio destino ya existe, y si no crearlo
  try {
    val carpetaDestino = dbutils.fs.ls(destinoCarpeta)
    println(s"El directorio de destino ya existe: $destinoCarpeta")
  } catch {
    case e: Exception => 
      // Si no existe, creamos el directorio
      dbutils.fs.mkdirs(destinoCarpeta)
      println(s"Directorio creado: $destinoCarpeta")
  }

  // Mover el archivo a la carpeta correspondiente
  dbutils.fs.mv(file.path, destino)
  println(s"Archivo ${file.name} movido a $destino")
}

