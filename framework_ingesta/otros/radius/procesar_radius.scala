// Databricks notebook source
dbutils.widgets.text("adls_container","abfss://ingestas@stbigdatadev02.dfs.core.windows.net")
val adls_container = dbutils.widgets.get("adls_container")

dbutils.widgets.text("dir_adls_rel","/data/trafico/trafico_red/radius")
val dir_adls_rel = dbutils.widgets.get("dir_adls_rel")

dbutils.widgets.text("catalogo","bi_ingestas")
val catalogo = dbutils.widgets.get("catalogo")

val path_radius=adls_container+dir_adls_rel+"/raw"
val sourceDirectory = adls_container + dir_adls_rel + "/landing/"

// COMMAND ----------

import org.apache.spark.sql.{SparkSession, Row, DataFrame}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Encoder, Encoders}
import java.text.SimpleDateFormat
import java.sql.Timestamp

import scala.io.Source
import scala.collection.JavaConverters._
import spark.implicits._

spark.sql("set spark.sql.legacy.timeParserPolicy=LEGACY")

// Function to process the content of the files and handle repeated fields
def processLines(
  lines: List[String], 
  location: String, 
  sequence: String, 
  year: String, 
  month: String, 
  day: String,
  partition_date: String
): Seq[Row] = {
  var result = Seq[Row]()
  var currentRow = Map[String, String]()
  val dateFormat = new SimpleDateFormat("EEE MMM dd HH:mm:ss yyyy")
  val dateFormat2 = new SimpleDateFormat("MMM dd yyyy HH:mm:ss X")
  val pattern = "^[A-Z].*".r
  
  lines.foreach { line =>
    if (pattern.findFirstIn(line).isDefined) {
      if (currentRow.nonEmpty) {
        result = result :+ Row(
      new Timestamp(dateFormat.parse(currentRow.getOrElse("Fecha",null)).getTime),
      currentRow.getOrElse("Acct-Status-Type", null),
      currentRow.getOrElse("User-Name", null),
      new Timestamp(dateFormat2.parse(currentRow.getOrElse("Event-Timestamp",null)).getTime),
      try { currentRow.getOrElse("Acct-Delay-Time", null).toInt } catch { case _: NumberFormatException => null },
      currentRow.getOrElse("NAS-Identifier", null),
      currentRow.getOrElse("Acct-Session-Id", null),
      currentRow.getOrElse("NAS-IP-Address", null),
      currentRow.getOrElse("Service-Type", null),
      currentRow.getOrElse("Framed-Protocol", null),
      currentRow.getOrElse("Framed-Compression", null),
      currentRow.getOrElse("ERX-Pppoe-Description", null),
      currentRow.getOrElse("Framed-IP-Address", null),
      currentRow.getOrElse("Framed-IP-Netmask", null),
      currentRow.getOrElse("ERX-Ingress-Policy-Name", null),
      currentRow.getOrElse("ERX-Egress-Policy-Name", null),
      currentRow.getOrElse("Calling-Station-Id", null),
      try { currentRow.getOrElse("Acct-Input-Gigawords", null).toInt } catch { case _: NumberFormatException => null },
      try { currentRow.getOrElse("Acct-Input-Octets", null).toLong } catch { case _: NumberFormatException => null },
      try { currentRow.getOrElse("Acct-Output-Gigawords", null).toInt } catch { case _: NumberFormatException => null },
      try { currentRow.getOrElse("Acct-Output-Octets", null).toLong } catch { case _: NumberFormatException => null },
      try { currentRow.getOrElse("ERX-Input-Gigapkts", null).toInt } catch { case _: NumberFormatException => null },
      try { currentRow.getOrElse("Acct-Input-Packets", null).toLong } catch { case _: NumberFormatException => null },
      try { currentRow.getOrElse("ERX-Output-Gigapkts", null).toInt } catch { case _: NumberFormatException => null },
      try { currentRow.getOrElse("Acct-Output-Packets", null).toLong } catch { case _: NumberFormatException => null },
      currentRow.getOrElse("NAS-Port-Type", null),
      try { currentRow.getOrElse("NAS-Port", null).toLong } catch { case _: NumberFormatException => null },
      currentRow.getOrElse("NAS-Port-Id", null),
      currentRow.getOrElse("Acct-Authentic", null),
      try { currentRow.getOrElse("Acct-Session-Time", null).toLong } catch { case _: NumberFormatException => null },
      currentRow.getOrElse("Acct-Terminate-Cause", null),
      try { currentRow.getOrElse("Timestamp", null).toLong } catch { case _: NumberFormatException => null },
      currentRow.getOrElse("Filter-Id", null),
      currentRow.getOrElse("ERX-Dhcp-Mac-Addr", null),
      currentRow.getOrElse("ERX-Service-Bundle", null),
      currentRow.getOrElse("ERX-IPv6-Acct-Input-Octets", null),
      try { currentRow.getOrElse("ERX-IPv6-Acct-Output-Octets", null).toInt } catch { case _: NumberFormatException => null },
      try { currentRow.getOrElse("ERX-IPv6-Acct-Input-Packets", null).toInt } catch { case _: NumberFormatException => null },
      try { currentRow.getOrElse("ERX-IPv6-Acct-Output-Packets", null).toInt } catch { case _: NumberFormatException => null },
      try { currentRow.getOrElse("ERX-IPv6-Acct-Input-Gigawords", null).toInt } catch { case _: NumberFormatException => null },
      try { currentRow.getOrElse("ERX-IPv6-Acct-Output-Gigawords", null).toInt } catch { case _: NumberFormatException => null },
      try { currentRow.getOrElse("Connect-Info", null).toInt } catch { case _: NumberFormatException => null },
      currentRow.getOrElse("Tunnel-Type:0", null),
      currentRow.getOrElse("Tunnel-Medium-Type:0", null),
      currentRow.getOrElse("Tunnel-Client-Endpoint:0", null),
      currentRow.getOrElse("Tunnel-Client-Auth-Id:0", null),
      currentRow.getOrElse("Tunnel-Server-Endpoint:0", null),
      currentRow.getOrElse("Tunnel-Server-Auth-Id:0", null),
      currentRow.getOrElse("Tunnel-Assignment-Id:0", null),
      currentRow.getOrElse("Acct-Tunnel-Connection", null),
      currentRow.getOrElse("Framed-Route", null),
      currentRow.getOrElse("Attr-26", null),
      location, 
      sequence,
      partition_date,
      year, 
      month, 
      day 
        )
      }
      currentRow = Map("Fecha" -> line.trim)
    } else if (line.contains("=")) {
      val Array(fieldx, value) = line.split("=").map(_.trim)
      val field = if (fieldx.startsWith("Attr-26")) "Attr-26" else fieldx
      val value2 = value.replace("'", "")
      currentRow += (field -> currentRow.get(field).map(_ + "|" + value2).getOrElse(value2))
    }
  }

  if (currentRow.nonEmpty) {
    result = result :+ Row(
      new Timestamp(dateFormat.parse(currentRow.getOrElse("Fecha",null)).getTime),
      currentRow.getOrElse("Acct-Status-Type", null),
      currentRow.getOrElse("User-Name", null),
      new Timestamp(dateFormat2.parse(currentRow.getOrElse("Event-Timestamp",null)).getTime),
      try { currentRow.getOrElse("Acct-Delay-Time", null).toInt } catch { case _: NumberFormatException => null },
      currentRow.getOrElse("NAS-Identifier", null),
      currentRow.getOrElse("Acct-Session-Id", null),
      currentRow.getOrElse("NAS-IP-Address", null),
      currentRow.getOrElse("Service-Type", null),
      currentRow.getOrElse("Framed-Protocol", null),
      currentRow.getOrElse("Framed-Compression", null),
      currentRow.getOrElse("ERX-Pppoe-Description", null),
      currentRow.getOrElse("Framed-IP-Address", null),
      currentRow.getOrElse("Framed-IP-Netmask", null),
      currentRow.getOrElse("ERX-Ingress-Policy-Name", null),
      currentRow.getOrElse("ERX-Egress-Policy-Name", null),
      currentRow.getOrElse("Calling-Station-Id", null),
      try { currentRow.getOrElse("Acct-Input-Gigawords", null).toInt } catch { case _: NumberFormatException => null },
      try { currentRow.getOrElse("Acct-Input-Octets", null).toLong } catch { case _: NumberFormatException => null },
      try { currentRow.getOrElse("Acct-Output-Gigawords", null).toInt } catch { case _: NumberFormatException => null },
      try { currentRow.getOrElse("Acct-Output-Octets", null).toLong } catch { case _: NumberFormatException => null },
      try { currentRow.getOrElse("ERX-Input-Gigapkts", null).toInt } catch { case _: NumberFormatException => null },
      try { currentRow.getOrElse("Acct-Input-Packets", null).toLong } catch { case _: NumberFormatException => null },
      try { currentRow.getOrElse("ERX-Output-Gigapkts", null).toInt } catch { case _: NumberFormatException => null },
      try { currentRow.getOrElse("Acct-Output-Packets", null).toLong } catch { case _: NumberFormatException => null },
      currentRow.getOrElse("NAS-Port-Type", null),
      try { currentRow.getOrElse("NAS-Port", null).toLong } catch { case _: NumberFormatException => null },
      currentRow.getOrElse("NAS-Port-Id", null),
      currentRow.getOrElse("Acct-Authentic", null),
      try { currentRow.getOrElse("Acct-Session-Time", null).toLong } catch { case _: NumberFormatException => null },
      currentRow.getOrElse("Acct-Terminate-Cause", null),
      try { currentRow.getOrElse("Timestamp", null).toLong } catch { case _: NumberFormatException => null },
      currentRow.getOrElse("Filter-Id", null),
      currentRow.getOrElse("ERX-Dhcp-Mac-Addr", null),
      currentRow.getOrElse("ERX-Service-Bundle", null),
      currentRow.getOrElse("ERX-IPv6-Acct-Input-Octets", null),
      try { currentRow.getOrElse("ERX-IPv6-Acct-Output-Octets", null).toInt } catch { case _: NumberFormatException => null },
      try { currentRow.getOrElse("ERX-IPv6-Acct-Input-Packets", null).toInt } catch { case _: NumberFormatException => null },
      try { currentRow.getOrElse("ERX-IPv6-Acct-Output-Packets", null).toInt } catch { case _: NumberFormatException => null },
      try { currentRow.getOrElse("ERX-IPv6-Acct-Input-Gigawords", null).toInt } catch { case _: NumberFormatException => null },
      try { currentRow.getOrElse("ERX-IPv6-Acct-Output-Gigawords", null).toInt } catch { case _: NumberFormatException => null },
      try { currentRow.getOrElse("Connect-Info", null).toInt } catch { case _: NumberFormatException => null },
      currentRow.getOrElse("Tunnel-Type:0", null),
      currentRow.getOrElse("Tunnel-Medium-Type:0", null),
      currentRow.getOrElse("Tunnel-Client-Endpoint:0", null),
      currentRow.getOrElse("Tunnel-Client-Auth-Id:0", null),
      currentRow.getOrElse("Tunnel-Server-Endpoint:0", null),
      currentRow.getOrElse("Tunnel-Server-Auth-Id:0", null),
      currentRow.getOrElse("Tunnel-Assignment-Id:0", null),
      currentRow.getOrElse("Acct-Tunnel-Connection", null),
      currentRow.getOrElse("Framed-Route", null),
      currentRow.getOrElse("Attr-26", null),
      location, 
      sequence,
      partition_date,  
      year, 
      month, 
      day 
    )
  }
  result
}

// Function to read and process a file from ADLS
def processFileFromADLS(filePath: String): Seq[Row] = {
  val fileDF: DataFrame = spark.read.text(filePath)
  val lines = fileDF.as[String].collect().toList  
  val fileName = filePath.split("/").last
  val ip = fileName.split("_")(2)
  val location = ip match {
    case "172.25.1.1" => "huasco"
    case "172.25.1.2" => "elqui"
    case "172.25.1.3" => "maipo"
    case "172.25.1.6" => "puangue"
    case "172.25.1.9" => "rapel"
    case "172.25.1.12" => "teno"
    case _ => "NF"
  }
  val file_sequence = fileName.split("_")(3).split("\\.")(0)
  val year = fileName.substring(0, 4)
  val month = fileName.substring(4, 6)
  val day = fileName.substring(6, 8)
  val partition_date = s"$year-$month-$day"
  processLines(lines, location, file_sequence, year, month, day,partition_date)
}

// Define the schema of the DataFrame
val schema = StructType(Array(
  StructField("fecha", TimestampType, true),
  StructField("acct_status_type", StringType, true),
  StructField("user_name", StringType, true),
  StructField("event_date", TimestampType, true),
  StructField("acct_delay_time", IntegerType, true),
  StructField("nas_identifier", StringType, true),
  StructField("acct_session_id", StringType, true),
  StructField("nas_ip_address", StringType, true),
  StructField("service_type", StringType, true),
  StructField("framed_protocol", StringType, true),
  StructField("framed_compression", StringType, true),
  StructField("erx_pppoe_description", StringType, true),
  StructField("frmaed_ip_address", StringType, true),
  StructField("framed_ip_netmask", StringType, true),
  StructField("erx_ingress_policy_name", StringType, true),
  StructField("erx_egress_policy_name", StringType, true),
  StructField("callid_station_id", StringType, true),
  StructField("acct_input_gigawords", IntegerType, true),
  StructField("acct_input_octets", LongType, true),
  StructField("acct_output_gigawords", IntegerType, true),
  StructField("acct_output_octets", LongType, true),
  StructField("erx_input_gigapkts", IntegerType, true),
  StructField("acct_input_packets", LongType, true),
  StructField("erx_output_gigapkts", IntegerType, true),
  StructField("acct_output_packets", LongType, true),
  StructField("nas_port_type", StringType, true),
  StructField("nas_port", LongType, true),
  StructField("nas_port_id", StringType, true),
  StructField("acct_authentic", StringType, true),
  StructField("acct_session_time", LongType, true),
  StructField("acct_terminate_cause", StringType, true),
  StructField("unix_time", LongType, true),
  StructField("filter_iderx_attr_106", StringType, true),
  StructField("erx_dhcp_mac_addr", StringType, true),
  StructField("erx_service_bundle", StringType, true),
  StructField("erx_ipv6_acct_input_octets", StringType, true),
  StructField("erx_ipv6_acct_output_octets", IntegerType, true),
  StructField("erx_ipv6_acct_input_packets", IntegerType, true),
  StructField("erx_ipv6_acct_output_packets", IntegerType, true),
  StructField("erx_ipv6_acct_input_gigawords", IntegerType, true),
  StructField("erx_ipv6_acct_output_gigawords", IntegerType, true),
  StructField("connect_info", IntegerType, true),
  StructField("tunnel_type", StringType, true),
  StructField("tunnel_medium", StringType, true),
  StructField("tunnel_client_endpoint", StringType, true),
  StructField("tunnel_client_auth_id", StringType, true),
  StructField("tunnel_server_endpoint", StringType, true),
  StructField("tunnel_server_auth_id", StringType, true),
  StructField("tunnel_assignment_id", StringType, true),
  StructField("acct_tunnel_connection", StringType, true),
  StructField("framed_route", StringType, true),
  StructField("attr_26", StringType, true),
  StructField("location", StringType, true),
  StructField("file_sequence", StringType, true),
  StructField("partition_date", StringType, true),
  StructField("year", StringType, true),
  StructField("month", StringType, true),
  StructField("day", StringType, true)
))



val files = dbutils.fs.ls(sourceDirectory).map(_.path).filter(_.endsWith(".txt"))

if(files.nonEmpty )
{
  files.grouped(30).foreach { batchFiles =>
    val allRows = batchFiles.flatMap(processFileFromADLS)
    val df: DataFrame = spark.createDataFrame(allRows.asJava, schema)
    df.write.format("delta").mode("append").partitionBy("year", "month", "day").option("path", path_radius).saveAsTable(catalogo + ".raw_trafico.radius")
    batchFiles.foreach(file => dbutils.fs.rm(file))
  }
}
