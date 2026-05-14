// Databricks notebook source
spark.conf.set("spark.databricks.delta.optimizeWrite.enabled", "true")
spark.conf.set("spark.databricks.delta.autoCompact.enabled", "true")
spark.conf.set("spark.sql.files.maxPartitionBytes", "64m") 
spark.conf.set("spark.sql.files.openCostInBytes", "4m")

spark.conf.set("spark.sql.shuffle.partitions", "200")
spark.conf.set("spark.databricks.delta.concurrentWrites", "true")

// COMMAND ----------

import org.apache.hadoop.conf.Configuration

import java.net.URI
import java.text.SimpleDateFormat
import java.util.Calendar
import scala.collection.mutable.{ArrayBuffer, ListBuffer}
import scala.util.Try

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

// COMMAND ----------


import org.apache.spark.sql.SparkSession

val spark = SparkSession.builder().getOrCreate()

// COMMAND ----------

//dbutils.widgets.text("path_stage","/data/trafico/senalizacion/contadores_nokia/stage_14_2/")
//dbutils.widgets.text("path_adls","abfss://ingestas@stbigdatadev02.dfs.core.windows.net")
//dbutils.widgets.text("path_destino","/data/trafico/senalizacion/contadores_nokia/")
//dbutils.widgets.text("catalogo","bi_ingestas")
//dbutils.widgets.text("esquema","raw_trafico")

// COMMAND ----------

val path_adls = dbutils.widgets.get("path_adls")
val path_stage = path_adls + dbutils.widgets.get("path_stage")
val path_destino = path_adls + dbutils.widgets.get("path_destino")
val catalogo = dbutils.widgets.get("catalogo")
val esquema = dbutils.widgets.get("esquema")

// COMMAND ----------

if (dbutils.fs.ls(path_stage).nonEmpty) {

  val schema = StructType(Array(
    StructField("_measInfoId", StringType, true),
    StructField("granPeriod", StructType(Array(
      StructField("_duration", StringType, true),
      StructField("_endTime", LongType, true)
    )), true),
    StructField("measTypes", StringType, true),
    StructField("measValue", StructType(Array(
      StructField("_measObjLdn", StringType, true),
      StructField("measResults", StringType, true)
    )), true),
    StructField("repPeriod", StructType(Array(
      StructField("_duration", StringType, true)
    )), true)
  ))

  // Lectura y transformación inicial de los datos
  val df_measInfoG = spark.read
    .format("xml")
    .option("rowTag", "measInfo")
    .schema(schema)
    .option("recursiveFileLookup", "true")
    .load(path_stage)
    .select(
      col("_measInfoId"),
      col("granPeriod._duration").as("duracion"),
      col("granPeriod._endTime").as("endTime"),
      col("measTypes"),
      col("measValue._measObjLdn").as("measObjLdn"),
      col("measValue.measResults").as("measResults"),
      col("repPeriod._duration").as("granPeriodduracion")
    )
    .dropDuplicates()
    .withColumn("contadores", split(col("measTypes"), " "))
    .withColumn("valores", split(col("measResults"), " "))
    .withColumn("bigdata_close_date", current_date())
    .withColumn("bigdata_ctrl_id", expr("unix_timestamp()").cast("long"))
    .withColumn("year", substring(from_unixtime(col("endTime") / 1000000, "yyyy-MM-dd HH:mm:ss"), 1, 4))
    .withColumn("month", substring(from_unixtime(col("endTime") / 1000000, "yyyy-MM-dd HH:mm:ss"), 6, 2))
    .withColumn("day", substring(from_unixtime(col("endTime") / 1000000, "yyyy-MM-dd HH:mm:ss"), 9, 2))
    .withColumn("hour", substring(from_unixtime(col("endTime") / 1000000, "yyyy-MM-dd HH:mm:ss"), 12, 2))

val listaFamilias = List("lte_cell_load","lte_pwr_and_qual_ul","lte_s1ap","lte_intra_enb_handover_extension","lte_x2_gnb","lte_transport_load","lte_x2ap","lte_mbms","lte_sinr","lte_handover","lte_inter_enb_ho","lte_volte_bler_histogram","cat_m_accessibility","lte_cell_throughput","lte_mobility_events","lte_rsrp_and_rsrq_histogram","lte_inter_sys_ho","lte_rrc","lte_inter_enb_via_x2_handover","lte_eutra_carrier_frequency","lte_received_interference_power","lte_mac","lte_ho_rlf_trigger","lte_gnss","lte_ran_sharing","lte_broadcast","lte_mro_utran_frequency_related","lte_eps_bearer","lte_cell_avail","lte_volte_voice_break_period_histogram","lte_pwr_and_qual_dl","lte_ue_state","lte_m_per_lncel","lte_enb_load","lte_s1_sctp_statistics","lte_x2_gnb_per_lncel","lte_qos","lte_radio_bearer","lte_ue_and_servdiff","lte_inter_home_enb_handover","lte_m_per_lnbts","lte_inter_enb_via_s1_handover","lte_ue_quantity","lte_equivalent_isotropic_radiated_power","lte_isys_ho_utran_nb","cat_m_power_and_quality","lte_sales_item_monitoring","cat_m_mobility","cat_m_retainability","lte_proximity_services","lte_cell_resource","lte_neighb_cell_ho","cat_m_usage","lte_x2_sctp_statistics","lte_intra_enb_ho","lte_nb_iot","cell_resource","intra_system_handover","hsdpa_wbts","rrc","traffic","service_level","rcpm_olpc_wcel","lte_nb_iot_bts_level_measurements","packet_call","cat_m_integrity","soft_handover","lte_vlan_ip_stats","loose_phase_and_time_sync_stats","lte_ethif_stats","lte_twamp_stats","lte_ipsec","lte_ethernet_link","ntp_stats","lte_port_network_access_security_stats","lte_vlan_ipv4_stats","vlan_stats","lte_ip_filtering","lte_vlan_ipv6_stats","lte_tac_stats","cell_thrput","rcpm_rlc_rnc","lte_top_freqsync_stats","srb3c","rsadu","ne1un","nngcc","sbts_rfm_energy_monitoring","nrbc","tcp_udp_stats","nrrcd","nxncc","nrngu","nifc","nf1uu","pdcp2","phb_stats","nmsdu","necup","ltemo","nrmg","ipv6_rx_phb_stats","nx2cc","sbts_sm_energy_monitoring","ndlsq","ninfc","nnsaue","nrmo","nlnrl","scdsch_stats","ethlk_stats","srb3d","sbts_ncpri","nngcb","nmocu","sbts_sfp_rmod","sbts_bbmod_temperature","nrasu","nmpdu","ne1us","nf1ud","nrta","ipv6_interface_stats","ncac","ltemg","nx2ub","ethif_stats","racdu","nreirp","sbts_smod_temperature","nrlfc","ne1cu","ipv4_interface_stats","ncadu","nlrlc","ipv4_rx_phb_stats","ntraf","nf1ub","raccu","ne1cb","nrach","nhrlc","ncela","nrans","pdcp1","ncav","ne1cn","sbts_sfp_bbmod","nrbf","ip_filtering_stats","ne1cs","nmodu","nrrcc","nxncb","rsacu","bridge_port_stats","ip_data_traffic_volume_stats","nf1cd","ipif_stats","sbts_rmod_temperature","sctp_stats","ndlhq","ipno_stats","ns1ub","nulhq","nx2cb","nulsq","nf1cc","sbts_energy_consumption","hspa_wbts_extension","rcpm_ueq","rcpm_olpc_rnc","signaling_load_wbts","dl_pwr_wcel","wbts_level_monitoring","wcdma_si_monitoring","wbts_r99_hw","frame_protocol_wbts","cell_throughput_wbts","hspa_scheduler_monitoring","ngns","sfpbm","sfprm","rnc_ip_cac","ethernet_link_utilization_stats","fsch_stats","ipv6_tx_phb_stats","ethernet_interface_utilization_stats","ipv4_tx_phb_stats","vlan_interface_utilization_stats","ipv4_addr_stats","twamp_stats","ethernet_interface_utilization_untagged_frames_stats","ip_based_route","ftm_top_freqsync_statistics","lte_dac_word_statistics","l3reloc_v2","sbts_antenna_line","sfp_bbmod","ftm_ethernet_interface","inter_system_handover","sfp_rmod","s1_x2_tac_stats","lte_baseband_pool_monitoring","availability","vlan_phb_stats","vlan_ip_stats","ftm_stp_stats","dsp_state","ftm_phb_stats","overload_wac","ftm_ethernet_link","ftm_ip_security","top_freqsync_stats","ftm_ip_filtering","ftm_ip_stats","ethernet_perf","atm_virtual_channel_new","ftm_aal2_sched","ftm_twamp","top_phasesync_stats","aal5meadmx","ftm_cesopsn_if","ftm_pdh_if","intra_system_hho_rnc","ftm_atm_vc","aal2_at_uni_new","m3ua","soft_handover_rnc","ftm_l2swi_stats","ftm_atm_if","device_detection","aal2_resource_res_new","rnc_rtp_rtcp","autodef_ifho_v2","autodef_sho_v2","l3iub","rcpm_rlc_wcel","rcpm_olpc","nrrip","nruc","autodef_isho_v2","sbts_sfp_smod","lte_active_users_and_latency_statistics_per_pmqap_profile","lte_rlf_statistics_per_pmqap_profile","lte_erab_statistics_per_pmqap_profile","lte_handovers_per_pmqap_profile","lte_mbms_per_lnbts","unit_load","sfp_smod","aal5meachorus","ip_qos","fsm_level_monitoring","udp_meas_on_ip","l3iu","ftm_atm_vp","vcc_bdl_bw_utl","dsp_res_util_v2","rnc_capa_usage","aal2_at_nni_new")

var contador = 1
listaFamilias.foreach { fami =>
    Try {
      val df_fam = df_measInfoG.filter(lower(col("_measInfoId")) === fami)
      val ruta_final = s"$path_destino/$fami/raw"
      val tabla_salida = s"$catalogo.$esquema.nokia_nt_$fami"

      println(s"Posicion: $contador - Escribiendo en path: $ruta_final y en la tabla: $tabla_salida")
      if (!spark.catalog.tableExists(tabla_salida)) {
        // Crear la tabla si no existe
        df_fam.repartition(10)
          .write
          .mode("overwrite")
          .format("delta")
          .partitionBy("year", "month", "day")
          .option("path", ruta_final)
          .saveAsTable(tabla_salida)
      } else {
        // Agregar datos si la tabla ya existe
        df_fam.repartition(10)
          .write
          .mode("append")
          .format("delta")
          .option("path", ruta_final)
          .saveAsTable(tabla_salida)
      }
      contador += 1
    }.recover {
      case e: Exception =>
        println(s"Error procesando familia $fami: ${e.getMessage}")
    }
  }
} else {
  println("No hay archivos en la carpeta.")
}

// COMMAND ----------

path_stage

// COMMAND ----------

dbutils.fs.rm(path_stage,true)
