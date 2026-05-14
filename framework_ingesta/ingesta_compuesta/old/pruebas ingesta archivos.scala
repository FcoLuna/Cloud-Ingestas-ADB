// Databricks notebook source
// MAGIC %run /Shared/framework_ingesta/funciones/old/funciones_ingesta_simple_v3

// COMMAND ----------

// display(dbutils.fs.ls("abfss://bigdata@stbigdatadev02.dfs.core.windows.net/modeols/parametros_ingesta3/"))

// COMMAND ----------

val dir_descarga= "abfss://bigdata@stbigdatadev02.dfs.core.windows.net/modelos/parametros_ingestas/"
// mkdir(dir_descarga+"/processing")
// mkdir(dir_descarga+"/processing_error")
// mkdir(dir_descarga+"/stage")
val nombre_archivo = "parametros_ingesta4"
println(nombre_archivo)
// mkdir(dir_descarga+"/conformado")
// mkdir(dir_descarga+"/raw")

// COMMAND ----------

//spark.read.parquet("abfss://contenedor-test@stbigdatadev02.dfs.core.windows.net/data/producto_asignado/productos_sva/gestor_ott/parque_ott/raw")

// COMMAND ----------

// MAGIC %md
// MAGIC ## CREACION DE HQL Y JSON

// COMMAND ----------

val content2 = """{
  "SMARTCARE": [
    {
      "host_sftp": "10.179.201.194",
	  "port_sftp": 22,
	  "user_sftp": "dsi_user2",
	  "dir_sftp": ""
    },
    {
      "host_sftp": "10.179.201.195",
	  "port_sftp": 22,
	  "user_sftp": "dsi_user2",
	  "dir_sftp": ""
    },
	{
      "host_sftp": "10.179.201.196",
	  "port_sftp": 22,
	  "user_sftp": "dsi_user2",
	  "dir_sftp": ""
    },
	{
      "host_sftp": "10.179.201.197",
	  "port_sftp": 22,
	  "user_sftp": "dsi_user2",
	  "dir_sftp": ""
    },
	{
      "host_sftp": "10.179.201.198",
	  "port_sftp": 22,
	  "user_sftp": "dsi_user2",
	  "dir_sftp": ""
    },
  ]
}'"""
makeTxtFile(dir_descarga + "/" + nombre_archivo + ".json", content2)

