// Databricks notebook source
// MAGIC %md
// MAGIC ### **VARIABLES**

// COMMAND ----------

var dir_adls = ""
dir_adls = dbutils.widgets.get("dir_adls")
val directoryPath = dir_adls
val files = dbutils.fs.ls(directoryPath)

// COMMAND ----------

// MAGIC %md
// MAGIC ### **LIMPIA**

// COMMAND ----------

files.foreach { file =>
  if (!file.name.contains("ind") && !file.name.contains("mmt") && !file.name.contains("pta") && !file.name.contains("ccp") && !file.name.contains("chi") && !file.name.contains("chl") && !file.name.contains("ges") && !file.name.contains("mm1") && !file.name.contains("mm3") && !file.name.contains("pe4") && !file.name.contains("sm2") && !file.name.contains("sm3")
  ) {
    dbutils.fs.rm(file.path, true)
  }
}
