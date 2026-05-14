# Databricks notebook source
# MAGIC %run ../funciones/Manejo_ADLS

# COMMAND ----------

# MAGIC %scala
# MAGIC var dir_adls = "abfss://contenedor-test@stbigdatadev02.dfs.core.windows.net/modelos/"
# MAGIC
# MAGIC mkdir(dir_adls+"/processing")
# MAGIC mkdir(dir_adls+"/processing_error")
# MAGIC mkdir(dir_adls+"/stage")
# MAGIC mkdir(dir_adls+"/stage_error")
# MAGIC mkdir(dir_adls+"/landing")
# MAGIC mkdir(dir_adls+"/raw")
# MAGIC mkdir(dir_adls+"/conformado")
# MAGIC
# MAGIC var fecha_aux = "2000-01-01T01:01:01.001Z"
# MAGIC makeTxtFile(dir_adls + "/last_ingest_time.txt", "timestamp;\n"+fecha_aux+";")
