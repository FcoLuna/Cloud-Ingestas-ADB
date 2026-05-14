# Databricks notebook source
# MAGIC %md
# MAGIC ###Probar existencia de archivos en ADLS

# COMMAND ----------

def exists_file(filepath):
    try:
        dbutils.fs.ls(filepath)
        return True
    except Exception as e:
        if "The specified path does not exist." in str(e):
            return False
        else:
            import traceback
            traceback.print_exc()
            return False


# COMMAND ----------

# MAGIC %md
# MAGIC ### Creación directorios

# COMMAND ----------

def mkdir(dirPath):
    try:
        dbutils.fs.mkdirs(dirPath)
        return True
    except Exception as e:
        import traceback
        traceback.print_exc()
        return False

