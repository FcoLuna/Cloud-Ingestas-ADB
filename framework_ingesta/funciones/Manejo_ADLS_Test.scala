// Databricks notebook source
def exists_file(filepath: String): Boolean = {
  try
  {
    dbutils.fs.ls(filepath)
    return true
  }
  catch
  {
    case e: Exception => {
      if (e.getMessage.contains("The specified path does not exist.")) {
        return false
      } else {
        e.printStackTrace
        return false
      }
    }
  }
}

// COMMAND ----------

// MAGIC %md
// MAGIC ### Creación de archivos

// COMMAND ----------

def createNewFile(filepath: String, contents: String): Boolean = {
  try{
    if(exists_file(filepath)){
      return false
      }
    else{
      dbutils.fs.put(filepath, contents)
      return true
      }
  }
  catch {
    case e: Exception => e.printStackTrace
      return false
  }
}
