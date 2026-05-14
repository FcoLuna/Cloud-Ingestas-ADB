// Databricks notebook source
// MAGIC %md
// MAGIC # Funciones

// COMMAND ----------

import java.sql.{Connection, DriverManager}
import java.text.SimpleDateFormat
import java.util.{Calendar, Properties}

// COMMAND ----------

// función para obtener fecha
def getDate(daysToProcess: Int, format: String): String = {
  val cal = Calendar.getInstance
  if (daysToProcess.!=(0))
    cal.add(Calendar.DAY_OF_YEAR, daysToProcess)
  val dateTime = cal.getTime
  val dateFormat = new SimpleDateFormat(format)
  val fecha = dateFormat.format(dateTime)
  fecha
}
