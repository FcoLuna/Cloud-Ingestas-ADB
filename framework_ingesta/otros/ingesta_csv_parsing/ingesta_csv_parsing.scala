// Databricks notebook source
// // package com.tchile.bigdata.etl

// import java.text.SimpleDateFormat
// import java.time.LocalDateTime
// import java.util.Date
// import java.time.format.DateTimeFormatter
// import java.util.concurrent.TimeUnit
// import java.util.Calendar
// import org.apache.spark.SparkConf
// import org.apache.spark.sql.SaveMode
// import org.apache.spark.sql.SparkSession
// import org.apache.spark.sql.types.StringType
// import org.apache.spark.sql.types.StructField
// import org.apache.spark.sql.types.StructType
// import org.apache.spark.sql.functions._
// import org.apache.spark.sql.types.DataType
// import org.apache.spark.sql.types.IntegerType
// import org.apache.spark.sql.expressions.Window
// // import com.tchile.bigdata.hdfs.ManejoHdfs

// // Librerías HDFS
// import org.apache.hadoop.fs.FileStatus
// import org.apache.hadoop.fs.FileSystem
// import org.apache.hadoop.fs.Path
// import org.apache.hadoop.fs.LocatedFileStatus
// import org.apache.hadoop.fs.RemoteIterator

// // Conexión al FileSystem del cluster
// import org.apache.hadoop.conf.Configuration
// val abfs = FileSystem.get(new Configuration())

// COMMAND ----------

// MAGIC %python
// MAGIC import subprocess
// MAGIC import sys
// MAGIC from pyspark.sql.functions import *
// MAGIC from pyspark import SparkConf, SparkContext
// MAGIC from pyspark.sql import SparkSession
// MAGIC
// MAGIC conf =  SparkConf() \
// MAGIC            .setMaster("yarn") \
// MAGIC            .setAppName("csv_parser") \
// MAGIC            .set("spark.driver.allowMultipleContexts", "true") \
// MAGIC            .set("spark.yarn.queue", "ingesta") \
// MAGIC            .set("spark.sql.crossJoin.enabled", "true") \
// MAGIC            .set("spark.sql.debug.maxToStringFields", "1000") \
// MAGIC            .set("spark.sql.autoBroadcastJoinThreshold", "-1")
// MAGIC
// MAGIC spark = SparkSession.builder \
// MAGIC     .appName("csv_parser") \
// MAGIC     .getOrCreate()
// MAGIC
// MAGIC # parametros_in = sys.argv[1].split(':')
// MAGIC ### AGREGUÉ MENSAJE NIFI PORQUE NO ENCONTRE DONDE PASA LAS VARIABLES
// MAGIC mensaje_nifi = ";:false:nps_respuestas_fija/parsing_in/20231010103145:nps_respuestas_fija/parsing_out/20231010103145:false:\\"
// MAGIC parametros_in = mensaje_nifi.split(':')
// MAGIC delimitador = parametros_in[0]
// MAGIC encabezado = parametros_in[1]
// MAGIC dir_hdfs_in = "/mnt/flightdata-TestKeyVault/" + parametros_in[2]
// MAGIC dir_hdfs_out = "/mnt/flightdata-TestKeyVault/" + parametros_in[3]
// MAGIC quoteAll = parametros_in[4]
// MAGIC escape = parametros_in[5] or "\\"

// COMMAND ----------

// MAGIC %python
// MAGIC df = spark.read.option("header",encabezado).option("delimiter",delimitador).option("multiLine","true").option("escape",escape).csv(dir_hdfs_in)
// MAGIC display(df)

// COMMAND ----------

// MAGIC %python
// MAGIC df2 = df.select(*(regexp_replace(col(c),"[\r\n]+","").alias(c) for c in df.columns))
// MAGIC display(df2)

// COMMAND ----------

// MAGIC %python
// MAGIC df3 = df2.select(*(regexp_replace(col(c),"\\s+"," ").alias(c) for c in df.columns))
// MAGIC display(df3)

// COMMAND ----------

// MAGIC %python
// MAGIC df3.write.option("header",encabezado).option("delimiter",delimitador).option("quoteAll",quoteAll).csv(dir_hdfs_out)

// COMMAND ----------

// subprocess.call(["hadoop", "fs", "-rm", "-r", "-skipTrash", dir_hdfs_in])
