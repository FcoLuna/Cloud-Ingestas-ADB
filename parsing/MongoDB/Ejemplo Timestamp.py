# Databricks notebook source
from pyspark.sql.functions import col, from_unixtime

epoch_columns = ["fechaMovimiento","fechaVigencia"]

for col_name in epoch_columns:
    print(col_name)
    df = df.withColumn(f"{col_name}_con_formato", from_unixtime(col(col_name) / 1000, "yyyy-MM-dd HH:mm:ss"))
    df = df.drop(col(col_name))
    df = df.withColumnRenamed(f"{col_name}_con_formato", col_name)
display(df)
