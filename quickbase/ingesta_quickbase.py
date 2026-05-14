# Databricks notebook source
# MAGIC %md
# MAGIC ### Variables y Parametros

# COMMAND ----------

# MAGIC %scala
# MAGIC println("[INFO] Parametros=====")
# MAGIC //RESQUEST
# MAGIC val v_request = dbutils.widgets.get("v_request")
# MAGIC //TOKEN
# MAGIC val v_token = dbutils.widgets.get("v_token")
# MAGIC //ID
# MAGIC val v_id = dbutils.widgets.get("v_id")
# MAGIC // @concat(pipeline().parameters.dir_adls_rel, '/stage')
# MAGIC val ruta_stage = dbutils.widgets.get("ruta_stage")

# COMMAND ----------

# MAGIC %md
# MAGIC ### CONSULTA APPI CON DATA

# COMMAND ----------

import requests
import pandas as pd
from io import StringIO #libreria para trabajar con cadenas de texto como un archivo
 
 
#variables
#v_request = 'https://tchile.quickbase.com/db/bqj7htjtg'
#v_token = 'b3rgb9_jxig_0_b2ncaagbsbssy9btptwbtcnntv5r'
#v_id = 6

v_request = dbutils.widgets.get("v_request")
v_token = dbutils.widgets.get("v_token")
v_id = dbutils.widgets.get("v_id")
 
#headers
headers = {
    'Content-Type': 'application/xml',
    'QUICKBASE-ACTION': 'API_GenResultsTable'
}
 
#solicitud
data = f'''
<qdbapi>
    <usertoken>{v_token}</usertoken>
    <qid>{v_id}</qid>
    <options>csv</options>
</qdbapi>
'''
 
#solicitud POST
response = requests.post(v_request, headers=headers, data=data)
datos = response.text
df = pd.read_csv(StringIO(datos))
 
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ###  CARGAR EN EL STAGE LA DATA

# COMMAND ----------

# MAGIC %scala
# MAGIC df.count
