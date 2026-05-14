# Databricks notebook source
import requests
from io import StringIO
import re
import pandas as pd
from datetime import datetime, timedelta
import pytz

# COMMAND ----------

# Set the timezone to GMT
gmt = pytz.timezone('GMT')

# Calculate the current time minus 24 hours
now = datetime.now(gmt)
nw = int((now - timedelta(days=1)).timestamp() * 1000)
nw_yyyyMMdd = (now - timedelta(days=1)).strftime('%Y%m%d')

# Calculate ts_from and ts_to
ts_from = int((now - timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0).timestamp() * 1000)
ts_to = int((now - timedelta(days=1)).replace(hour=23, minute=59, second=59, microsecond=999999).timestamp() * 1000)

print(f"nw: {nw}")
print(f"nw_yyyyMMdd: {nw_yyyyMMdd}")
print(f"ts_from: {ts_from}")
print(f"ts_to: {ts_to}")

# COMMAND ----------

url = 'https://apiv2-eu.devo.com/search/query'
headers = {
    'Content-Type': 'application/json',
    'Authorization': 'Bearer e9280a84c08ab7ed0d73aea4b6136759'
}
data = {
    "query": "from my.synthesis.chile select *",
    "from": str(ts_from)[:-3],  # Remove the last 3 digits to match the original format
    "to": str(ts_to)[:-3],      # Remove the last 3 digits to match the original format
    "mode": {"type": "csv"},
    "dateFormat": "default"
}


# COMMAND ----------

response = requests.post(url, headers=headers, json=data)
csv_data = response.text

# COMMAND ----------

print(csv_data)

# COMMAND ----------

# Remove unwanted ASCII control characters
cleaned_data = re.sub(r'[\x0F\x08\x02\x06\x0E\x18\x19\x7F]', '', csv_data)
cleaned_data = cleaned_data.replace('\r', '').replace('""', '')

# COMMAND ----------

# Load the cleaned data into a Pandas DataFrame
data = StringIO(cleaned_data)
df = pd.read_csv(data)

# COMMAND ----------

# Filter rows with exactly 57 fields
filtered_df = df[df.apply(lambda x: len(x) == 57, axis=1)]

# COMMAND ----------

# Display the filtered DataFrame
filtered_df.head()

# COMMAND ----------

filtered_df['ssid_24ghz'] = filtered_df['ssid_24ghz'].astype(str)
filtered_df['ssid_5ghz'] = filtered_df['ssid_5ghz'].astype(str)
filtered_df['ssid_guest'] = filtered_df['ssid_5ghz'].astype(str)
spark_df = spark.createDataFrame(filtered_df)

# COMMAND ----------

spark_df.repartition(1).write.mode("overwrite").format("csv").save("abfss://ingestas@stbigdatadev02.dfs.core.windows.net/data/gestion_recursos/operacion_red/smart_wifi/my_synthesis_chile/landing")

# COMMAND ----------

display(dbutils.fs.ls("abfss://ingestas@stbigdatadev02.dfs.core.windows.net/data/gestion_recursos/operacion_red/smart_wifi/my_synthesis_chile/landing"))

# COMMAND ----------

lista_archivos = dbutils.fs.ls("abfss://ingestas@stbigdatadev02.dfs.core.windows.net/data/gestion_recursos/operacion_red/smart_wifi/my_synthesis_chile/landing")
for elemento in lista_archivos:
    if ".csv" in elemento.path:
        dbutils.fs.mv(elemento.path, "abfss://ingestas@stbigdatadev02.dfs.core.windows.net/data/gestion_recursos/operacion_red/smart_wifi/my_synthesis_chile/landing" + "/smartwifi_" + str(nw_yyyyMMdd) + ".csv")
    else:
        dbutils.fs.rm(elemento.path)
