# Databricks notebook source
import os
import requests

# COMMAND ----------

# MAGIC %sh
# MAGIC
# MAGIC # Define la fecha deseada
# MAGIC fecha_inicio="2024-12-01"
# MAGIC fecha_fin="2024-12-01"
# MAGIC
# MAGIC # Realiza la solicitud POST utilizando curl
# MAGIC curl -k -H "Content-Type: application/json" \
# MAGIC      -d '{"customerId" : 1283, "exportId" : 2979, "customerKey" : "b58c5151-9fdf-440a-99b6-3301db7efce1", "periodStart" : "'${fecha_inicio}'", "periodEnd" : "'${fecha_fin}'"}' \
# MAGIC      -X POST https://ws.nperf.com/cloudV1/getFile
# MAGIC
# MAGIC
# MAGIC echo $url

# COMMAND ----------

# MAGIC %sh
# MAGIC export PYTHONIOENCODING=utf8
# MAGIC fecha="2024-01-01"
# MAGIC url=$(curl -H "Content-Type: application/json" -d '{"customerId" : 1283, "exportId" : 2979, "customerKey" : "b58c5151-9fdf-440a-99b6-3301db7efce1", "periodStart" : "'${fecha}'", "periodEnd" : "'${fecha}'"}' -X POST https://ws.nperf.com/cloudV1/getFile | python3 -c "import sys, json; print json.load(sys.stdin)['FileURL']")
# MAGIC
# MAGIC curl "$url" > tele-cl_${fecha}.txt
# MAGIC

# COMMAND ----------

import requests
import json
import subprocess
import os

# Configurar la codificación
os.environ['PYTHONIOENCODING'] = 'utf8'

# Fecha de ejemplo (puedes reemplazarla con el valor que necesites)
fecha = '2024-11-01'  # Reemplaza con la fecha deseada

# Datos para la solicitud POST
data = {
    "customerId": 1283,
    "exportId": 2979,
    "customerKey": "b58c5151-9fdf-440a-99b6-3301db7efce1",
    "periodStart": fecha,
    "periodEnd": fecha
}

# Realizar la solicitud POST
response = requests.post('https://ws.nperf.com/cloudV1/getFile', headers={'Content-Type': 'application/json'}, data=json.dumps(data))

print(response.text)

# COMMAND ----------

#SHELL ORIGINAL
export PYTHONIOENCODING=utf8
fecha=$1
url=$(curl -H "Content-Type: application/json" -d '{"customerId" : 1283, "exportId" : 2979, "customerKey" : "b58c5151-9fdf-440a-99b6-3301db7efce1", "periodStart" : "'${fecha}'", "periodEnd" : "'${fecha}'"}' -X POST https://ws.nperf.com/cloudV1/getFile | python2 -c "import sys, json; print json.load(sys.stdin)['FileURL']")
curl "$url" > tele-cl_${fecha}.csv.bz2
bzip2 -d tele-cl_*
sed 's/\t/~/g' tele-cl_*.csv > /home/TCHILE.LOCAL/srv_ingepr/nperf/nperf_${fecha}.csv
rm -r tele-cl_*

# COMMAND ----------

!python --version

# COMMAND ----------

# MAGIC %sh
# MAGIC
# MAGIC export PYTHONIOENCODING=utf8
# MAGIC fecha="2024-11-01"
# MAGIC url=$(curl -H "Content-Type: application/json" -d '{"customerId" : 1283, "exportId" : 2979, "customerKey" : "b58c5151-9fdf-440a-99b6-3301db7efce1", "periodStart" : "'${fecha}'", "periodEnd" : "'${fecha}'"}' -X POST https://ws.nperf.com/cloudV1/getFile | python2 -c "import sys, json; print json.load(sys.stdin)['FileURL']")
# MAGIC curl "$url" > tele-cl_${fecha}.csv.bz2
# MAGIC bzip2 -d tele-cl_*
# MAGIC sed 's/\t/~/g' tele-cl_*.csv > /home/TCHILE.LOCAL/srv_ingepr/nperf/nperf_${fecha}.csv
# MAGIC rm -r tele-cl_*

# COMMAND ----------

# MAGIC %sh
# MAGIC export PYTHONIOENCODING=utf8
# MAGIC fecha="2024-12-14"
# MAGIC
# MAGIC # Realiza la solicitud y guarda la respuesta en una variable
# MAGIC response=$(curl -H "Content-Type: application/json" -d "{\"customerId\" : 1283, \"exportId\" : 2979, \"customerKey\" : \"b58c5151-9fdf-440a-99b6-3301db7efce1\", \"periodStart\" : \"${fecha}\", \"periodEnd\" : \"${fecha}\"}" -X POST https://ws.nperf.com/cloudV1/getFile)
# MAGIC
# MAGIC # Imprime la respuesta para depuración
# MAGIC echo "Response: $response"
# MAGIC
# MAGIC # Intenta extraer la URL del JSON si la respuesta es válida
# MAGIC url=$(echo "$response" | python3 -c "import sys, json; print(json.load(sys.stdin).get('FileURL', ''))")
# MAGIC
# MAGIC # Imprime la URL para depuración
# MAGIC echo "URL: $url"
# MAGIC
# MAGIC # Si la URL no está vacía, procede con la descarga y procesamiento del archivo
# MAGIC if [ -n "$url" ]; then
# MAGIC   curl "$url" > tele-cl_${fecha}.csv.bz2
# MAGIC   bzip2 -d tele-cl_*
# MAGIC   mkdir -p /home/TCHILE.LOCAL/srv_ingepr/nperf/
# MAGIC   sed 's/\t/~/g' tele-cl_*.csv > /home/TCHILE.LOCAL/srv_ingepr/nperf/nperf_${fecha}.csv
# MAGIC   rm -r tele-cl_*
# MAGIC else
# MAGIC   echo "Error: No se pudo obtener la URL del archivo."
# MAGIC fi
# MAGIC

# COMMAND ----------

# MAGIC %sh
# MAGIC export PYTHONIOENCODING=utf8
# MAGIC fecha="2024-12-15"
# MAGIC
# MAGIC # Realiza la solicitud y guarda la respuesta en una variable
# MAGIC response=$(curl -H "Content-Type: application/json" -d "{\"customerId\" : 1283, \"exportId\" : 2979, \"customerKey\" : \"b58c5151-9fdf-440a-99b6-3301db7efce1\", \"periodStart\" : \"${fecha}\", \"periodEnd\" : \"${fecha}\"}" -X POST https://ws.nperf.com/cloudV1/getFile)
# MAGIC
# MAGIC # Imprime la respuesta para depuración
# MAGIC echo "Response: $response"
# MAGIC
# MAGIC # Intenta extraer la URL del JSON si la respuesta es válida
# MAGIC url=$(echo "$response" | python3 -c "import sys, json; 
# MAGIC try: 
# MAGIC     print(json.load(sys.stdin).get('FileURL', '')) 
# MAGIC except json.JSONDecodeError: 
# MAGIC     print('')")
# MAGIC
# MAGIC # Imprime la URL para depuración
# MAGIC echo "URL: $url"
# MAGIC
# MAGIC # Si la URL no está vacía, procede con la descarga y procesamiento del archivo
# MAGIC if [ -n "$url" ]; then
# MAGIC   curl "$url" > tele-cl_${fecha}.csv.bz2
# MAGIC   bzip2 -d tele-cl_*
# MAGIC   mkdir -p /home/TCHILE.LOCAL/srv_ingepr/nperf/
# MAGIC   sed 's/\t/~/g' tele-cl_*.csv > /home/TCHILE.LOCAL/srv_ingepr/nperf/nperf_${fecha}.csv
# MAGIC   rm -r tele-cl_*
# MAGIC else
# MAGIC   echo "Error: No se pudo obtener la URL del archivo."
# MAGIC fi

# COMMAND ----------

# MAGIC %sh
# MAGIC export PYTHONIOENCODING=utf8
# MAGIC fecha=2024-12-13
# MAGIC
# MAGIC # Realiza la solicitud y guarda la respuesta en una variable
# MAGIC response=$(curl -H "Content-Type: application/json" -d "{\"customerId\" : 1283, \"exportId\" : 2979, \"customerKey\" : \"b58c5151-9fdf-440a-99b6-3301db7efce1\", \"periodStart\" : \"${fecha}\", \"periodEnd\" : \"${fecha}\"}" -X POST https://ws.nperf.com/cloudV1/getFile)
# MAGIC
# MAGIC # Imprime la respuesta para depuración
# MAGIC echo "Response: $response"
# MAGIC
# MAGIC # Intenta extraer la URL del JSON si la respuesta es válida
# MAGIC url=$(echo "$response" | python3 -c "import sys, json; 
# MAGIC try: 
# MAGIC     print(json.load(sys.stdin).get('FileURL', '')) 
# MAGIC except json.JSONDecodeError: 
# MAGIC     print('')")
# MAGIC
# MAGIC # Imprime la URL para depuración
# MAGIC echo "URL: $url"
# MAGIC
# MAGIC # Si la URL no está vacía, procede con la descarga y procesamiento del archivo
# MAGIC if [ -n "$url" ]; then
# MAGIC   curl "$url" > tele-cl_${fecha}.csv.bz2
# MAGIC   bzip2 -d tele-cl_*
# MAGIC   mkdir -p /home/TCHILE.LOCAL/srv_ingepr/nperf/
# MAGIC   sed 's/\t/~/g' tele-cl_*.csv > /home/TCHILE.LOCAL/srv_ingepr/nperf/nperf_${fecha}.csv
# MAGIC   rm -r tele-cl_*
# MAGIC else
# MAGIC   echo "Error: No se pudo obtener la URL del archivo."
# MAGIC fi
# MAGIC
