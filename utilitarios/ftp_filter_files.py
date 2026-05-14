# Databricks notebook source
# MAGIC %md
# MAGIC ### Notebook de filtro para archivos en FTP
# MAGIC 2025-01-21 add by js. Se genera lógica para obtener los archivos a descargar según la información devuelva de last_ingest_time.txt y de FTP

# COMMAND ----------

import json
import datetime
import re
import pytz

# COMMAND ----------

dbutils.widgets.text('offset_time',"")

files_ftp_to_filter     = dbutils.widgets.get("files_ftp_to_filter")
last_ingest_time_str    = dbutils.widgets.get("last_ingest_time")
dateFormatFile          = dbutils.widgets.get("dateFormatFile")
offset_time             = dbutils.widgets.get("offset_time")

# COMMAND ----------

replace_format = [("yyyy","%Y"), ("YYYY","%Y"), ("MMMMM","%B"), ("MMM","%b"), ("MM","%m"), ("dd","%d"), ("hh","%I"), ("HH","%H"), ("mm","%M"), ("ss","%S")]

for i in replace_format:
    dateFormatFile = dateFormatFile.replace(i[0], i[1])

# COMMAND ----------

archivos = json.loads("[" + files_ftp_to_filter + "]")

# COMMAND ----------

q_archivos = len(archivos)
print(f"Archivos por filtrar desde FTP: {q_archivos}")

# COMMAND ----------

# Convertimos el valor de last_ingest
last_ingest_time_str = re.sub(r'([.][0-9]+)','',last_ingest_time_str)
last_ingest_time = datetime.datetime.strptime(last_ingest_time_str, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=datetime.timezone.utc)
last_ingest_time.isoformat()

# COMMAND ----------

# creación de Offset
# Diccionario para mapear unidades de tiempo
time_units = {"seconds": 0, "minutes": 0, "hours": 0, "days": 0}

# Identificar la unidad de tiempo y extraer el valor numérico
for unit in time_units:
    if offset_time.endswith(unit):
        time_units[unit] = int(re.sub(r'\D', '', offset_time))
        break  # Detenemos el bucle cuando encontramos la coincidencia

# Crear el timedelta con los valores obtenidos
print(time_units)
offset = datetime.timedelta(**time_units)
print(offset)

# COMMAND ----------

local = pytz.timezone("America/Santiago")

#OBS: se agrega el offset a la fecha de creacion del archivo en vez de la fecha creada artificialmente en base al nombre

result_exit = {}
lista_archivos = []
for archivo in archivos:
    fecha_str = re.sub(r'[^0-9]', '', archivo["name"]) # se dejan solo los numeros, que corresponden a la fecha en formato dateFileFormat
    fecha = datetime.datetime.strptime(fecha_str, dateFormatFile)
    # Excepción para manejar cambios de horario
    # Ejemplo error anterior: AmbiguousTimeError: 2025-04-05 23:00:00
    try:
        local_dt = local.localize(fecha, is_dst=None)
    except Exception as e:
        print(e)
    utc_dt = local_dt.astimezone(pytz.utc)
    if utc_dt > last_ingest_time + offset:
        lista_archivos.append(archivo)

# COMMAND ----------

result_exit["archivos_a_descargar"] = lista_archivos

# COMMAND ----------

dbutils.notebook.exit(result_exit)
