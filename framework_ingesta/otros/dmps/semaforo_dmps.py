# Databricks notebook source
# Obtener la ruta base desde el widget
ruta_base = dbutils.widgets.get("ruta_completa")  # Ruta de ADF

# Nombre del archivo
nombre_archivo = "dmps_semaforo_ok.txt"

# Construir la ruta completa del archivo
ruta_archivo = f"{ruta_base.rstrip('/')}/{nombre_archivo}"

# Contenido del archivo
contenido = "Estado: OK\n"

# Escribir el archivo en la ruta especificada
dbutils.fs.put(ruta_archivo, contenido, overwrite=True)

print(f"Archivo creado exitosamente en: {ruta_archivo}")
