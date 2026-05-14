# Databricks notebook source
# MAGIC %md
# MAGIC **Envío de correos**
# MAGIC
# MAGIC Este notebook se utiliza para el envío de correos de notificación. Para su correcto funcionamiento se debe entregar los parámetros:
# MAGIC
# MAGIC - servidor smtp
# MAGIC - puerto smtp
# MAGIC - email del remitente
# MAGIC - listado de emails destinatarios
# MAGIC - asunto del mail
# MAGIC - cuerpo del mail

# COMMAND ----------

import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

# COMMAND ----------

dbutils.widgets.text('smtp_server', '10.232.220.142')
dbutils.widgets.text('smtp_port', '25')
dbutils.widgets.text('from_email', 'info_ingestas_bigdata@telefonica.com')
dbutils.widgets.text('to_email', 'operaciondeldato.cl@telefonica.com')
dbutils.widgets.text('subject', '[OK] Proceso []')
dbutils.widgets.text('body', 'Este es el cuerpo del correo.')

# COMMAND ----------

# Configuración del servidor SMTP
smtp_server = dbutils.widgets.get("smtp_server") #'10.232.220.142'  # Reemplaza con la IP del servidor SMTP
smtp_port = dbutils.widgets.get("smtp_port") #25  # Puerto 25 para SMTP sin cifrado
 
# Datos del correo
from_email = dbutils.widgets.get('from_email')
to_email = dbutils.widgets.get('to_email') #para destinatarios múltiples separar correos con  punto y coma ';'
subject = dbutils.widgets.get('subject')
body = dbutils.widgets.get('body')

to_email_list = [email.strip() for email in to_email.split(';')] 
to_email_distinct_list = list(dict.fromkeys(to_email_list))

# Crear el mensaje
msg = MIMEMultipart()
msg['From'] = from_email
msg['To'] = ','.join(to_email_distinct_list)
msg['Subject'] = subject
msg.attach(MIMEText(body, 'html','utf-8'))

# Enviar el correo
try:
    server = smtplib.SMTP(smtp_server, smtp_port)
    text = msg.as_string()
    server.sendmail(from_email, to_email_distinct_list, text)
    server.quit()
    print("Correo enviado exitosamente!")
except Exception as e:
    print(f"Error al enviar el correo: {e}")

# COMMAND ----------

dbutils.notebook.exit("Done")
