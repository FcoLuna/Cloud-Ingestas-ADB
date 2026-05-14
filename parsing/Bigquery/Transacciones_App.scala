// Databricks notebook source
// MAGIC %python
// MAGIC from google.cloud import bigquery
// MAGIC from google.oauth2 import service_account
// MAGIC import json
// MAGIC from pyspark.sql import SparkSession
// MAGIC
// MAGIC def clean_text(text):
// MAGIC     if isinstance(text, str):
// MAGIC         return text.replace('\n', ' ')
// MAGIC     return text
// MAGIC
// MAGIC # Obtener las credenciales desde un secreto
// MAGIC secret_value = dbutils.secrets.get(scope="secrets-ingestas", key="sec-json-mimovistar-prod")
// MAGIC
// MAGIC # Cargar las credenciales
// MAGIC credentials_dict = json.loads(secret_value)
// MAGIC credentials = service_account.Credentials.from_service_account_info(credentials_dict)
// MAGIC
// MAGIC # Crear el cliente de BigQuery utilizando las credenciales
// MAGIC client = bigquery.Client(credentials=credentials, project=credentials.project_id)
// MAGIC
// MAGIC # Obtener el nombre de la tabla desde ADF
// MAGIC table_name = dbutils.widgets.get("tableName")  # El nombre de la tabla proporcionado por ADF
// MAGIC
// MAGIC # Construir la consulta (usando directamente el parámetro table_name)
// MAGIC query = f"""
// MAGIC SELECT *
// MAGIC FROM (SELECT SAFE_CONVERT_BYTES_TO_STRING(FROM_BASE64(user_id)) AS decode_user_iD,
// MAGIC split(SAFE_CONVERT_BYTES_TO_STRING(FROM_BASE64(user_id)), '*')[OFFSET(0)] AS rut,
// MAGIC SAFE_CAST(SPLIT(SAFE_CONVERT_BYTES_TO_STRING(FROM_BASE64(user_id)), '*')[OFFSET(1)] AS INT64) AS ani_access_id,
// MAGIC event_date,
// MAGIC FORMAT_TIMESTAMP("%Y-%m-%d %H:%M:%S", TIMESTAMP_MICROS(event_timestamp)) AS fecha_formateada,
// MAGIC event_timestamp,
// MAGIC event_name,
// MAGIC (SELECT value.string_value
// MAGIC FROM unnest(event_params)
// MAGIC WHERE KEY = 'eventCategory') AS event_category,
// MAGIC (SELECT value.string_value
// MAGIC FROM unnest(event_params)
// MAGIC WHERE KEY = 'eventAction') AS event_action,
// MAGIC (SELECT value.string_value
// MAGIC FROM unnest(event_params)
// MAGIC WHERE KEY = 'eventLabel') AS event_label,
// MAGIC (SELECT value.string_value
// MAGIC FROM unnest(event_params)
// MAGIC WHERE KEY = 'firebase_screen') AS event_firebase_screen,
// MAGIC (SELECT value.string_value
// MAGIC FROM unnest(event_params)
// MAGIC WHERE KEY = 'segment_app') AS event_segment_app,
// MAGIC (SELECT value.int_value
// MAGIC FROM unnest(event_params)
// MAGIC WHERE KEY = 'ga_session_id') AS event_ga_session_id,
// MAGIC (SELECT value.int_value
// MAGIC FROM unnest(event_params)
// MAGIC WHERE KEY = 'ga_session_number') AS event_ga_session_number,
// MAGIC (SELECT value.int_value
// MAGIC FROM unnest(event_params)
// MAGIC WHERE KEY = 'engagement_time_msec') AS event_engagement_time_msec,
// MAGIC event_previous_timestamp,
// MAGIC event_server_timestamp_offset,
// MAGIC user_id,
// MAGIC user_pseudo_id,
// MAGIC (SELECT value.string_value
// MAGIC FROM unnest(user_properties)
// MAGIC WHERE KEY = 'segment') AS user_segmento,
// MAGIC user_first_touch_timestamp,
// MAGIC privacy_info.analytics_storage,
// MAGIC privacy_info.ads_storage,
// MAGIC privacy_info.uses_transient_token,
// MAGIC device.category,
// MAGIC device.mobile_brand_name,
// MAGIC device.mobile_model_name,
// MAGIC device.mobile_marketing_name,
// MAGIC device.mobile_os_hardware_model,
// MAGIC device.operating_system,
// MAGIC device.operating_system_version,
// MAGIC device.language,
// MAGIC device.is_limited_ad_tracking,
// MAGIC device.time_zone_offset_seconds,
// MAGIC geo.city,
// MAGIC geo.continent,
// MAGIC geo.region,
// MAGIC geo.sub_continent,
// MAGIC app_info.id,
// MAGIC app_info.version,
// MAGIC app_info.firebase_app_id,
// MAGIC app_info.install_source,
// MAGIC traffic_source.name,
// MAGIC traffic_source.medium,
// MAGIC traffic_source.source,
// MAGIC platform,
// MAGIC is_active_user,
// MAGIC CASE
// MAGIC WHEN event_name = 'BOLETAS_EMITIDAS_OPCIONES_DE_BOLETA_OPCI' THEN 'VER BOLETA'
// MAGIC WHEN event_name = 'CONSUMO_GIGAS' THEN 'REVISAR CONSUMO'
// MAGIC WHEN event_name = 'CONSUMO_MINUTOS' THEN 'REVISAR CONSUMO'
// MAGIC WHEN event_name = 'CONSUMO_SMS' THEN 'REVISAR CONSUMO'
// MAGIC WHEN event_name = 'HOME_RESUMEN_INGRESA_AL_CHAT' THEN 'CHAT-BOT'
// MAGIC WHEN event_name = 'AYUDA_ASISTENTE_VIRTUAL' THEN 'CHAT-BOT'
// MAGIC WHEN event_name = 'PERFIL_ACTUALIZAR_DATOS_DE_CONTACTO' THEN 'ACTUALIZA DATOS DE CONTACTO'
// MAGIC WHEN event_name = 'app_remove' THEN 'DESINSTALACION'
// MAGIC WHEN event_name = 'PAGAR_HOME_OPCIONES_DE_PAGO_Y_BOLETA' THEN 'PAGO'
// MAGIC WHEN event_name = 'VER_Y_PAGAR' THEN 'PAGO'
// MAGIC WHEN event_name = 'screen_view' AND
// MAGIC (SELECT VALUE.string_value
// MAGIC FROM UNNEST(event_params)
// MAGIC WHERE KEY = 'firebase_screen') = 'PLAN_PLAN' THEN 'VER DETALLE PLAN'
// MAGIC ELSE 'TIPOLOGIA NO DEFINIDA'
// MAGIC END
// MAGIC AS TIPOLOGIA,
// MAGIC CASE
// MAGIC WHEN event_name IN ('CONTABILIZADOR_USUARIO_LOGIN', 'user_engagement', 'DEUDA_HOME',
// MAGIC 'DEPLIEGUE_OK_DEUDA_HOME', 'NO_DEUDA_HOME', 'HOME_BARRA_LATERAL',
// MAGIC 'DEPLIEGUE_ERRONEO_DEUDA_HOME', 'PERMITIR', 'HOME_RESUMEN_PLAN_MOVIL',
// MAGIC 'ABRIR_APP', 'session_start', 'ERROR_API_REFRESH_TOKEN', 'DESLOGUEO',
// MAGIC 'DEEPLINK_CORRECTO', 'APP_ABIERTA_INGRESAR', 'EVALUARAPP_VOLVER',
// MAGIC 'DESLOGUEO_SIN_RED', 'click', 'reached_7_session', 'APP_ABIERTA_MENU',
// MAGIC 'APP_ABIERTA_ACCESO_ICONO_PERFIL', 'VOLVER_FEEDBACK_VALORACION_APP',
// MAGIC 'notification_receive', 'REFRESH_TOKEN', 'HOME_RESUMEN_MAS_MOVISTAR',
// MAGIC 'ERROR_VOLVER_A_CARGAR_HISTCONSUMOROAMING', 'COMENZAR_REGISTRO_APP',
// MAGIC 'ga_extra_params', 'DEJARCOMENTARIOAPP_NO_GRACIAS', 'NO_PERMITIR',
// MAGIC 'ENVIAR_FEEDBACK_VALORACION_APP_ADICIONAL', 'remove_from_cart',
// MAGIC 'FALLA_MASIVA_SEGUIMIENTO_REPOSICION_HOME', 'VALIDA_RUT_REGISTRO_APP',
// MAGIC 'FINALIZAR_CREA_CONTRASENA_REGISTRO_APP', 'select_item', 'view_item',
// MAGIC 'IR_A_RESUMEN_REGISTRO_APP_EXITOSO', 'purchase', 'begin_checkout',
// MAGIC 'QUIERO_REGISTRARME_REGISTRO_APP', 'SIGUIENTE_PERMISO_IVR_SUPERPONER',
// MAGIC 'view_item_list', 'add_to_cart', 'notification receive', 'OPINIONAPP_NO_GRACIAS',
// MAGIC 'ERROR_3ER_INTENTO_REFRESH_TOKEN', 'ERROR_3ER_INTENTO_REFRESH_TOKEN_SIN_RED',
// MAGIC 'NO_GRACIAS_VALORACION_POR_BOLETA', 'OPINIONAPP_CERRAR', 'APP_ABIERTA_RESUMEN',
// MAGIC 'COMENTAR_FEEDBACK_VALORACION_APP', 'CONTINUAR_INGRESA_RUT_Y_NUM_SERIE',
// MAGIC 'APP_ABIERTA_REGISTRAR', 'screen_view_ns', 'notification_foreground',
// MAGIC 'CONSUMO_ERROR_GRAL', 'VOLVER_INGRESA_RUT_Y_NUM_SERIE', 'HOME_RESUMEN',
// MAGIC 'NO_CONECTADO_CREDENCIALES_NO_DATOS_NI_WI', 'SELECCION_TIPO_PLAN_PARA_TODOS',
// MAGIC 'component_view', 'HOME_MAS_PLAN_HOGAR_MI_APP', 'notification_dismiss',
// MAGIC 'CONTINUAR_INGRESA_NOM_Y_APELLIDO', 'login_exitoso', 'EXPERIENCIAENAPP_CERRAR',
// MAGIC 'CONTINUAR_INGRESA_DATOS_CONTACTO', 'CERRAR_INFO_NUM_SERIE_INGRESA_RUT_Y_NUM_',
// MAGIC 'firebase_campaign') THEN 'ruido'
// MAGIC WHEN event_name LIKE '%LOGIN%' THEN 'ruido'
// MAGIC WHEN event_name LIKE '%VOLVER%' THEN 'ruido'
// MAGIC WHEN event_name LIKE ('CARRUSEL_IMPRESION%') THEN 'ruido'
// MAGIC WHEN event_name LIKE ('LOGIN_BIOMETRICO%') THEN 'ruido'
// MAGIC WHEN event_name LIKE ('%CERRAR_SESI%') THEN 'ruido'
// MAGIC WHEN event_name LIKE ('%TOOLTIP_VALORACION_APP') THEN 'ruido'
// MAGIC WHEN event_name IN ('PERFIL_ACTUALIZAR_DATOS_DE_CONTACTO', 'HOME_MAS_PLAN_HOGAR_PERFIL',
// MAGIC 'PERFIL_ACTUALIZAR_CONTRASENA', 'PERFIL_AGREGAR_SERVICIO_A_CUENTA',
// MAGIC 'HOME_MAS_PREPAGO_PERFIL') THEN 'perfil'
// MAGIC WHEN event_name IN ('app_update', 'os_update') THEN 'actualizacion app'
// MAGIC WHEN event_name IN ('HOME_RESUMEN_CONECTATE_CON_UN_PLAN_HOGAR', 'TIENDA',
// MAGIC 'APP_ABIERTA_TIENDA', 'HOME_RESUMEN_CONECTATE_CON_UN_PLAN_MOVIL') THEN 'tienda'
// MAGIC WHEN event_name IN ('APPOLO', 'APP_ABIERTA_JUEGO') THEN 'appolo'
// MAGIC WHEN event_name IN ('Asistente_virtual', 'HOME_RESUMEN_INGRESA_AL_CHAT', 'AYUDA',
// MAGIC 'AYUDA_ASISTENTE_VIRTUAL', 'APP_ABIERTA_ATENCION',
// MAGIC 'APP_ABIERTA_CHAT', 'IR_AL_CHAT_INSTALACION_Y_REPARACION_DEL_',
// MAGIC 'AYUDA_DEEPLINK_PRUEBA', 'CHAT_SOLICITUD_INST_REP') THEN 'atencion'
// MAGIC WHEN event_name = 'screen_view'
// MAGIC AND (SELECT VALUE.string_value
// MAGIC FROM UNNEST(event_params)
// MAGIC WHERE KEY = 'firebase_screen') = 'AUTOGESTIONA_PROD_HOGAR' THEN 'autogestiona hogar'
// MAGIC WHEN event_name = 'ACCESOS_MAS_USADOS'
// MAGIC AND (SELECT VALUE.string_value
// MAGIC FROM UNNEST(event_params)
// MAGIC WHERE KEY = 'eventLabel') = 'Estado de tus Solicitudes técnicas' THEN 'autogestiona hogar'
// MAGIC WHEN event_name IN ('INSTALACION_Y_REPARACION_DEL_HOGAR_INSTA',
// MAGIC 'REPARA_PROBLEMAS_TECNICOS', 'AUTODIAGNOSTICO_INSTALACIONYREP',
// MAGIC 'APP_ABIERTA_INSTALACION_REPARACION', 'AUTODIAGNOSTICO_INST_REP',
// MAGIC 'OPTIMIZA_WIFI_AUTOGESTIONA_PROD_HOGAR', 'INSTALACION_INST_REP',
// MAGIC 'VER_ESTADO_DEL_SERVICIO_INSTALACION_Y_RE') THEN 'autogestiona hogar'
// MAGIC WHEN event_name IN ('HOME_MAS_PLAN_MOVIL_CONTROL_PLAN', 'HOME_RESUMEN_TU_PLAN',
// MAGIC 'HOME_RESUMEN_VER_DETALLES_TELEVISION', 'HOME_MAS_PREPAGO_TARIFA',
// MAGIC 'HOME_MAS_PREPAGO_TUS_BOLSAS_COMPRADAS', 'HOME_RESUMEN_TU_EQUIPO') THEN 'ver plan'
// MAGIC WHEN event_name = 'screen_view'
// MAGIC AND (SELECT VALUE.string_value
// MAGIC FROM UNNEST(event_params)
// MAGIC WHERE KEY = 'firebase_screen') IN ('PLAN_PLAN') THEN 'ver plan'
// MAGIC WHEN UPPER(event_name) LIKE '%BOLSA%' THEN 'bolsas'
// MAGIC WHEN (SELECT VALUE.string_value
// MAGIC FROM UNNEST(event_params)
// MAGIC WHERE KEY = 'eventAction') LIKE '%BOLSA%' THEN 'bolsas'
// MAGIC WHEN event_name LIKE ('CARDADMIN_%')
// MAGIC AND (SELECT VALUE.string_value
// MAGIC FROM UNNEST(event_params)
// MAGIC WHERE KEY = 'eventCategory') LIKE '%_CLICK' THEN 'cardamin'
// MAGIC WHEN event_name IN ('HOME_BANNER_CARRUSEL', 'APP_ABIERTA_CARRUSEL') THEN 'carrusel'
// MAGIC WHEN event_name IN ('DESCUENTO_CLUB', 'APP_ABIERTA_BENEFICIOS',
// MAGIC 'PERFIL_IR_A_CLUB_MOVISTAR', 'HOME_MAS_PREPAGO_BENEFICIOS',
// MAGIC 'HOME_MAS_PLAN_HOGAR_BENEFICIOS', 'BENEFICIOS',
// MAGIC 'HOME_MAS_PLAN_MOVIL_CONTROL_BENEFICIOS') THEN 'club'
// MAGIC WHEN event_name IN ('HOME_RESUMEN_GIGAS_VER_CONSUMO', 'CONSUMO_GIGAS',
// MAGIC 'HOME_MAS_PLAN_HOGAR_DETALLE_DE_LLAMADAS', 'CONSUMO_SMS',
// MAGIC 'HOME_MAS_PLAN_MOVIL_CONTROL_CONSUMO_Y_TR', 'CONSUMO_MINUTOS',
// MAGIC 'HOME_MAS_PLAN_MOVIL_SALTA_CONSUMO_Y_TRAF',
// MAGIC 'CONSUMO_MINUTOS_VER_DETALLE', 'CONSUMO_VOLVER_A_CARGAR',
// MAGIC 'CONSUMO_SMS_VER_DETALLE',
// MAGIC 'HOME_MAS_PREPAGO_DETALLE_DEL_TRAFICO',
// MAGIC 'HOME_MAS_PREPAGO_HISTORIAL_DE_RECARGAS',
// MAGIC 'CONSUMO_SMS_TODAVIA_NO_HAS_ENVIADO_NINGU',
// MAGIC 'CONSUMO_MINUTOS_TODAVIA_NO_HAS_CONSUMIDO') THEN 'consumo'
// MAGIC WHEN event_name IN ('OPINIONAPP_SI') THEN 'da opinion app'
// MAGIC WHEN event_name IN ('app_remove') THEN 'desinstalacion app'
// MAGIC WHEN event_name IN ('FALLA_MASIVA_MENSAJE_HOME') THEN 'error'
// MAGIC WHEN event_name IN ('EVALUARAPP_PUEDE_MEJORAR', 'PUEDE_MEJORAR',
// MAGIC 'EXPERIENCIAENAPP_PUEDE_MEJORAR') THEN 'valoracion negativa app'
// MAGIC WHEN event_name IN ('EVALUARAPP_ME_ENCANTA', 'ME_ENCANTA') THEN 'valoracion positiva app'
// MAGIC WHEN event_name IN ('app_exception') THEN 'exception app'
// MAGIC WHEN event_name IN ('HOME_RESUMEN_OPCIONES_DE_PAGO_Y_BOLETA', 'ACTUALIZAR_DEUDA',
// MAGIC 'OPCIONES_DE_BOLETA_OPCIONES_DE_PAGO_Y_BO',
// MAGIC 'BOLETAS_EMITIDAS_OPCIONES_DE_BOLETA_OPCI',
// MAGIC 'Modal_Opciones_Boleta_y_Pago', 'HISTORIAL_DE_PAGOS_VER_BOLETA',
// MAGIC 'OPCIONES_DE_PAGO_Y_BOLETA_HISTORIAL_DE_P',
// MAGIC 'SUSCRIPCIONES_DE_PAGO_OPCIONES_DE_PAGO_Y',
// MAGIC 'HISTORIAL_DE_PAGOS_VER_HISTORICO_DE_PAGO',
// MAGIC 'HOME_MAS_PLAN_HOGAR_MIS_BOLETAS',
// MAGIC 'PRORROGA_OPCIONES_DE_BOLETA_OPCIONES_DE_',
// MAGIC 'HISTORIAL_DE_PAGOS_CAMBIATE_A_BOLETA_ECO',
// MAGIC 'PRORROGA_ACTIVA_OPCIONES_DE_BOLETA_OPCIO',
// MAGIC 'MOVISTAR_PAY_SUSCRIPCIONES_DE_PAGO_OPCIO',
// MAGIC 'ONE_CLICK_SUSCRIPCIONES_DE_PAGO_OPCIONES',
// MAGIC 'HOME_MAS_PLAN_MOVIL_CONTROL_MIS_BOLETAS') THEN 'facturacion'
// MAGIC WHEN event_name IN ('HOME_MAS_PLAN_HOGAR_CONTRATAR_SERVICIOS', 'PERFIL_MOVISTAR_TV_DETALLE')
// MAGIC THEN 'hogar servicios adicionales'
// MAGIC WHEN event_name IN ('MI_BUZON', 'NOTIFICACION_PUSH', 'APP_ABIERTA_NOTIFICACIONES')
// MAGIC THEN 'notificaciones'
// MAGIC WHEN event_name IN ('PAGAR_HOME_OPCIONES_DE_PAGO_Y_BOLETA', 'APP_ABIERTA_PAGAR',
// MAGIC 'VER_Y_PAGAR', 'APP_ABIERTA_MENU_MAS_PAGAR') THEN 'pagar'
// MAGIC WHEN event_name IN ('RECAMBIO_EXPRESS_DERIVACION',
// MAGIC 'HOME_MAS_PLAN_MOVIL_CONTROL_EQUIPO') THEN 'renovar equipo'
// MAGIC WHEN event_name IN ('RECARGA_HOME', 'APP_ABIERTA_RECARGA',
// MAGIC 'HOME_MAS_PLAN_MOVIL_CONTROL_RECARGAR_SAL',
// MAGIC 'HOME_MAS_PREPAGO_RECARGAR_SALDO') THEN 'recarga'
// MAGIC WHEN event_name LIKE ('%ROAMING%') THEN 'roaming'
// MAGIC WHEN event_name IN ('SERVICIOS_DIGITALES_MIS_COMPRAS_TUS_SUSC',
// MAGIC 'MENU_ENTRETENCION_SSDD_TIENDA',
// MAGIC 'MENU_GAMING_SSDD_TIENDA',
// MAGIC 'MENU_SEGURIDAD_SSDD_TIENDA',
// MAGIC 'MENU_ASISTENCIA_SSDD_TIENDA',
// MAGIC 'MENU_TODOS_SSDD_TIENDA',
// MAGIC 'MENU_APRENDIZAJE_SSDD_TIENDA',
// MAGIC 'SERVICIOS_DIGITALES_MIS_COMPRAS_TU_HISTO') THEN 'servicios digitales'
// MAGIC WHEN event_name = 'HOME_BARRA_INFERIOR'
// MAGIC AND (SELECT VALUE.string_value
// MAGIC FROM UNNEST(event_params)
// MAGIC WHERE KEY = 'eventLabel') = 'Tienda' THEN 'tienda'
// MAGIC WHEN event_name = 'HOME_BARRA_INFERIOR'
// MAGIC AND (SELECT VALUE.string_value
// MAGIC FROM UNNEST(event_params)
// MAGIC WHERE KEY = 'eventLabel') = 'Atención' THEN 'atencion'
// MAGIC WHEN event_name = 'HOME_BARRA_INFERIOR'
// MAGIC AND (SELECT VALUE.string_value
// MAGIC FROM UNNEST(event_params)
// MAGIC WHERE KEY = 'eventLabel') = 'Club' THEN 'club'
// MAGIC WHEN event_name = 'HOME_BARRA_INFERIOR'
// MAGIC AND (SELECT VALUE.string_value
// MAGIC FROM UNNEST(event_params)
// MAGIC WHERE KEY = 'eventLabel') = 'Notificaciones' THEN 'notificaciones'
// MAGIC WHEN event_name = 'ACCESOS_MAS_USADOS'
// MAGIC AND (SELECT VALUE.string_value
// MAGIC FROM UNNEST(event_params)
// MAGIC WHERE KEY = 'eventLabel') = 'Tus servicios adicionales' THEN 'hogar servicios adicionales'
// MAGIC WHEN event_name = 'ACCESOS_MAS_USADOS'
// MAGIC AND (SELECT VALUE.string_value
// MAGIC FROM UNNEST(event_params)
// MAGIC WHERE KEY = 'eventLabel') = 'Autogestiona tus productos hogar' THEN 'autogestiona hogar'
// MAGIC WHEN event_name = 'ACCESOS_MAS_USADOS'
// MAGIC AND (SELECT VALUE.string_value
// MAGIC FROM UNNEST(event_params)
// MAGIC WHERE KEY = 'eventLabel') = 'Revisa Tus Bolsas' THEN 'bolsas'
// MAGIC WHEN event_name = 'ACCESOS_MAS_USADOS'
// MAGIC AND (SELECT VALUE.string_value
// MAGIC FROM UNNEST(event_params)
// MAGIC WHERE KEY = 'eventLabel') = 'Estado de tus Solicitudes técnicas' THEN 'autogestiona hogar'
// MAGIC WHEN event_name = 'ACCESOS_MAS_USADOS'
// MAGIC AND (SELECT VALUE.string_value
// MAGIC FROM UNNEST(event_params)
// MAGIC WHERE KEY = 'eventLabel') = 'Revisa tu Tarifa' THEN 'ver plan'
// MAGIC WHEN event_name = 'ACCESOS_MAS_USADOS'
// MAGIC AND (SELECT VALUE.string_value
// MAGIC FROM UNNEST(event_params)
// MAGIC WHERE KEY = 'eventLabel') = 'Revisa tu Historial de Recargas' THEN 'consumo'
// MAGIC WHEN event_name = 'ACCESOS_MAS_USADOS'
// MAGIC AND (SELECT VALUE.string_value
// MAGIC FROM UNNEST(event_params)
// MAGIC WHERE KEY = 'eventLabel') = 'Salta de Prepago a Plan' THEN 'tienda'
// MAGIC WHEN event_name = 'screen_view' THEN 'ruido'
// MAGIC WHEN event_name = 'HOME_BARRA_INFERIOR' THEN 'ruido'
// MAGIC WHEN event_name = 'ACCESOS_MAS_USADOS' THEN 'ruido'
// MAGIC WHEN event_name LIKE 'CARDADMIN_%' THEN 'ruido'
// MAGIC ELSE 'otros'
// MAGIC END AS tipologia_analytics
// MAGIC FROM `mimovistar-prod.analytics_182807761.{table_name}`
// MAGIC WHERE LEFT(user_id, 2) <> 'U2')
// MAGIC WHERE (TIPOLOGIA <> 'TIPOLOGIA NO DEFINIDA')
// MAGIC OR (tipologia_analytics NOT IN ('ruido', 'otros'))
// MAGIC """
// MAGIC
// MAGIC # Ejecutar la consulta en BigQuery
// MAGIC query_job = client.query(query)
// MAGIC
// MAGIC # Convertir el resultado en un DataFrame de Pandas
// MAGIC result_df = query_job.to_dataframe()
// MAGIC
// MAGIC # Limpia santos de lineas
// MAGIC result_df = result_df.applymap(clean_text)
// MAGIC
// MAGIC # Convertir el DataFrame de Pandas a un DataFrame de PySpark
// MAGIC spark = SparkSession.builder.appName("BigQueryToAzure").getOrCreate()
// MAGIC spark_df = spark.createDataFrame(result_df)
// MAGIC
// MAGIC # Crear el widget
// MAGIC dbutils.widgets.text("output_save", "", "Output Save")
// MAGIC output_save = dbutils.widgets.get("output_save")
// MAGIC
// MAGIC # Guardar el DataFrame en ADL
// MAGIC spark_df.repartition(1).write \
// MAGIC     .option("header", "true") \
// MAGIC     .option("delimiter", ",") \
// MAGIC     .mode("append") \
// MAGIC     .csv(output_save)
// MAGIC

// COMMAND ----------

//Actualizar el nombre del archivo

import java.text.SimpleDateFormat
import java.util.Date

// Obtener la fecha actual
val dateFormat = new SimpleDateFormat("yyyyMMdd")
val dateStr = dateFormat.format(new Date())

// Crear el nombre del archivo con la fecha
val tableName = dbutils.widgets.get("tableName")

// Agregar '.csv' al final del nombre de la tabla
val tableNameWithExtension = s"${tableName}.csv"

var output_rename = dbutils.widgets.get("output_rename")

// Listar los archivos en el directorio y filtrar el archivo que comienza con "part-"
val files = dbutils.fs.ls(output_rename)
val oldFilePathOption = files.find(file => file.name.startsWith("part-"))

// Renombrar archivo
oldFilePathOption match {
  case Some(oldFile) =>
    val oldFilePath = oldFile.path
    val newFilePath = output_rename + tableNameWithExtension
    dbutils.fs.mv(oldFilePath, newFilePath)
    println(s"El archivo ha sido renombrado a: $tableNameWithExtension")

  case None =>
    println("No se encontró ningún archivo que cumpla con el criterio.")
}
println(tableNameWithExtension)
