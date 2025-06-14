# Databricks notebook source
# MAGIC %pip uninstall -y azure-eventhub
# MAGIC %pip uninstall -y typing_extensions
# MAGIC %pip install typing_extensions==3.10.0.2
# MAGIC %pip install azure-eventhub

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

import os
import pandas as pd
from azure.eventhub import EventHubProducerClient, EventData
import json

# --- CONFIGURACIÓN ---
EVENTHUB_CONNECTION_STR = "Endpoint=sb://<your-namespace>.servicebus.windows.net/;SharedAccessKeyName=MyPolicy;SharedAccessKey=...;EntityPath=ecommerce-transactions" # <--- ¡PEGA AQUÍ TU CADENA!
EVENTHUB_NAME = "ecommerce-transactions" 

# --- CARGAR DATOS ---
df_data_to_send = spark.table("bronze_ecommerce.retail_data").toPandas() 
print(f"✅ Datos cargados: {len(df_data_to_send)} filas.")

# --- ENVIAR DATOS ---
try:
    producer = EventHubProducerClient.from_connection_string(conn_str=EVENTHUB_CONNECTION_STR, eventhub_name=EVENTHUB_NAME)
    print(f"Iniciando envío a '{EVENTHUB_NAME}'...")
    with producer:
        for index, row in df_data_to_send.iterrows():
            event_data_batch = producer.create_batch()
            event_data_batch.add(EventData(json.dumps(row.to_dict())))
            producer.send_batch(event_data_batch)
            if index % 1000 == 0: print(f"  Enviados {index + 1} eventos...")
    print("✅ Envío completado.")
except Exception as e:
    print(f"❌ Error al enviar: {e}")