# Databricks notebook source
from pyspark.sql.functions import col, round, year, month, dayofmonth, hour
import logging

# Configurar logging básico para información más detallada
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logging.info("Iniciando notebook de la capa Silver: Ecommerce_Silver_TransformAndEnrich")

# COMMAND ----------

storage_account_name = "ecotomdatalake"
secret_scope_name = "ecommerce-keyvault-scope"

logging.info("Intentando obtener secretos y configurar ADLS para Silver...")
try:
    client_id = dbutils.secrets.get(scope=secret_scope_name, key='client-id')
    client_secret = dbutils.secrets.get(scope=secret_scope_name, key='client-secret')
    tenant_id = dbutils.secrets.get(scope=secret_scope_name, key='tenant-id2')

    configs = {
        "fs.azure.account.auth.type": "OAuth",
        "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
        "fs.azure.account.oauth2.client.id": client_id,
        "fs.azure.account.oauth2.client.secret": client_secret,
        "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"
    }
    logging.info("✅ Secretos y configuraciones de ADLS obtenidas exitosamente.")

except Exception as e:
    logging.error(f"❌ ERROR: No se pudieron obtener los secretos o configurar las credenciales. Detalles: {e}")
    logging.error("Asegúrate de que 'secret_scope_name' y las claves ('client-id', 'client-secret', 'tenant-id2') sean correctas y accesibles.")
    raise # Relanzar la excepción para detener la ejecución si falla


# COMMAND ----------

# --- Montaje de Silver ---
container_name_silver = "silver"
mount_point_silver = f"/mnt/{container_name_silver}"

# Desmonta si ya está montado, o monta si no lo está
if any(mount.mountPoint == mount_point_silver for mount in dbutils.fs.mounts()):
    logging.info(f"Desmontando {mount_point_silver} existente...")
    dbutils.fs.unmount(mount_point_silver)
logging.info(f"Montando el contenedor '{container_name_silver}' en {mount_point_silver}...")
dbutils.fs.mount(
    source = f"abfss://{container_name_silver}@{storage_account_name}.dfs.core.windows.net/",
    mount_point = mount_point_silver,
    extra_configs = configs
)
logging.info(f"✅ Montaje de '{container_name_silver}' completado.")

logging.info("Proceso de montaje de contenedores de Silver finalizado.")

# COMMAND ----------

# NOTA: Asegúrate que la tabla bronze_ecommerce.retail_data ya fue creada por el notebook de Bronze.
logging.info("Leyendo datos de la capa BRONZE desde la tabla Delta 'bronze_ecommerce.retail_data'.")

try:
    df_silver_raw = spark.table("bronze_ecommerce.retail_data") # LEEMOS DE LA TABLA DELTA BRONZE
    logging.info("✅ DataFrame 'df_silver_raw' cargado exitosamente desde la capa Bronze.")
    print("\nEsquema del DataFrame leído de Bronze:")
    df_silver_raw.printSchema()
    print("\nPrimeras 5 filas del DataFrame leído de Bronze:")
    df_silver_raw.show(5, truncate=False)
except Exception as e:
    logging.error(f"❌ ERROR al cargar el DataFrame desde la capa Bronze: {e}")
    raise # Relanzar la excepción para detener la ejecución si falla

# COMMAND ----------

logging.info("Iniciando enriquecimiento y transformación del DataFrame para la capa Silver...")

# --- Calcular el LineTotal (Cantidad * Precio Unitario) ---
# Esta transformación se hace en Silver ya que es un enriquecimiento directo de la transacción
df_silver_enriched = df_silver_raw.withColumn(
    "LineTotal",
    round(col("Quantity") * col("UnitPrice"), 2) # Redondeamos a 2 decimales para moneda
)
logging.info("✅ 'LineTotal' calculado para la capa Silver.")

# --- Extraer Componentes de Tiempo de InvoiceDate ---
# Estos componentes también son enriquecimientos directos de la transacción
df_silver_enriched = df_silver_enriched.withColumn("InvoiceYear", year(col("InvoiceDate"))) \
                                        .withColumn("InvoiceMonth", month(col("InvoiceDate"))) \
                                        .withColumn("InvoiceDay", dayofmonth(col("InvoiceDate"))) \
                                        .withColumn("InvoiceHour", hour(col("InvoiceDate")))
logging.info("✅ Componentes de tiempo (Año, Mes, Día, Hora) extraídos de 'InvoiceDate' para la capa Silver.")

# --- Aquí podrías añadir más transformaciones específicas de Silver: ---
# - Uniones con tablas de dimensión (ej. clientes, productos)
# - Estandarización de nombres o categorías
# - Creación de flags o indicadores
# - Limpieza de datos más profunda o compleja si se justifica aquí

logging.info("Enriquecimiento y transformación del DataFrame para la capa Silver finalizado.")
print("\nEsquema del DataFrame enriquecido para Silver:")
df_silver_enriched.printSchema()
print("\nPrimeras 5 filas del DataFrame enriquecido para Silver:")
df_silver_enriched.show(5, truncate=False)

# COMMAND ----------

spark.sql("CREATE DATABASE IF NOT EXISTS silver_ecommerce")

# Define la ruta donde se almacenará la tabla Delta en Silver
silver_delta_table_path = f"{mount_point_silver}/transactions_silver_delta"

logging.info(f"Iniciando escritura del DataFrame enriquecido en la capa Silver como tabla Delta en: {silver_delta_table_path}")

try:
    (df_silver_enriched.write
        .format("delta")
        .mode("overwrite") # 'overwrite' reemplazará la tabla cada vez que se ejecute
        .option("path", silver_delta_table_path) # Ruta física en ADLS Gen2
        .saveAsTable("silver_ecommerce.transactions_silver") # Registra la tabla en el metastore
    )
    logging.info(f"✅ DataFrame enriquecido guardado como tabla Delta 'silver_ecommerce.transactions_silver' en: {silver_delta_table_path}")
except Exception as e:
    logging.error(f"❌ ERROR al escribir el DataFrame en la capa Silver: {e}")
    raise

# --- Verificación (Opcional) ---
print("\n--- ✅ Verificando la tabla Delta de Silver recién creada: ---")
spark.sql("DESCRIBE DETAIL silver_ecommerce.transactions_silver").display()
print("-" * 50)