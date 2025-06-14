# Databricks notebook source
from pyspark.sql.functions import col, sum, countDistinct, dayofweek, dayofmonth, hour, when, avg, collect_list, array_distinct, size, array_contains, explode, count
import logging

# Configurar logging básico para información más detallada
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logging.info("Iniciando notebook de la capa Gold: Ecommerce_Gold_AggregateAndPublish")

# COMMAND ----------

storage_account_name = "ecotomdatalake"
secret_scope_name = "ecommerce-keyvault-scope"

logging.info("Intentando obtener secretos y configurar ADLS para Gold...")
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
    raise

# COMMAND ----------

# DBTITLE 1,Untitled October 23, 2023 12:00 PM
logging.info("Iniciando proceso de montaje de contenedores para Gold...")

# --- Montaje de Gold ---
container_name_gold = "gold"
mount_point_gold = f"/mnt/{container_name_gold}"

# Desmonta si ya está montado, o monta si no lo está
if any(mount.mountPoint == mount_point_gold for mount in dbutils.fs.mounts()):
    logging.info(f"Desmontando {mount_point_gold} existente...")
    dbutils.fs.unmount(mount_point_gold)
logging.info(f"Montando el contenedor '{container_name_gold}' en {mount_point_gold}...")
dbutils.fs.mount(
    source = f"abfss://{container_name_gold}@{storage_account_name}.dfs.core.windows.net/",
    mount_point = mount_point_gold,
    extra_configs = configs
)
logging.info(f"✅ Montaje de '{container_name_gold}' completado.")

logging.info("Proceso de montaje de contenedores de Gold finalizado.")


# COMMAND ----------

# NOTA: Asegúrate que la tabla silver_ecommerce.transactions_silver ya fue creada por el notebook de Silver.
logging.info("Leyendo datos de la capa SILVER desde la tabla Delta 'silver_ecommerce.transactions_silver'.")

try:
    # LEEMOS DE LA TABLA DELTA SILVER
    df_gold_raw = spark.table("silver_ecommerce.transactions_silver")
    logging.info("✅ DataFrame 'df_gold_raw' cargado exitosamente desde la capa Silver.")
    print("\nEsquema del DataFrame leído de Silver:")
    df_gold_raw.printSchema()
    print("\nPrimeras 5 filas del DataFrame leído de Silver:")
    df_gold_raw.show(5, truncate=False)
except Exception as e:
    logging.error(f"❌ ERROR al cargar el DataFrame desde la capa Silver: {e}")
    raise # Relanzar la excepción para detener la ejecución si falla

# COMMAND ----------

logging.info("Iniciando cálculo de agregaciones para la capa Gold...")

# --- Agregación 1: Ventas Diarias por País ---
# Usamos df_gold_raw que es el DataFrame leído de Silver
df_daily_sales_by_country = df_gold_raw.groupBy("InvoiceDate", "Country") \
                                         .agg(
                                             sum("LineTotal").alias("TotalSales"),
                                             sum("Quantity").alias("TotalQuantitySold"),
                                             countDistinct("InvoiceNo").alias("TotalOrders")
                                         ) \
                                         .orderBy("InvoiceDate", "Country")
logging.info("✅ 'Ventas Diarias por País' calculado.")
print("\n--- Ejemplo: Ventas Diarias por País (df_daily_sales_by_country) ---")
df_daily_sales_by_country.printSchema()
df_daily_sales_by_country.show(5, truncate=False)

# --- Agregación 2: Productos Más Vendidos (por cantidad y valor) ---
# Usamos df_gold_raw que es el DataFrame leído de Silver
df_top_products = df_gold_raw.groupBy("StockCode", "Description") \
                               .agg(
                                   sum("Quantity").alias("TotalQuantitySold"),
                                   sum("LineTotal").alias("TotalRevenue"),
                                   countDistinct("InvoiceNo").alias("NumOrders")
                               ) \
                               .orderBy(col("TotalQuantitySold").desc(), col("TotalRevenue").desc())
logging.info("✅ 'Productos Más Vendidos' calculado.")
print("\n--- Ejemplo: Productos Más Vendidos (df_top_products) ---")
df_top_products.printSchema()
df_top_products.show(5, truncate=False)

logging.info("Cálculo de agregaciones finalizado.")

# COMMAND ----------

logging.info("Calculando Patrones de Compra por Hora y Día de la Semana...")

# Mapeo de números de día de la semana a nombres (Spark default: Lunes=2, Domingo=1)
# Ajustamos para que Lunes sea 1 o usamos un mapping más intuitivo
df_patterns = df_gold_raw.withColumn("DayOfWeekNum", dayofweek(col("InvoiceDate"))) \
                         .withColumn("DayOfWeekName",
                             when(col("DayOfWeekNum") == 1, "Sunday")
                             .when(col("DayOfWeekNum") == 2, "Monday")
                             .when(col("DayOfWeekNum") == 3, "Tuesday")
                             .when(col("DayOfWeekNum") == 4, "Wednesday")
                             .when(col("DayOfWeekNum") == 5, "Thursday")
                             .when(col("DayOfWeekNum") == 6, "Friday")
                             .when(col("DayOfWeekNum") == 7, "Saturday")
                             .otherwise("Unknown")
                         )

df_purchase_patterns = df_patterns.groupBy("InvoiceHour", "DayOfWeekName", "DayOfWeekNum") \
                                  .agg(
                                      countDistinct("InvoiceNo").alias("TotalOrders"),
                                      sum("LineTotal").alias("TotalSales"),
                                      sum("Quantity").alias("TotalQuantitySold")
                                  ) \
                                  .orderBy("DayOfWeekNum", "InvoiceHour")

logging.info("✅ Patrones de Compra por Hora/Día de la Semana calculados.")
print("\n--- Ejemplo: Patrones de Compra por Hora/Día de la Semana (df_purchase_patterns) ---")
df_purchase_patterns.printSchema()
df_purchase_patterns.show(10, truncate=False)


# COMMAND ----------

logging.info("Calculando Análisis Básico de Cestas de Mercado (MBA)...")

# Paso 1: Recopilar todos los StockCode por cada InvoiceNo
df_basket_items = df_gold_raw.groupBy("InvoiceNo") \
                             .agg(collect_list("StockCode").alias("ItemsInBasket"))

# Paso 2: Filtrar cestas con al menos 2 ítems únicos para MBA (si solo hay 1, no hay combinaciones)
df_valid_baskets = df_basket_items.withColumn("UniqueItemsInBasket", array_distinct(col("ItemsInBasket"))) \
                                  .filter(size(col("UniqueItemsInBasket")) >= 2)

# Paso 3: Generar todas las combinaciones de pares de ítems dentro de cada cesta
# Esto es una simplificación; un MBA completo requiere más lógica para asociaciones.
# Aquí identificamos simplemente qué ítems aparecen juntos.
df_item_pairs = df_valid_baskets.withColumn("item1", explode(col("UniqueItemsInBasket"))) \
                                .withColumn("item2", explode(col("UniqueItemsInBasket"))) \
                                .filter(col("item1") != col("item2")) # Evitar pares del mismo ítem

# Para asegurar que ItemA, ItemB es lo mismo que ItemB, ItemA y evitar duplicados en la agregación
df_item_pairs_cleaned = df_item_pairs.filter(col("item1") < col("item2")) \
                                     .select(col("item1"), col("item2")) \
                                     .distinct() # Asegurarse de que cada par único se cuente una vez por InvoiceNo

# Paso 4: Contar la co-ocurrencia de cada par de ítems
# Esto requiere un join de auto-referencia o una lógica más compleja para realmente contar co-ocurrencias
# Una forma más sencilla para esta demostración es:
# Iterar sobre los ítems para contar sus apariciones conjuntas.
# Para el propósito de una "demostración básica de MBA", a menudo se muestra la co-ocurrencia de los pares de productos más frecuentes.
# Vamos a contar cuántas veces cada par de ítems aparece en una misma factura (InvoiceNo)
# Esto implica volver a las facturas y contar cuántas veces un par (item1, item2) ocurre.

df_co_occurrence = df_valid_baskets.alias("basket") \
    .join(df_gold_raw.alias("itemA"), col("basket.InvoiceNo") == col("itemA.InvoiceNo")) \
    .join(df_gold_raw.alias("itemB"), col("basket.InvoiceNo") == col("itemB.InvoiceNo")) \
    .filter(
        (col("itemA.StockCode") < col("itemB.StockCode")) & # Evitar duplicados (A,B vs B,A) y el mismo ítem
        (col("itemA.InvoiceNo") == col("itemB.InvoiceNo"))
    ) \
    .groupBy(col("itemA.StockCode").alias("Product1Code"), col("itemB.StockCode").alias("Product2Code")) \
    .agg(countDistinct("itemA.InvoiceNo").alias("CoOccurrenceCount")) \
    .orderBy(col("CoOccurrenceCount").desc())

logging.info("✅ Análisis Básico de Cestas de Mercado (MBA) calculado.")
print("\n--- Ejemplo: Pares de Productos Co-ocurrentes (df_co_occurrence) ---")
df_co_occurrence.printSchema()
df_co_occurrence.show(10, truncate=False)

# COMMAND ----------

logging.info("Iniciando escritura de DataFrames en la capa Gold...")

# Crear la base de datos si no existe (se mantiene)
spark.sql("CREATE DATABASE IF NOT EXISTS gold_ecommerce")

# --- Escribe la tabla de ventas diarias por país (como Delta - se mantiene) ---
daily_sales_delta_path = f"{mount_point_gold}/daily_sales_by_country_delta"
try:
    (df_daily_sales_by_country.write
        .format("delta")
        .mode("overwrite")
        .option("path", daily_sales_delta_path)
        .saveAsTable("gold_ecommerce.daily_sales_by_country")
    )
    logging.info(f"✅ Tabla Delta 'gold_ecommerce.daily_sales_by_country' guardada en: {daily_sales_delta_path}")
except Exception as e:
    logging.error(f"❌ ERROR al escribir gold_ecommerce.daily_sales_by_country: {e}")
    raise

# --- Escribe la tabla de productos más vendidos (como Delta - se mantiene) ---
top_products_delta_path = f"{mount_point_gold}/top_products_delta"
try:
    (df_top_products.write
        .format("delta")
        .mode("overwrite")
        .option("path", top_products_delta_path)
        .saveAsTable("gold_ecommerce.top_products")
    )
    logging.info(f"✅ Tabla Delta 'gold_ecommerce.top_products' guardada en: {top_products_delta_path}")
except Exception as e:
    logging.error(f"❌ ERROR al escribir gold_ecommerce.top_products: {e}")
    raise

# --- NUEVA: Escribe la tabla de patrones de compra por hora/día (como Delta) ---
purchase_patterns_delta_path = f"{mount_point_gold}/purchase_patterns_delta"
try:
    (df_purchase_patterns.write
        .format("delta")
        .mode("overwrite")
        .option("path", purchase_patterns_delta_path)
        .saveAsTable("gold_ecommerce.purchase_patterns")
    )
    logging.info(f"✅ Tabla Delta 'gold_ecommerce.purchase_patterns' guardada en: {purchase_patterns_delta_path}")
except Exception as e:
    logging.error(f"❌ ERROR al escribir gold_ecommerce.purchase_patterns: {e}")
    raise

# --- NUEVA: Escribe la tabla de co-ocurrencia de productos (como Delta) ---
co_occurrence_delta_path = f"{mount_point_gold}/product_co_occurrence_delta"
try:
    (df_co_occurrence.write
        .format("delta")
        .mode("overwrite")
        .option("path", co_occurrence_delta_path)
        .saveAsTable("gold_ecommerce.product_co_occurrence")
    )
    logging.info(f"✅ Tabla Delta 'gold_ecommerce.product_co_occurrence' guardada en: {co_occurrence_delta_path}")
except Exception as e:
    logging.error(f"❌ ERROR al escribir gold_ecommerce.product_co_occurrence: {e}")
    raise

logging.info("Escritura de DataFrames en la capa Gold finalizada.")

# COMMAND ----------

# Celda 9: Verificación de Tablas Delta en la Capa Gold (Opcional - Se añaden las nuevas verificaciones)

logging.info("Iniciando verificación de tablas Delta en la capa Gold...")

print("\n--- ✅ Detalle de la tabla 'gold_ecommerce.daily_sales_by_country': ---")
spark.sql("DESCRIBE DETAIL gold_ecommerce.daily_sales_by_country").display()

print("\n--- ✅ Detalle de la tabla 'gold_ecommerce.top_products': ---")
spark.sql("DESCRIBE DETAIL gold_ecommerce.top_products").display()

print("\n--- ✅ Detalle de la tabla 'gold_ecommerce.purchase_patterns': ---")
spark.sql("DESCRIBE DETAIL gold_ecommerce.purchase_patterns").display()

print("\n--- ✅ Detalle de la tabla 'gold_ecommerce.product_co_occurrence': ---")
spark.sql("DESCRIBE DETAIL gold_ecommerce.product_co_occurrence").display()

logging.info("Verificación de tablas Delta en la capa Gold finalizada.")

# COMMAND ----------

num_rows_co_occurrence = df_co_occurrence.count()
print(f"La tabla 'product_co_occurrence' tiene {num_rows_co_occurrence} filas.")

# COMMAND ----------

# MAGIC %md
# MAGIC