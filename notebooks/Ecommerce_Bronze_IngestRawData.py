# Databricks notebook source
dbutils.secrets.listScopes()

# COMMAND ----------

from pyspark.sql.functions import to_timestamp, col, sum, lit
storage_account_name = "ecotomdatalake"
secret_scope_name = "ecommerce-keyvault-scope"

try:
    client_id = dbutils.secrets.get(scope=secret_scope_name, key='client-id')
    client_secret = dbutils.secrets.get(scope=secret_scope_name, key='client-secret')
    tenant_id = dbutils.secrets.get(scope=secret_scope_name, key='tenant-id2')

    configs = {"fs.azure.account.auth.type": "OAuth",
               "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
               "fs.azure.account.oauth2.client.id": client_id,
               "fs.azure.account.oauth2.client.secret": client_secret,
               "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}

    # Define el nombre del contenedor (file system) que quieres montar
    container_name = "bronze" 
    
    # Define el punto de montaje en DBFS
    mount_point = f"/mnt/{container_name}"

    # Desmonta si ya está montado (para evitar errores si ejecutas varias veces)
    if any(mount.mountPoint == mount_point for mount in dbutils.fs.mounts()):
        print(f"Desmontando {mount_point} existente...")
        dbutils.fs.unmount(mount_point)

    print(f"Montando abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/ en {mount_point}...")

    dbutils.fs.mount(
        source = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/",
        mount_point = mount_point,
        extra_configs = configs
    )

    print(f"✅ Montaje completado en: {mount_point}")

    # Ahora puedes leer el archivo usando la ruta montada
    mounted_file_path = f"{mount_point}/transactions/data.csv"
    df_bronze = spark.read.option("header", True).option("inferSchema", True).csv(mounted_file_path)
    
    print("\n✅ Esquema del DataFrame 'df_bronze' cargado desde el punto de montaje:")
    df_bronze.printSchema()
    print("\n✅ Primeras filas del DataFrame 'df_bronze':")
    df_bronze.display()

except Exception as e:
    print(f"❌ ERROR en el montaje o lectura: {e}")
    raise

# COMMAND ----------



# COMMAND ----------

# --- 4. Limpieza y Transformación del DataFrame (Capa Bronze) ---

# Paso 4.1: Conversión de tipo de dato para InvoiceDate
# Convierte la columna InvoiceDate a formato Timestamp.
# Las fechas con formato inválido se convertirán a NULL en esta etapa.
df_bronze_stg1 = df_bronze.withColumn("InvoiceDate", to_timestamp(col("InvoiceDate"), "M/d/yyyy H:mm"))
print("\n✅ 'InvoiceDate' convertido a Timestamp.")

# Conteo de nulos después de la conversión de fecha (para capturar nulos introducidos)
print("\n✅ Conteo de nulos después de conversión de fecha:")
df_bronze_stg1.select([sum(col(c).isNull().cast("integer")).alias(c) for c in df_bronze_stg1.columns]).show()

# Paso 4.2: Manejo de nulos en columnas críticas (eliminación de filas)
# Estas columnas son esenciales para la validez de la transacción.
# Las filas con nulos aquí se consideran inválidas y se eliminan.
critical_cols_for_nulls = ["InvoiceNo", "StockCode", "Quantity", "InvoiceDate", "UnitPrice"]
initial_rows_count = df_bronze_stg1.count()
df_bronze_cleaned = df_bronze_stg1.dropna(subset=critical_cols_for_nulls)
print(f"✅ Filas eliminadas por nulos en columnas críticas: {initial_rows_count - df_bronze_cleaned.count()} (incluye fechas inválidas).")

# Mostrar filas eliminadas por nulos en columnas críticas
print("\n--- 🔍 Filas con Nulos en Columnas Críticas Eliminadas: ---")
df_null_rows_removed = df_bronze_stg1.filter(
    (col("InvoiceNo").isNull()) |
    (col("StockCode").isNull()) |
    (col("Quantity").isNull()) |
    (col("InvoiceDate").isNull()) |
    (col("UnitPrice").isNull())
)
df_null_rows_removed.show(truncate=False)
print("-" * 50)

# Paso 4.3: Manejo de nulos en columnas opcionales (rellenar)
# Rellena nulos en CustomerID con -1 para "cliente desconocido".
# Rellena Description y Country con "UNKNOWN" si faltan.
df_bronze_cleaned = df_bronze_cleaned.na.fill({
    "CustomerID": -1,
    "Description": "UNKNOWN",
    "Country": "UNKNOWN"
})

# Conteo de nulos después de la limpieza general
print("\n✅ Conteo de nulos después de limpieza general:")
df_bronze_cleaned.select([sum(col(c).isNull().cast("integer")).alias(c) for c in df_bronze_cleaned.columns]).show()

# Paso 4.4: Eliminación de duplicados
# Elimina filas completamente duplicadas para evitar inflar métricas.
initial_rows_dedup = df_bronze_cleaned.count()
df_bronze_cleaned_before_dedup = df_bronze_cleaned # Guardar para inspección
df_bronze_cleaned = df_bronze_cleaned.drop_duplicates()
print(f"✅ Filas duplicadas eliminadas: {initial_rows_dedup - df_bronze_cleaned.count()}.")

# Mostrar filas duplicadas eliminadas
print("\n--- 🔍 Ejemplos de Filas Duplicadas Eliminadas (mostrará al menos una de las copias): ---")
df_bronze_cleaned_before_dedup.exceptAll(df_bronze_cleaned).show(truncate=False)
print("-" * 50)

# Paso 4.5: Filtrar cantidades o precios unitarios inválidos (<= 0)
# Elimina transacciones con cantidades o precios que no tienen sentido para una venta.
initial_rows_filtered = df_bronze_cleaned.count()
df_bronze_cleaned_before_invalid_filter = df_bronze_cleaned # Guardar para inspección
df_bronze_cleaned = df_bronze_cleaned.filter((col("Quantity") > 0) & (col("UnitPrice") > 0))
print(f"✅ Filas con Quantity/UnitPrice inválido eliminadas: {initial_rows_filtered - df_bronze_cleaned.count()}.")

# Mostrar filas con Quantity/UnitPrice inválido
print("\n--- 🔍 Filas con Quantity/UnitPrice Inválido Eliminadas: ---")
df_invalid_qty_price_removed = df_bronze_cleaned_before_invalid_filter.filter(
    (col("Quantity") <= 0) | (col("UnitPrice") <= 0)
)
df_invalid_qty_price_removed.show(truncate=False)
print("-" * 50)


# --- 5. Muestra del DataFrame Limpio Final ---
print("\n--- ✅ DataFrame Limpio (df_bronze_cleaned) ---")
df_bronze_cleaned.printSchema()
df_bronze_cleaned.show(5, truncate=False)

# --- 6. Guardar el DataFrame Limpio como Tabla Delta en la Capa Bronze ---

# Define la ruta donde se almacenará la tabla Delta en ADLS Gen2
# Esto estará dentro de tu punto de montaje /mnt/bronze
bronze_delta_table_location = f"{mount_point}/retail_data_delta" # Usa un nombre de carpeta específico para la tabla Delta

# Crea la base de datos si no existe (importante para organizar tus tablas)
spark.sql("CREATE DATABASE IF NOT EXISTS bronze_ecommerce")

# Escribe el DataFrame limpio en formato Delta y registra la tabla
(df_bronze_cleaned.write
    .format("delta")
    .mode("overwrite") # 'overwrite' reemplazará la tabla cada vez que se ejecute. Para incremental, sería 'append'.
    .option("path", bronze_delta_table_location) # Especifica la ubicación física en ADLS Gen2
    .saveAsTable("bronze_ecommerce.retail_data") # Registra la tabla en el metastore de Databricks
)

print(f"✅ DataFrame limpio guardado como tabla Delta en: {bronze_delta_table_location}")
print("✅ Tabla Delta 'bronze_ecommerce.retail_data' registrada en el metastore.")

# --- 7. Verificar la existencia de la tabla Delta (Opcional, pero recomendado) ---
print("\n--- ✅ Verificando la tabla Delta recién creada: ---")
spark.sql("DESCRIBE DETAIL bronze_ecommerce.retail_data").display()
print("-" * 50)

# COMMAND ----------

# --- Montaje del Contenedor 'silver' en Databricks File System (DBFS) ---
# Asegúrate de que las variables client_id, client_secret, tenant_id
# ya estén definidas en una celda anterior (ej. la celda donde obtienes los secretos).

print("Intentando montar el contenedor 'silver'...")

container_name_silver = "silver" # El nombre de tu contenedor 'silver' en ADLS Gen2
mount_point_silver = f"/mnt/{container_name_silver}"

# Configuración de OAuth2 con Service Principal para el montaje de 'silver'
configs_silver = {
    "fs.azure.account.auth.type": "OAuth",
    "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
    "fs.azure.account.oauth2.client.id": client_id,
    "fs.azure.account.oauth2.client.secret": client_secret,
    "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"
}

# Desmonta si ya está montado (para asegurar una operación limpia)
if any(mount.mountPoint == mount_point_silver for mount in dbutils.fs.mounts()):
    print(f"Desmontando {mount_point_silver} existente...")
    dbutils.fs.unmount(mount_point_silver)

# Realiza el montaje del contenedor 'silver'
dbutils.fs.mount(
    source = f"abfss://{container_name_silver}@{storage_account_name}.dfs.core.windows.net/",
    mount_point = mount_point_silver,
    extra_configs = configs_silver
)
print(f"✅ Montaje del contenedor 'silver' completado en: {mount_point_silver}")


silver_path = f"/mnt/silver/transactions"

# Escribe el DataFrame limpio en formato Parquet
# Usamos mode("overwrite") para reemplazar los datos existentes.
# Opciones comunes: "append" (añadir), "ignore" (no hacer nada si ya existe), "errorifexists" (lanzar error si ya existe)
df_bronze_cleaned.write \
  .mode("overwrite") \
  .format("parquet") \
  .save(silver_path)

print(f"✅ DataFrame limpio guardado en la capa Silver como Parquet en: {silver_path}")
