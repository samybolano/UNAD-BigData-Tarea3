# ==============================================================================
# UNAD - VICERRECTORÍA ACADÉMICA Y DE INVESTIGACIÓN (ECBTI)
# CURSO: BIG DATA - CÓDIGO: 202016911
# TAREA 3: PROCESAMIENTO DE DATOS CON APACHE SPARK
# COMPONENTE: STREAMING EN TIEMPO REAL CON SPARK STREAMING & KAFKA
# ARCHIVO: spark_streaming_consumer.py
# DESCRIPCIÓN: Consumidor en tiempo real con ventanas de cálculo en streaming
#              (Implementación oficial basada en el Anexo 3 de la UNAD)
# ==============================================================================

import sys
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window
from pyspark.sql.types import StructType, StructField, IntegerType, FloatType, TimestampType

def main():
    print("=" * 80)
    print(" INICIANDO CONSUMIDOR SPARK STRUCTURED STREAMING")
    print(" Integración Apache Kafka -> Apache Spark Streaming (Anexo 3)")
    print("=" * 80)

    # 1. Crear e inicializar la sesión de Spark
    spark = SparkSession.builder \
        .appName("SensorDataAnalysis") \
        .getOrCreate()

    # Configurar el nivel de log a WARN para reducir los mensajes INFO en pantalla
    spark.sparkContext.setLogLevel("WARN")

    # 2. Definir el esquema de los datos de entrada
    schema = StructType([
        StructField("sensor_id", IntegerType(), True),
        StructField("temperature", FloatType(), True),
        StructField("humidity", FloatType(), True),
        StructField("timestamp", TimestampType(), True)
    ])

    topic_name = "sensor_data"
    bootstrap_servers = "localhost:9092"

    print(f"\n[+] Suscribiéndose al topic de Kafka: '{topic_name}' ({bootstrap_servers})")

    # 3. Configurar el lector de streaming para leer desde Apache Kafka
    df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", bootstrap_servers) \
        .option("subscribe", topic_name) \
        .load()

    # 4. Parsear los datos JSON provenientes del broker Kafka
    parsed_df = df.select(
        from_json(col("value").cast("string"), schema).alias("data")
    ).select("data.*")

    # 5. Calcular estadísticas agregadas por ventana de tiempo (1 minuto) y por sensor
    windowed_stats = parsed_df \
        .groupBy(window(col("timestamp"), "1 minute"), "sensor_id") \
        .agg({"temperature": "avg", "humidity": "avg"})

    # 6. Escribir los resultados continuos en la consola
    print("\n[+] Escuchando eventos en tiempo real. Mostrando agregaciones en consola...")
    print("    -> Para finalizar la ejecución, presione CTRL + C.\n")

    query = windowed_stats \
        .writeStream \
        .outputMode("complete") \
        .format("console") \
        .option("truncate", "false") \
        .start()

    try:
        query.awaitTermination()
    except KeyboardInterrupt:
        print("\n[!] Detención del consumidor solicitada por el usuario.")
    finally:
        query.stop()
        spark.stop()
        print("[+] Sesión de Spark Streaming finalizada exitosamente.")

if __name__ == "__main__":
    main()
