# ==============================================================================
# UNAD - VICERRECTORÍA ACADÉMICA Y DE INVESTIGACIÓN (ECBTI)
# CURSO: BIG DATA - CÓDIGO: 202016911
# TAREA 3: PROCESAMIENTO DE DATOS CON APACHE SPARK
# COMPONENTE: STREAMING EN TIEMPO REAL CON APACHE KAFKA
# ARCHIVO: kafka_producer.py
# DESCRIPCIÓN: Productor Kafka que simula telemetría de sensores IoT en tiempo real
# ==============================================================================

import time
import json
import random
import sys
from kafka import KafkaProducer

def generate_sensor_data():
    """
    Genera un diccionario con lecturas simuladas de sensores IoT:
    - sensor_id: Identificador del sensor (entre 1 y 10)
    - temperature: Temperatura simulada en grados Celsius (20 a 30 °C)
    - humidity: Humedad relativa simulada en porcentaje (30 a 70 %)
    - timestamp: Marca de tiempo UNIX actual (segundos)
    """
    return {
        "sensor_id": random.randint(1, 10),
        "temperature": round(random.uniform(20.0, 30.0), 2),
        "humidity": round(random.uniform(30.0, 70.0), 2),
        "timestamp": int(time.time())
    }

def main():
    topic_name = "sensor_data"
    bootstrap_server = "localhost:9092"

    print("=" * 70)
    print(" INICIANDO PRODUCTOR APACHE KAFKA (Simulador IoT)")
    print(f" Servidor Bootstrap: {bootstrap_server}")
    print(f" Tópico destino:     {topic_name}")
    print(" Presione CTRL + C para detener el envío de datos.")
    print("=" * 70)

    try:
        # Inicialización del cliente productor de Kafka
        producer = KafkaProducer(
            bootstrap_servers=[bootstrap_server],
            value_serializer=lambda x: json.dumps(x).encode('utf-8')
        )
        print("[+] Conexión establecida con el Broker Kafka.")
    except Exception as e:
        print(f"[ERROR] No se pudo conectar al Broker de Kafka en {bootstrap_server}.")
        print(f"Detalle del error: {e}")
        print("Asegúrese de haber iniciado ZooKeeper y Kafka Server previamente.")
        sys.exit(1)

    contador = 0
    try:
        while True:
            contador += 1
            sensor_data = generate_sensor_data()
            # Envío del mensaje al tópico correspondiente
            producer.send(topic_name, value=sensor_data)
            producer.flush()
            print(f"[{contador:04d}] Mensaje enviado a Kafka -> {sensor_data}")
            # Pausa de 1 segundo entre envíos continuos
            time.sleep(1)
    except KeyboardInterrupt:
        print("\n\n[!] Detención solicitada por el usuario (CTRL + C).")
    finally:
        producer.close()
        print("[+] Conexión del productor cerrada de manera segura.")

if __name__ == "__main__":
    main()
