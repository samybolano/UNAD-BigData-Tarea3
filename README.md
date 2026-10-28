# Tarea 3: Procesamiento de Datos con Apache Spark y Apache Kafka

**Curso:** Big Data (Código: 202016911)  
**Programa:** Ingeniería de Sistemas  
**Universidad Nacional Abierta y a Distancia (UNAD)**  
**Autor:** Samuel Bolaño  
**Repositorio Oficial:** https://github.com/samybolano/UNAD-BigData-Tarea3  
**Periodo:** 2026 II - Periodo 16-04  

---

## 📋 Descripción del Proyecto

Este repositorio contiene la solución completa para la **Tarea 3 - Procesamiento de Datos con Apache Spark**, abordando:
1. **Procesamiento por Lotes (Batch Processing):** Pipeline analítico en PySpark implementando limpieza, transformaciones y análisis exploratorio de datos (EDA) tanto con **DataFrames** como con **RDDs** sobre un dataset masivo de la Tasa Representativa del Mercado (TRM) almacenado en Hadoop HDFS.
2. **Procesamiento en Tiempo Real (Stream Processing):** Ingesta continua de eventos simulados de telemetría IoT mediante un productor en **Apache Kafka** y consumo/agregación en tiempo real por ventanas temporales fijas de 1 minuto usando **Spark Structured Streaming**.

---

## 🏗️ Arquitectura de la Solución

```
+-------------------------------------------------------------------+
|                        FUENTES DE DATOS                           |
|  1. Dataset Histórico TRM (CSV)       2. Sensores IoT Simulados    |
+-----------------------------------+-------------------------------+
                                    |                               |
                                    v                               v
                       +-------------------------+     +-------------------------+
                       |       HADOOP HDFS       |     |      APACHE KAFKA       |
                       | /Tarea3/rows.csv        |     | Topic: sensor_data      |
                       +------------+------------+     +------------+------------+
                                    |                               |
                                    v                               v
                       +-------------------------+     +-------------------------+
                       |   PySpark Batch Engine  |     | Spark Structured Stream |
                       | (DataFrames & RDD DAG)  |     | (Ventanas de 1 minuto)  |
                       +------------+------------+     +------------+------------+
                                    |                               |
                                    v                               v
                       +-------------------------+     +-------------------------+
                       | Resumen Estadístico EDA |     | Salida Continua Consola |
                       | (Consola / Métricas)    |     | (Micro-batches Spark)   |
                       +-------------------------+     +-------------------------+
                                    \                              /
                                     v                            v
                                  +----------------------------------+
                                  |   Spark Web UI (Puerto 4040)     |
                                  +----------------------------------+
```

---

## 📁 Estructura del Repositorio

```
├── codigo/
│   ├── tarea3_batch.py             # Script de procesamiento Batch (PySpark DataFrames + RDDs)
│   ├── kafka_producer.py           # Productor Kafka que emite telemetría de sensores IoT
│   └── spark_streaming_consumer.py # Consumidor Spark Streaming con agregaciones por ventana
├── rows.csv                        # Dataset de prueba (Tasa Representativa del Mercado - Colombia)
└── README.md                       # Documentación técnica e instrucciones de ejecución
```

---

## ⚙️ Requisitos del Entorno

* Sistema Operativo: Ubuntu 22.04 LTS (o máquina virtual VirtualBox)
* Java JDK: OpenJDK 11
* Hadoop: Apache Hadoop 3.3.6 (HDFS activo)
* Spark: Apache Spark 3.5.x
* Kafka: Apache Kafka 3.9.x (con ZooKeeper)
* Python: Python 3.10+
* Librerías Python:
  ```bash
  pip install pyspark kafka-python
  ```

---

## 🚀 Instrucciones de Ejecución

### 1. Iniciar el Clúster Hadoop y Cargar el Dataset
```bash
# Cambiar al usuario hadoop
su - hadoop
start-all.sh

# Crear directorio en HDFS y subir archivo
hdfs dfs -mkdir /Tarea3
wget https://www.datos.gov.co/api/views/mcec-87by/rows.csv
hdfs dfs -put rows.csv /Tarea3
hdfs dfs -ls /Tarea3
exit
```

### 2. Ejecutar el Procesamiento en Batch
```bash
# Iniciar servicios de Spark
start-master.sh
start-worker.sh spark://bigdata:7077

# Ejecutar el script batch
python3 tarea3_batch.py
```

### 3. Ejecutar el Flujo en Tiempo Real (Kafka & Spark Streaming)

**Paso A: Iniciar ZooKeeper y Kafka Server**
```bash
sudo /opt/Kafka/bin/zookeeper-server-start.sh /opt/Kafka/config/zookeeper.properties &
sudo /opt/Kafka/bin/kafka-server-start.sh /opt/Kafka/config/server.properties &

# Crear el topic (si no existe)
/opt/Kafka/bin/kafka-topics.sh --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic sensor_data
```

**Paso B: Terminal 1 - Ejecutar el Productor Kafka**
```bash
python3 kafka_producer.py
```

**Paso C: Terminal 2 - Ejecutar el Consumidor Spark Streaming**
```bash
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3 spark_streaming_consumer.py
```

---

## 📊 Resultados Destacados

* **Análisis Batch:**
  * Procesamiento de más de 7,800 observaciones históricas de la TRM.
  * Identificación del máximo histórico (\$5,061.21 COP) y jornadas con cotizaciones > \$5,000 COP.
  * Cálculo de agregaciones anuales y transformaciones con RDDs (`map`, `filter`, `reduceByKey`).
* **Análisis Streaming:**
  * Ingesta de eventos JSON segundo a segundo sin pérdida de mensajes.
  * Consolidación de micro-batches con promedios de temperatura y humedad en ventanas de 60 segundos por sensor.
  * Supervisión activa de DAGs y tareas en Spark Web UI (`http://<ip-servidor>:4040`).

---

## 📜 Licencia
Proyecto académico bajo fines educativos para la Universidad Nacional Abierta y a Distancia (UNAD).
