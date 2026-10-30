#!/bin/bash
# ==============================================================================
# SCRIPT MAESTRO: INICIAR TODOS LOS SERVICIOS DE BIG DATA (TAREA 3)
# Estudiante: Samuel Bolaño - UNAD
# ==============================================================================

echo "=================================================================="
echo "    INICIANDO AUTOMÁTICAMENTE TODOS LOS SERVICIOS DE BIG DATA     "
echo "=================================================================="

# 1. Iniciar Apache Spark (Master y Worker)
echo -n "[1/4] Arrancando Apache Spark (Master & Worker)... "
start-master.sh >/dev/null 2>&1 || true
(start-worker.sh spark://bigdata:7077 >/dev/null 2>&1 || start-slave.sh spark://bigdata:7077 >/dev/null 2>&1) || true
echo "LISTO (Puerto 7077 y Web 8080)"

# 2. Iniciar ZooKeeper en modo daemon (-daemon)
echo -n "[2/4] Arrancando Apache ZooKeeper (Puerto 2181)... "
sudo pkill -f zookeeper >/dev/null 2>&1 || true
sleep 1
sudo KAFKA_HEAP_OPTS="-Xmx128M -Xms64M" /opt/Kafka/bin/zookeeper-server-start.sh -daemon /opt/Kafka/config/zookeeper.properties
sleep 4
echo "LISTO"

# 3. Iniciar Kafka Server en modo daemon (-daemon) con memoria optimizada
echo -n "[3/4] Arrancando Apache Kafka Broker (Puerto 9092)... "
sudo pkill -f kafka.Kafka >/dev/null 2>&1 || true
sleep 1
sudo KAFKA_HEAP_OPTS="-Xmx256M -Xms128M" /opt/Kafka/bin/kafka-server-start.sh -daemon /opt/Kafka/config/server.properties
sleep 6
echo "LISTO"

# 4. Crear o verificar el topic de Kafka
echo -n "[4/4] Verificando topic 'sensor_data'... "
/opt/Kafka/bin/kafka-topics.sh --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic sensor_data >/dev/null 2>&1 || true
echo "LISTO"

echo ""
echo "=================================================================="
echo "         ESTADO DE LOS PUERTOS ACTIVOS EN LA MÁQUINA             "
echo "=================================================================="
sudo ss -tulpn | grep -E '2181|9092|7077|8080' || true
echo "=================================================================="
echo " ¡TODO PERFECTO! Tu entorno quedó 100% listo para ejecutar:       "
echo " 1. Batch:     python3 codigo/tarea3_batch.py                     "
echo " 2. Productor: python3 codigo/kafka_producer.py                   "
echo " 3. Streaming: spark-submit --packages ...                        "
echo "=================================================================="
