# ==============================================================================
# UNAD - VICERRECTORÍA ACADÉMICA Y DE INVESTIGACIÓN (ECBTI)
# CURSO: BIG DATA - CÓDIGO: 202016911
# TAREA 3: PROCESAMIENTO DE DATOS CON APACHE SPARK
# COMPONENTE: PROCESAMIENTO EN BATCH (SPARK / PYSPARK)
# ARCHIVO: tarea3_batch.py
# DESCRIPCIÓN: Carga, limpieza, transformación, EDA y análisis con DataFrames y RDDs
# ==============================================================================

import sys
import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, DateType

def main():
    print("=" * 80)
    print(" INICIO DE PROCESAMIENTO EN BATCH CON APACHE SPARK")
    print(" Curso: Big Data (UNAD) - Tarea 3")
    print("=" * 80)

    # --------------------------------------------------------------------------
    # 1. INICIALIZACIÓN DE LA SESIÓN DE SPARK (SparkSession)
    # --------------------------------------------------------------------------
    spark = SparkSession.builder \
        .appName("Tarea3_ProcesamientoBatch_TRM") \
        .getOrCreate()

    # Reducir verbosidad de logs de Spark a WARN para mantener la salida legible
    spark.sparkContext.setLogLevel("WARN")

    print("\n[+] Sesión de Spark iniciada exitosamente.")
    print(f"    - Versión de Spark: {spark.version}")
    print(f"    - Master configurado: {spark.sparkContext.master}")

    # --------------------------------------------------------------------------
    # 2. DEFINICIÓN DE RUTA Y CARGA DEL DATASET
    # --------------------------------------------------------------------------
    # Intentamos primero la ruta oficial en HDFS (entorno de la Máquina Virtual)
    # Si no se encuentra disponible (ej. ejecución local), se usa el archivo local.
    hdfs_path = "hdfs://localhost:9000/Tarea3/rows.csv"
    local_path = "rows.csv"
    alt_local_path = "../rows.csv"

    dataset_path = hdfs_path
    try:
        print(f"\n[+] Intentando cargar dataset desde HDFS: {hdfs_path}")
        df_raw = spark.read.format("csv") \
            .option("header", "true") \
            .option("inferSchema", "true") \
            .load(hdfs_path)
        print(f"    -> Cargado exitosamente desde HDFS.")
    except Exception as e:
        print(f"    [!] No se pudo conectar a HDFS ({e}). Conmutando a modo local...")
        if os.path.exists(local_path):
            dataset_path = local_path
        elif os.path.exists(alt_local_path):
            dataset_path = alt_local_path
        else:
            dataset_path = "rows.csv"
        
        print(f"[+] Cargando dataset desde archivo local: {dataset_path}")
        df_raw = spark.read.format("csv") \
            .option("header", "true") \
            .option("inferSchema", "true") \
            .load(dataset_path)

    # --------------------------------------------------------------------------
    # 3. INSPECCIÓN INICIAL DEL ESQUEMA Y PRIMEROS REGISTROS
    # --------------------------------------------------------------------------
    print("\n" + "=" * 50)
    print(" 3. ESQUEMA ORIGINAL DEL DATASET")
    print("=" * 50)
    df_raw.printSchema()

    total_registros = df_raw.count()
    print(f"\n[+] Total de registros cargados en crudo: {total_registros:,}")

    print("\n[+] Primeros 10 registros del dataset:")
    df_raw.show(10, truncate=False)

    # --------------------------------------------------------------------------
    # 4. LIMPIEZA Y TRANSFORMACIÓN DE DATOS (DataFrames)
    # --------------------------------------------------------------------------
    print("\n" + "=" * 50)
    print(" 4. LIMPIEZA Y TRANSFORMACIÓN DE TIPOS DE DATOS")
    print("=" * 50)

    # Se realiza:
    # a) Casteo de columna VALOR a DoubleType
    # b) Conversión de fechas en formato dd/MM/yyyy a DateType
    # c) Extracción de Año, Mes y Día para análisis temporales
    # d) Filtrado de posibles valores nulos o inválidos
    df_clean = df_raw \
        .withColumn("VALOR", F.col("VALOR").cast(DoubleType())) \
        .withColumn("FECHA_DESDE", F.to_date(F.col("VIGENCIADESDE"), "dd/MM/yyyy")) \
        .withColumn("FECHA_HASTA", F.to_date(F.col("VIGENCIAHASTA"), "dd/MM/yyyy")) \
        .filter(F.col("VALOR").isNotNull() & F.col("FECHA_DESDE").isNotNull()) \
        .withColumn("ANIO", F.year(F.col("FECHA_DESDE"))) \
        .withColumn("MES", F.month(F.col("FECHA_DESDE")))

    print("\n[+] Esquema transformado y enriquecido:")
    df_clean.printSchema()

    registros_limpios = df_clean.count()
    print(f"\n[+] Total de registros válidos tras la limpieza: {registros_limpios:,}")

    # --------------------------------------------------------------------------
    # 5. ANÁLISIS EXPLORATORIO DE DATOS (EDA)
    # --------------------------------------------------------------------------
    print("\n" + "=" * 50)
    print(" 5. RESUMEN ESTADÍSTICO (EDA)")
    print("=" * 50)
    df_clean.select("VALOR").summary().show()

    # Récord máximo y mínimo histórico
    max_record = df_clean.orderBy(F.col("VALOR").desc()).first()
    min_record = df_clean.orderBy(F.col("VALOR").asc()).first()
    print(f"[+] TRM Máxima histórica: ${max_record['VALOR']:,.2f} COP registrada el {max_record['FECHA_DESDE']}")
    print(f"[+] TRM Mínima histórica: ${min_record['VALOR']:,.2f} COP registrada el {min_record['FECHA_DESDE']}")

    # Consulta solicitada en la guía: Días con valor mayor a 5000 COP
    print("\n" + "=" * 50)
    print(" CONSULTA: DÍAS CON TRM MAYOR A $5,000 COP")
    print("=" * 50)
    dias_mayores_5000 = df_clean.filter(F.col("VALOR") > 5000) \
        .select("FECHA_DESDE", "VALOR", "UNIDAD") \
        .orderBy(F.col("VALOR").desc())
    
    conteo_mayores_5000 = dias_mayores_5000.count()
    print(f"[+] Cantidad de jornadas con TRM superior a $5,000 COP: {conteo_mayores_5000}")
    dias_mayores_5000.show(15, truncate=False)

    # Consulta: Top 10 días con TRM más alta registrada
    print("\n" + "=" * 50)
    print(" TOP 10 JORNADAS CON MAYOR TRM HISTÓRICA")
    print("=" * 50)
    top_10_altas = df_clean.select("FECHA_DESDE", "VALOR") \
        .orderBy(F.col("VALOR").desc()) \
        .limit(10)
    top_10_altas.show(truncate=False)

    # Agregación y Métricas por Año (Promedio, Mínimo, Máximo y Desviación)
    print("\n" + "=" * 50)
    print(" ANÁLISIS HISTÓRICO DE TRM POR AÑO (ÚLTIMOS 10 AÑOS)")
    print("=" * 50)
    resumen_anual = df_clean.groupBy("ANIO") \
        .agg(
            F.round(F.avg("VALOR"), 2).alias("TRM_PROMEDIO"),
            F.round(F.min("VALOR"), 2).alias("TRM_MINIMA"),
            F.round(F.max("VALOR"), 2).alias("TRM_MAXIMA"),
            F.round(F.stddev("VALOR"), 2).alias("DESV_ESTANDAR"),
            F.count("VALOR").alias("JORNADAS")
        ) \
        .orderBy(F.col("ANIO").desc())

    resumen_anual.show(10, truncate=False)

    # --------------------------------------------------------------------------
    # 6. DEMOSTRACIÓN COMPLETA CON RDDS (TRANSFORMACIONES Y ACCIONES)
    #    (Cumplimiento explícito del criterio de operaciones con RDDs)
    # --------------------------------------------------------------------------
    print("\n" + "=" * 50)
    print(" 6. PROCESAMIENTO UTILIZANDO RDDs (SPARK CORE)")
    print("=" * 50)

    # Conversión del DataFrame limpio a RDD
    rdd_trm = df_clean.select("ANIO", "VALOR").rdd

    # A) TRANSFORMACIÓN: map()
    # Estructuramos a pares clave-valor: (Año, (Valor, 1)) para calcular promedios
    rdd_mapped = rdd_trm.map(lambda fila: (fila["ANIO"], (float(fila["VALOR"]), 1)))

    # B) TRANSFORMACIÓN: filter()
    # Filtramos años a partir del 2020 en adelante
    rdd_filtrado = rdd_mapped.filter(lambda par: par[0] >= 2020)

    # C) TRANSFORMACIÓN: reduceByKey()
    # Sumamos los valores acumulados y el conteo de días por cada año
    rdd_agregado = rdd_filtrado.reduceByKey(
        lambda a, b: (a[0] + b[0], a[1] + b[1])
    )

    # D) TRANSFORMACIÓN: mapValues() / map() para calcular el promedio final
    rdd_promedios = rdd_agregado.map(
        lambda item: (item[0], round(item[1][0] / item[1][1], 2), item[1][1])
    )

    # ACCIONES:
    # 1. count()
    total_anios_rdd = rdd_promedios.count()
    print(f"[+] [RDD Acción count()] Total de años analizados (>= 2020): {total_anios_rdd}")

    # 2. take()
    muestra_take = rdd_promedios.take(3)
    print(f"[+] [RDD Acción take(3)] Muestra de 3 elementos procesados:")
    for elem in muestra_take:
        print(f"    - Año {elem[0]}: Promedio = ${elem[1]:,.2f} COP (Total muestras: {elem[2]})")

    # 3. collect()
    print(f"\n[+] [RDD Acción collect()] Resultados consolidados por año desde RDD:")
    resultados_rdd = rdd_promedios.collect()
    resultados_rdd_ordenados = sorted(resultados_rdd, key=lambda x: x[0], reverse=True)
    for anio, prom, count in resultados_rdd_ordenados:
        print(f"    * Año: {anio} | TRM Promedio RDD: ${prom:,.2f} COP | Días evaluados: {count}")

    # 4. reduce()
    # Obtener el año con mayor promedio absoluto usando reduce
    max_anio_prom = rdd_promedios.reduce(lambda x, y: x if x[1] > y[1] else y)
    print(f"\n[+] [RDD Acción reduce()] Año con el promedio más elevado: {max_anio_prom[0]} (${max_anio_prom[1]:,.2f} COP)")

    # --------------------------------------------------------------------------
    # 7. CONCLUSIÓN Y CIERRE DE LA SESIÓN DE SPARK
    # --------------------------------------------------------------------------
    print("\n" + "=" * 80)
    print(" PROCESAMIENTO EN BATCH FINALIZADO CON ÉXITO")
    print("=" * 80)

    # Detener la sesión para liberar recursos en el cluster/JVM
    spark.stop()

if __name__ == "__main__":
    main()
