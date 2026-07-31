"""Spark Structured Streaming SQL AQI Health Advisory Processor.

Computes 10-minute sliding window average AQI per zone (1-minute slide interval) over the `urbanpulse.air_quality` stream.
Joins real-time window aggregations with static `zone_profile` metadata, filters for unhealthy air quality (rolling_avg_aqi > 150),
and writes enriched advisories in Update mode to `urbanpulse.health_advisories`.
"""

import os
import sys
import argparse
import logging

env_dir = os.path.dirname(os.path.dirname(sys.executable))
if os.path.exists(os.path.join(env_dir, "bin", "java")):
    os.environ["JAVA_HOME"] = env_dir

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, to_json, struct, to_timestamp, expr
)
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType
)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(name)s: %(message)s'
)
logger = logging.getLogger("AQIHealthAdvisory")


def setup_hadoop_win():
    """Sets up local Hadoop native binary dependencies when running on Windows."""
    if sys.platform == 'win32' and not os.environ.get("HADOOP_HOME"):
        base_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
        hadoop_dir = os.path.join(base_dir, "hadoop")
        bin_dir = os.path.join(hadoop_dir, "bin")
        os.makedirs(bin_dir, exist_ok=True)
        
        winutils_path = os.path.join(bin_dir, "winutils.exe")
        hadoop_dll_path = os.path.join(bin_dir, "hadoop.dll")
        
        import urllib.request
        opener = urllib.request.build_opener()
        opener.addheaders = [('User-agent', 'Mozilla/5.0')]
        urllib.request.install_opener(opener)
        
        if not os.path.exists(winutils_path):
            logger.info("Fetching winutils.exe binary...")
            url = "https://raw.githubusercontent.com/cdarlint/winutils/master/hadoop-3.3.5/bin/winutils.exe"
            try:
                urllib.request.urlretrieve(url, winutils_path)
            except Exception as e:
                logger.error(f"Failed to fetch winutils.exe: {e}")
                
        if not os.path.exists(hadoop_dll_path):
            logger.info("Fetching hadoop.dll binary...")
            url = "https://raw.githubusercontent.com/cdarlint/winutils/master/hadoop-3.3.5/bin/hadoop.dll"
            try:
                urllib.request.urlretrieve(url, hadoop_dll_path)
            except Exception as e:
                logger.error(f"Failed to fetch hadoop.dll: {e}")
                
        os.environ["HADOOP_HOME"] = hadoop_dir
        os.environ["PATH"] = bin_dir + os.pathsep + os.environ["PATH"]

setup_hadoop_win()


def main():
    parser = argparse.ArgumentParser(description="AQI Health Advisory Streaming SQL Pipeline")
    parser.add_argument('--bootstrap-servers', default='localhost:9092', help='Kafka bootstrap servers')
    parser.add_argument('--zone-profile-csv', default=None, help='Path to zone_profile.csv')
    
    base_dir = os.path.dirname(os.path.dirname(__file__))
    parser.add_argument('--checkpoint-dir', 
                        default=os.path.join(base_dir, 'data', 'checkpoints', 'aqi_advisory'),
                        help='Checkpoint path')
    args = parser.parse_args()

    if args.zone_profile_csv is None:
        base_dir = os.path.dirname(os.path.dirname(__file__))
        zone_csv = os.path.join(base_dir, "data", "zone_profile.csv")
    else:
        zone_csv = args.zone_profile_csv

    logger.info(f"Initializing AQI Health Advisory Query -> Bootstrap: {args.bootstrap_servers}")

    spark = (
        SparkSession.builder
        .appName("UrbanPulse-AQIHealthAdvisory")
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.streaming.stopGracefullyOnShutdown", "true")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    if not os.path.exists(zone_csv):
        logger.error(f"Zone profile CSV missing at {zone_csv}")
        return

    # Static metadata table for stream-static join
    static_zone_df = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv(zone_csv)
    )
    static_zone_df.createOrReplaceTempView("zone_profile")

    aqi_schema = StructType([
        StructField("sensor_id", StringType(), True),
        StructField("zone", StringType(), True),
        StructField("pm25", DoubleType(), True),
        StructField("pm10", DoubleType(), True),
        StructField("no2", DoubleType(), True),
        StructField("aqi", DoubleType(), True),
        StructField("timestamp", StringType(), True)
    ])

    raw_aqi_df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", args.bootstrap_servers)
        .option("subscribe", "urbanpulse.air_quality")
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load()
    )

    parsed_aqi_df = (
        raw_aqi_df
        .select(from_json(col("value").cast("string"), aqi_schema).alias("data"))
        .select("data.*")
        .filter(col("aqi").isNotNull())
        .withColumn("event_timestamp", to_timestamp(col("timestamp"), "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"))
        .withWatermark("event_timestamp", "15 minutes")
    )

    parsed_aqi_df.createOrReplaceTempView("air_quality_stream")

    # Streaming SQL query computing sliding 10-minute window averages and joining zone static metadata
    streaming_sql_query = """
        SELECT 
            w.start AS window_start,
            w.end AS window_end,
            s.zone,
            z.zone_name,
            z.population,
            z.num_schools,
            z.num_hospitals,
            ROUND(AVG(s.aqi), 1) AS rolling_avg_aqi,
            ROUND(AVG(s.pm25), 1) AS rolling_avg_pm25,
            ROUND(AVG(s.pm10), 1) AS rolling_avg_pm10,
            ROUND(AVG(s.no2), 1) AS rolling_avg_no2,
            CASE 
                WHEN AVG(s.aqi) > 300 THEN 'HAZARDOUS'
                WHEN AVG(s.aqi) > 200 THEN 'VERY_UNHEALTHY'
                ELSE 'UNHEALTHY'
            END AS advisory_level
        FROM (
            SELECT zone, aqi, pm25, pm10, no2, event_timestamp,
                   window(event_timestamp, '10 minutes', '1 minute') AS w
            FROM air_quality_stream
        ) s
        JOIN zone_profile z ON s.zone = z.zone
        GROUP BY w.start, w.end, s.zone, z.zone_name, z.population, z.num_schools, z.num_hospitals
        HAVING AVG(s.aqi) > 150
    """

    enriched_advisories_df = spark.sql(streaming_sql_query)

    kafka_sink_df = (
        enriched_advisories_df
        .select(
            col("zone").alias("key"),
            to_json(struct(
                col("window_start"),
                col("window_end"),
                col("zone"),
                col("zone_name"),
                col("population"),
                col("num_schools"),
                col("num_hospitals"),
                col("rolling_avg_aqi"),
                col("rolling_avg_pm25"),
                col("rolling_avg_pm10"),
                col("rolling_avg_no2"),
                col("advisory_level")
            )).alias("value")
        )
    )

    advisory_query = (
        kafka_sink_df.writeStream
        .format("kafka")
        .outputMode("update")
        .option("kafka.bootstrap.servers", args.bootstrap_servers)
        .option("topic", "urbanpulse.health_advisories")
        .option("checkpointLocation", args.checkpoint_dir)
        .queryName("AQIHealthAdvisorySink")
        .start()
    )

    logger.info("Spark Streaming SQL AQI Advisory Engine executing.")
    advisory_query.awaitTermination()


if __name__ == "__main__":
    main()
