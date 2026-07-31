"""Spark Structured Streaming Ward Energy Analytics Engine.

Computes 15-minute tumbling window aggregations per `ward_id` over the `urbanpulse.smart_meters` stream.
Applies a 45-minute event-time watermark to account for network transmission delays from smart meters.
Emits real-time updates to `urbanpulse.ward_energy_summary` Kafka topic (Update mode)
and writes finalized window records to partitioned Parquet storage (Append mode).
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
    col, from_json, to_json, struct, window, sum as _sum, avg as _avg,
    max as _max, min as _min, to_timestamp, date_format, expr, lit
)
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, TimestampType
)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(name)s: %(message)s'
)
logger = logging.getLogger("WardEnergyStreaming")


def setup_hadoop_win():
    """Sets up local Hadoop native binary dependencies when running on Windows environment."""
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
    parser = argparse.ArgumentParser(description="Spark Structured Streaming Ward Energy Engine")
    parser.add_argument('--bootstrap-servers', default='localhost:9092', help='Kafka bootstrap servers')
    
    base_dir = os.path.dirname(os.path.dirname(__file__))
    parser.add_argument('--checkpoint-dir', 
                        default=os.path.join(base_dir, 'data', 'checkpoints', 'ward_energy'),
                        help='Checkpoint path')
    parser.add_argument('--parquet-output-dir', 
                        default=os.path.join(base_dir, 'data', 'ward_energy_parquet'),
                        help='Parquet output path')
    args = parser.parse_args()

    logger.info(f"Initializing Ward Energy Engine -> Bootstrap: {args.bootstrap_servers}")

    spark = (
        SparkSession.builder
        .appName("UrbanPulse-WardEnergyAnalytics")
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.streaming.stopGracefullyOnShutdown", "true")
        .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    meter_schema = StructType([
        StructField("meter_id", StringType(), True),
        StructField("ward_id", StringType(), True),
        StructField("kwh_reading", DoubleType(), True),
        StructField("voltage", DoubleType(), True),
        StructField("power_factor", DoubleType(), True),
        StructField("timestamp", StringType(), True)
    ])

    raw_kafka_df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", args.bootstrap_servers)
        .option("subscribe", "urbanpulse.smart_meters")
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load()
    )

    parsed_df = (
        raw_kafka_df
        .select(from_json(col("value").cast("string"), meter_schema).alias("data"))
        .select("data.*")
        .withColumn("event_timestamp", to_timestamp(col("timestamp"), "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"))
        .withColumn("date", date_format(col("event_timestamp"), "yyyy-MM-dd"))
    )

    # Apply 45-minute event-time watermark to tolerate out-of-order smart meter telemetry
    watermarked_df = parsed_df.withWatermark("event_timestamp", "45 minutes")

    # Group by 15-minute tumbling event window and ward_id to compute consumption aggregates
    aggregated_df = (
        watermarked_df
        .groupBy(
            window(col("event_timestamp"), "15 minutes"),
            col("ward_id"),
            col("date")
        )
        .agg(
            (_max("kwh_reading") - _min("kwh_reading")).alias("total_kwh_consumed"),
            _avg("power_factor").alias("avg_power_factor"),
            _max("voltage").alias("peak_voltage")
        )
        .select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            col("ward_id"),
            col("date"),
            expr("round(total_kwh_consumed, 2)").alias("total_kwh_consumed"),
            expr("round(avg_power_factor, 3)").alias("avg_power_factor"),
            expr("round(peak_voltage, 1)").alias("peak_voltage"),
            lit("BATCH").alias("source_layer")
        )
    )

    # Kafka sink formatting (Key = ward_id)
    kafka_output_df = (
        aggregated_df
        .select(
            col("ward_id").alias("key"),
            to_json(struct(
                col("window_start"),
                col("window_end"),
                col("ward_id"),
                col("total_kwh_consumed"),
                col("avg_power_factor"),
                col("peak_voltage"),
                col("source_layer")
            )).alias("value")
        )
    )

    # Stream to Kafka in Update mode for active window visibility
    kafka_query = (
        kafka_output_df.writeStream
        .format("kafka")
        .outputMode("update")
        .option("kafka.bootstrap.servers", args.bootstrap_servers)
        .option("topic", "urbanpulse.ward_energy_summary")
        .option("checkpointLocation", os.path.join(args.checkpoint_dir, "kafka_sink"))
        .queryName("WardEnergyKafkaSink")
        .start()
    )

    # Write finalized windows to Parquet, partitioned by ward_id and date
    parquet_query = (
        aggregated_df.writeStream
        .format("parquet")
        .outputMode("append")
        .option("path", args.parquet_output_dir)
        .option("checkpointLocation", os.path.join(args.checkpoint_dir, "parquet_sink"))
        .partitionBy("ward_id", "date")
        .queryName("WardEnergyParquetSink")
        .start()
    )

    logger.info("Spark Structured Streaming Ward Energy Queries executing.")
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
