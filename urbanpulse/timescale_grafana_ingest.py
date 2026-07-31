"""
UrbanPulse TimescaleDB & Grafana Ingestion Daemon
=================================================
Consumes telemetry and incident events from Kafka topics:
  - urbanpulse.air_quality
  - urbanpulse.traffic_signals
  - urbanpulse.bus_gps
  - urbanpulse.smart_meters
  - urbanpulse.incidents

Persists structured telemetry records into TimescaleDB tables (hypertables)
so Grafana can query and display real-time live dashboards out of the box.
"""

import os
import sys
import json
import time
import logging
from datetime import datetime, timezone
import psycopg2
from psycopg2.extras import execute_values
from confluent_kafka import Consumer, KafkaError

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(name)s: %(message)s'
)
logger = logging.getLogger("TimescaleDBIngest")

BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = int(os.getenv("DB_PORT", "5432"))
DB_USER = os.getenv("DB_USER", "urbanpulse")
DB_PASS = os.getenv("DB_PASSWORD", "urbanpulse_2026")
DB_NAME = os.getenv("DB_NAME", "urbanpulse_db")

TOPICS = [
    "urbanpulse.air_quality",
    "urbanpulse.traffic_signals",
    "urbanpulse.bus_gps",
    "urbanpulse.smart_meters",
    "urbanpulse.incidents"
]

def init_db():
    """Create schema and TimescaleDB tables if not already existing."""
    while True:
        try:
            conn = psycopg2.connect(
                host=DB_HOST, port=DB_PORT, user=DB_USER, password=DB_PASS, dbname=DB_NAME
            )
            conn.autocommit = True
            cur = conn.cursor()
            
            # Enable TimescaleDB extension if available
            try:
                cur.execute("CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE;")
            except Exception as e:
                logger.warning(f"Could not enable timescaledb extension (standard PG will be used): {e}")

            # 1. Air Quality Table
            cur.execute("""
            CREATE TABLE IF NOT EXISTS air_quality_telemetry (
                timestamp TIMESTAMPTZ NOT NULL,
                sensor_id VARCHAR(64) NOT NULL,
                zone VARCHAR(64) NOT NULL,
                pm25 DOUBLE PRECISION,
                pm10 DOUBLE PRECISION,
                no2 DOUBLE PRECISION,
                aqi DOUBLE PRECISION
            );
            """)

            # 2. Traffic Signal Telemetry
            cur.execute("""
            CREATE TABLE IF NOT EXISTS traffic_signal_telemetry (
                timestamp TIMESTAMPTZ NOT NULL,
                junction_id VARCHAR(64) NOT NULL,
                zone VARCHAR(64) NOT NULL,
                vehicle_count INT,
                avg_wait_sec DOUBLE PRECISION,
                signal_phase VARCHAR(32)
            );
            """)

            # 3. Bus GPS Telemetry
            cur.execute("""
            CREATE TABLE IF NOT EXISTS bus_gps_telemetry (
                timestamp TIMESTAMPTZ NOT NULL,
                bus_id VARCHAR(64) NOT NULL,
                route_id VARCHAR(64) NOT NULL,
                lat DOUBLE PRECISION,
                lon DOUBLE PRECISION,
                speed_kmh DOUBLE PRECISION,
                occupancy_pct INT
            );
            """)

            # 4. Smart Meter Telemetry
            cur.execute("""
            CREATE TABLE IF NOT EXISTS smart_meter_telemetry (
                timestamp TIMESTAMPTZ NOT NULL,
                meter_id VARCHAR(64) NOT NULL,
                ward_id VARCHAR(64) NOT NULL,
                kwh_reading DOUBLE PRECISION,
                voltage DOUBLE PRECISION,
                power_factor DOUBLE PRECISION
            );
            """)

            # 5. Incidents Stream
            cur.execute("""
            CREATE TABLE IF NOT EXISTS urban_incidents (
                timestamp TIMESTAMPTZ NOT NULL,
                incident_id VARCHAR(64),
                incident_type VARCHAR(64) NOT NULL,
                key_id VARCHAR(64),
                severity VARCHAR(32),
                description TEXT
            );
            """)

            cur.close()
            conn.close()
            logger.info("✅ Database schema initialized successfully.")
            break
        except Exception as e:
            logger.error(f"Waiting for database connection to {DB_HOST}:{DB_PORT}... ({e})")
            time.sleep(3)

def main():
    init_db()

    consumer_config = {
        'bootstrap.servers': BOOTSTRAP_SERVERS,
        'group.id': 'urbanpulse-timescale-grafana-ingest-v2',
        'auto.offset.reset': 'earliest',
        'enable.auto.commit': True,
    }
    consumer = Consumer(consumer_config)
    consumer.subscribe(TOPICS)

    conn = psycopg2.connect(
        host=DB_HOST, port=DB_PORT, user=DB_USER, password=DB_PASS, dbname=DB_NAME
    )
    conn.autocommit = True
    cur = conn.cursor()

    logger.info(f"Subscribed to {TOPICS}. Streaming to TimescaleDB...")

    processed_count = 0
    start_time = time.time()

    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() != KafkaError._PARTITION_EOF:
                    logger.error(f"Kafka error: {msg.error()}")
                continue

            topic = msg.topic()
            try:
                data = json.loads(msg.value().decode('utf-8'))
            except Exception:
                continue

            ts_str = data.get("timestamp") or datetime.now(timezone.utc).isoformat()
            
            if topic == "urbanpulse.air_quality":
                if data.get("aqi") is not None:
                    cur.execute("""
                        INSERT INTO air_quality_telemetry (timestamp, sensor_id, zone, pm25, pm10, no2, aqi)
                        VALUES (%s, %s, %s, %s, %s, %s, %s)
                    """, (ts_str, data.get("sensor_id"), data.get("zone"), data.get("pm25"), data.get("pm10"), data.get("no2"), data.get("aqi")))

            elif topic == "urbanpulse.traffic_signals":
                cur.execute("""
                    INSERT INTO traffic_signal_telemetry (timestamp, junction_id, zone, vehicle_count, avg_wait_sec, signal_phase)
                    VALUES (%s, %s, %s, %s, %s, %s)
                """, (ts_str, data.get("junction_id"), data.get("zone"), data.get("vehicle_count"), data.get("avg_wait_sec"), data.get("signal_phase")))

            elif topic == "urbanpulse.bus_gps":
                cur.execute("""
                    INSERT INTO bus_gps_telemetry (timestamp, bus_id, route_id, lat, lon, speed_kmh, occupancy_pct)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                """, (ts_str, data.get("bus_id"), data.get("route_id"), data.get("lat"), data.get("lon"), data.get("speed_kmh"), data.get("occupancy_pct")))

            elif topic == "urbanpulse.smart_meters":
                cur.execute("""
                    INSERT INTO smart_meter_telemetry (timestamp, meter_id, ward_id, kwh_reading, voltage, power_factor)
                    VALUES (%s, %s, %s, %s, %s, %s)
                """, (ts_str, data.get("meter_id"), data.get("ward_id"), data.get("kwh_reading"), data.get("voltage"), data.get("power_factor")))

            elif topic == "urbanpulse.incidents":
                cur.execute("""
                    INSERT INTO urban_incidents (timestamp, incident_id, incident_type, key_id, severity, description)
                    VALUES (%s, %s, %s, %s, %s, %s)
                """, (ts_str, data.get("incident_id"), data.get("incident_type"), data.get("sensor_id") or data.get("junction_id") or data.get("route_id"), data.get("severity"), data.get("description")))

            processed_count += 1
            if processed_count % 500 == 0:
                elapsed = time.time() - start_time
                logger.info(f"Progress: {processed_count:,} records streamed to TimescaleDB ({processed_count/max(1, elapsed):.1f} rec/s)")

    except KeyboardInterrupt:
        logger.info("Stopping TimescaleDB Ingestion Daemon...")
    finally:
        cur.close()
        conn.close()
        consumer.close()

if __name__ == "__main__":
    main()
