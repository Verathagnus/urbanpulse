"""Air quality sensor telemetry simulator.

Ingests simulated AQI sensor data across 20 municipal zones into the 
'urbanpulse.air_quality' Kafka topic with at-least-once delivery guarantees.
Includes synthetic data quality anomalies (5% null AQI for DLQ routing tests)
and extreme pollution spikes (>300 AQI for Flink emergency alerts).
"""

import json
import time
import random
import logging
import argparse
from datetime import datetime, timezone
from confluent_kafka import Producer, KafkaError

TOPIC = "urbanpulse.air_quality"

# 20 municipal zones matching zone_profile.csv lookup table
ZONES = [
    "Zone_North", "Zone_NorthWest", "Zone_West", "Zone_Central",
    "Zone_South", "Zone_East", "Zone_NorthEast", "Zone_SouthEast",
    "Zone_Harbor", "Zone_Industrial", "Zone_SouthCentral", "Zone_WesternSuburb",
    "Zone_TransHarbor", "Zone_Extended", "Zone_Coastal", "Zone_Metro",
    "Zone_Heritage", "Zone_Commercial", "Zone_Suburban", "Zone_NewDev"
]

SENSORS_PER_ZONE = 30
NULL_AQI_RATE = 0.05          # 5% target null rate for DLQ schema validation
AQI_SPIKE_PROBABILITY = 0.02  # 2% probability of emergency spike (> 300 AQI)

MAX_RETRIES = 5
INITIAL_BACKOFF_MS = 100
MAX_BACKOFF_MS = 5000

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(name)s: %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger("AirQualityProducer")


class AQISensorSimulator:
    """Generates synthetic ambient air quality metrics using a mean-reverting random walk."""

    ZONE_BASE_AQI = {
        "Zone_Industrial": 180,
        "Zone_Metro": 160,
        "Zone_Commercial": 150,
        "Zone_TransHarbor": 145,
        "Zone_Central": 140,
        "Zone_East": 135,
        "Zone_NorthEast": 130,
        "Zone_SouthEast": 125,
        "Zone_North": 120,
        "Zone_NorthWest": 115,
        "Zone_West": 110,
        "Zone_SouthCentral": 130,
        "Zone_WesternSuburb": 105,
        "Zone_Extended": 100,
        "Zone_Harbor": 95,
        "Zone_Suburban": 90,
        "Zone_Heritage": 110,
        "Zone_South": 100,
        "Zone_Coastal": 75,
        "Zone_NewDev": 85
    }

    def __init__(self, sensor_id: str, zone: str):
        self.sensor_id = sensor_id
        self.zone = zone
        self.base_aqi = self.ZONE_BASE_AQI.get(zone, 100)
        self.sensor_offset = random.gauss(0, 15)
        self.last_aqi = self.base_aqi + self.sensor_offset

    def generate_reading(self) -> dict:
        """Simulates sensor sampling step with stochastic fluctuation and anomaly injection."""
        aqi_change = random.gauss(0, 8)
        mean_reversion = (self.base_aqi + self.sensor_offset - self.last_aqi) * 0.1
        self.last_aqi = max(0, self.last_aqi + aqi_change + mean_reversion)

        if random.random() < AQI_SPIKE_PROBABILITY:
            self.last_aqi = random.uniform(301, 480)
            logger.warning(f"AQI SPIKE: sensor={self.sensor_id}, zone={self.zone}, "
                           f"aqi={self.last_aqi:.0f} (HAZARDOUS)")

        aqi = round(self.last_aqi, 1)

        # Approximate atmospheric component concentrations
        pm25 = round(aqi * random.uniform(0.35, 0.45), 1)
        pm10 = round(aqi * random.uniform(0.50, 0.70), 1)
        no2 = round(aqi * random.uniform(0.10, 0.20), 1)

        # Corrupt 5% of payload readings to test DLQ processing
        inject_null = random.random() < NULL_AQI_RATE
        if inject_null:
            logger.info(f"Corrupting AQI payload: sensor={self.sensor_id}, zone={self.zone}")

        event = {
            "sensor_id": self.sensor_id,
            "zone": self.zone,
            "pm25": pm25,
            "pm10": pm10,
            "no2": no2,
            "aqi": None if inject_null else aqi,
            "timestamp": datetime.now(timezone.utc).strftime(
                "%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"
        }
        return event


def create_producer(bootstrap_servers: str) -> Producer:
    """Configures librdkafka client with idempotent at-least-once production guarantees."""
    config = {
        'bootstrap.servers': bootstrap_servers,
        'client.id': 'urbanpulse-air-quality-producer',
        'enable.idempotence': True,
        'acks': 'all',
        'retries': 2147483647,
        'max.in.flight.requests.per.connection': 5,
        'delivery.timeout.ms': 120000,
        'request.timeout.ms': 30000,
        'retry.backoff.ms': 100,
        'batch.size': 16384,
        'linger.ms': 10,
        'compression.type': 'snappy',
        'queue.buffering.max.kbytes': 32768,
    }
    return Producer(config)


class RetryableProducer:
    """Producer wrapper providing application-level exponential backoff on transient errors."""

    def __init__(self, producer: Producer):
        self.producer = producer
        self.retry_count = 0
        self.total_retries = 0
        self.total_failures = 0

    def produce_with_retry(self, topic: str, key: str, value: str, callback=None) -> bool:
        backoff_ms = INITIAL_BACKOFF_MS

        for attempt in range(1, MAX_RETRIES + 1):
            try:
                self.producer.produce(
                    topic=topic,
                    key=key.encode('utf-8') if key else None,
                    value=value.encode('utf-8'),
                    callback=callback
                )
                if attempt > 1:
                    self.total_retries += (attempt - 1)
                    logger.info(f"Delivered after {attempt} attempts (backoff: {backoff_ms}ms)")
                return True

            except BufferError:
                logger.warning(f"Producer buffer full on attempt {attempt}/{MAX_RETRIES}, polling...")
                self.producer.poll(backoff_ms / 1000.0)
                backoff_ms = min(backoff_ms * 2, MAX_BACKOFF_MS)

            except KafkaError as ke:
                logger.warning(f"Kafka exception on attempt {attempt}/{MAX_RETRIES}: {ke}")
                time.sleep(backoff_ms / 1000.0)
                backoff_ms = min(backoff_ms * 2, MAX_BACKOFF_MS)

            except Exception as e:
                logger.error(f"Unexpected error on attempt {attempt}/{MAX_RETRIES}: {e}")
                time.sleep(backoff_ms / 1000.0)
                backoff_ms = min(backoff_ms * 2, MAX_BACKOFF_MS)

        self.total_failures += 1
        logger.error(f"Failed to produce message after {MAX_RETRIES} attempts.")
        return False

    def poll(self, timeout=0):
        self.producer.poll(timeout)

    def flush(self, timeout=30):
        return self.producer.flush(timeout=timeout)


class DeliveryTracker:
    """Callback listener for tracking broker acknowledgments."""

    def __init__(self):
        self.delivered = 0
        self.failed = 0
        self.null_aqi_sent = 0
        self.aqi_spikes_sent = 0

    def callback(self, err, msg):
        if err is not None:
            self.failed += 1
            logger.error(f"Broker delivery failure: {err}")
        else:
            self.delivered += 1


def main():
    parser = argparse.ArgumentParser(description="Air Quality Telemetry Producer")
    parser.add_argument('--bootstrap-servers', default='localhost:9092', help='Kafka bootstrap servers')
    parser.add_argument('--rate', type=int, default=60, help='Target events per second')
    parser.add_argument('--duration', type=int, default=300, help='Stream duration in seconds')
    parser.add_argument('--num-sensors', type=int, default=60, help='Number of active sensors')
    args = parser.parse_args()

    logger.info(f"Initializing Air Quality Producer -> {TOPIC}")
    logger.info(f"Config: rate={args.rate}/s, duration={args.duration}s, bootstrap={args.bootstrap_servers}")

    raw_producer = create_producer(args.bootstrap_servers)
    producer = RetryableProducer(raw_producer)
    tracker = DeliveryTracker()

    sensors = []
    for zone in ZONES:
        num_sensors = max(1, args.num_sensors // len(ZONES))
        for i in range(num_sensors):
            sensor_id = f"AQI_{zone.split('_')[1]}_{i:02d}"
            sensors.append(AQISensorSimulator(sensor_id, zone))

    total_sent = 0
    null_aqi_count = 0
    spike_count = 0
    start_time = time.time()

    try:
        while args.duration <= 0 or (time.time() - start_time < args.duration):
            batch_start = time.time()

            for i in range(min(args.rate, len(sensors))):
                sensor = sensors[i % len(sensors)]
                event = sensor.generate_reading()

                if event["aqi"] is None:
                    null_aqi_count += 1
                elif event["aqi"] > 300:
                    spike_count += 1

                # Keying by zone ensures partition affinity for downstream windowing
                success = producer.produce_with_retry(
                    topic=TOPIC,
                    key=event["zone"],
                    value=json.dumps(event),
                    callback=tracker.callback
                )
                if success:
                    total_sent += 1

            producer.poll(0)

            batch_elapsed = time.time() - batch_start
            sleep_time = max(0, 1.0 - batch_elapsed)
            if sleep_time > 0:
                time.sleep(sleep_time)

            elapsed = time.time() - start_time
            if int(elapsed) % 10 == 0 and int(elapsed) > 0:
                actual_rate = total_sent / elapsed
                null_pct = (null_aqi_count / max(1, total_sent)) * 100
                logger.info(f"Status: {total_sent:,} sent | {actual_rate:.0f} evts/s | Null AQI: {null_pct:.1f}% | Spikes: {spike_count}")

    except KeyboardInterrupt:
        logger.info("Producer execution halted by user signal.")
    finally:
        logger.info("Flushing buffer queue...")
        producer.flush(timeout=30)

        elapsed = time.time() - start_time
        null_pct = (null_aqi_count / max(1, total_sent)) * 100

        logger.info("Summary Statistics:")
        logger.info(f"  Total Sent: {total_sent:,} | Delivered: {tracker.delivered:,} | Failures: {tracker.failed:,}")
        logger.info(f"  Null AQI Anomalies: {null_aqi_count:,} ({null_pct:.1f}%) | Spikes (>300): {spike_count:,}")
        logger.info(f"  Duration: {elapsed:.1f}s | Avg Rate: {total_sent/max(1,elapsed):.0f} evts/s")


if __name__ == "__main__":
    main()
