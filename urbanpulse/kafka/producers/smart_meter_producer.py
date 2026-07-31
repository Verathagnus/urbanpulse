"""Smart electricity meter telemetry producer.

Simulates power meter telemetry for 1.1 million smart meters mapped across 20 municipal wards.
Keyed by `ward_id` to route records to dedicated ward partitions for Spark Structured Streaming rollups.
"""

import json
import time
import random
import logging
import argparse
from datetime import datetime, timezone
from confluent_kafka import Producer

TOPIC = "urbanpulse.smart_meters"

WARSD = [f"WARD_{i:02d}" for i in range(1, 21)]

# Ward metadata definitions specifying baseline load profiles and consumer types
WARD_PROFILES = {
    "WARD_01": {"meters": 55000, "base_kwh": 1200, "type": "residential"},
    "WARD_02": {"meters": 58000, "base_kwh": 1350, "type": "residential"},
    "WARD_03": {"meters": 52000, "base_kwh": 1800, "type": "commercial"},
    "WARD_04": {"meters": 60000, "base_kwh": 1100, "type": "residential"},
    "WARD_05": {"meters": 48000, "base_kwh": 2500, "type": "industrial"},
    "WARD_06": {"meters": 54000, "base_kwh": 1250, "type": "residential"},
    "WARD_07": {"meters": 56000, "base_kwh": 1400, "type": "mixed"},
    "WARD_08": {"meters": 50000, "base_kwh": 2200, "type": "commercial"},
    "WARD_09": {"meters": 62000, "base_kwh": 1050, "type": "residential"},
    "WARD_10": {"meters": 45000, "base_kwh": 3000, "type": "industrial"},
    "WARD_11": {"meters": 57000, "base_kwh": 1300, "type": "residential"},
    "WARD_12": {"meters": 53000, "base_kwh": 1600, "type": "mixed"},
    "WARD_13": {"meters": 59000, "base_kwh": 1150, "type": "residential"},
    "WARD_14": {"meters": 51000, "base_kwh": 2100, "type": "commercial"},
    "WARD_15": {"meters": 55000, "base_kwh": 1450, "type": "mixed"},
    "WARD_16": {"meters": 47000, "base_kwh": 2800, "type": "industrial"},
    "WARD_17": {"meters": 61000, "base_kwh": 1000, "type": "residential"},
    "WARD_18": {"meters": 54000, "base_kwh": 1550, "type": "mixed"},
    "WARD_19": {"meters": 49000, "base_kwh": 2400, "type": "commercial"},
    "WARD_20": {"meters": 56000, "base_kwh": 1200, "type": "residential"},
}

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(name)s: %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger("SmartMeterProducer")


class MeterSimulator:
    """Simulates cumulative power meter readings and grid power quality metrics."""

    def __init__(self, meter_id: str, ward_id: str):
        self.meter_id = meter_id
        self.ward_id = ward_id
        profile = WARD_PROFILES.get(ward_id, {"base_kwh": 1200, "type": "residential"})
        self.base_kwh = profile["base_kwh"]
        self.meter_type = profile["type"]

        self.kwh_reading = random.uniform(500, 5000)
        self.base_voltage = 230.0
        self.base_power_factor = 0.92 if self.meter_type == "industrial" else 0.95

    def generate_event(self) -> dict:
        """Computes incremental power consumption and grid voltage stability metrics."""
        if self.meter_type == "industrial":
            kwh_increment = random.uniform(0.5, 3.0)
        elif self.meter_type == "commercial":
            kwh_increment = random.uniform(0.3, 2.0)
        else:
            kwh_increment = random.uniform(0.1, 1.0)

        self.kwh_reading += kwh_increment

        voltage = round(self.base_voltage + random.gauss(0, 5), 1)
        if random.random() < 0.01:
            voltage = round(random.uniform(200, 250), 1)

        power_factor = round(
            max(0.70, min(1.0, self.base_power_factor + random.gauss(0, 0.03))), 2)

        return {
            "meter_id": self.meter_id,
            "ward_id": self.ward_id,
            "kwh_reading": round(self.kwh_reading, 2),
            "voltage": voltage,
            "power_factor": power_factor,
            "timestamp": datetime.now(timezone.utc).strftime(
                "%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"
        }


def create_producer(bootstrap_servers: str) -> Producer:
    """Initializes idempotent Kafka producer client for energy metric publishing."""
    config = {
        'bootstrap.servers': bootstrap_servers,
        'client.id': 'urbanpulse-smart-meter-producer',
        'enable.idempotence': True,
        'acks': 'all',
        'retries': 2147483647,
        'max.in.flight.requests.per.connection': 5,
        'batch.size': 65536,
        'linger.ms': 10,
        'compression.type': 'lz4',
        'queue.buffering.max.kbytes': 65536,
    }
    return Producer(config)


def delivery_callback(err, msg):
    if err is not None:
        logger.error(f"Delivery failed: {err}")


def main():
    parser = argparse.ArgumentParser(description="Smart Meter Telemetry Producer")
    parser.add_argument('--bootstrap-servers', default='localhost:9092')
    parser.add_argument('--rate', type=int, default=1100)
    parser.add_argument('--duration', type=int, default=300)
    parser.add_argument('--num-meters', type=int, default=1100, help='Active meter count')
    args = parser.parse_args()

    logger.info(f"Initializing Smart Meter Producer -> {TOPIC} (key=ward_id)")

    producer = create_producer(args.bootstrap_servers)

    meters = []
    meters_per_ward = max(1, args.num_meters // len(WARSD))
    for ward_id in WARSD:
        for i in range(meters_per_ward):
            meter_id = f"MTR_{ward_id}_{i:04d}"
            meters.append(MeterSimulator(meter_id, ward_id))

    logger.info(f"Meters active: {len(meters)} across {len(WARSD)} wards")

    total_sent = 0
    start_time = time.time()

    try:
        while args.duration <= 0 or (time.time() - start_time < args.duration):
            batch_start = time.time()

            for i in range(min(args.rate, len(meters))):
                meter = meters[i % len(meters)]
                event = meter.generate_event()

                try:
                    # Keying by ward_id guarantees partition assignment for ward-level streaming aggregations
                    producer.produce(
                        topic=TOPIC,
                        key=event["ward_id"].encode('utf-8'),
                        value=json.dumps(event).encode('utf-8'),
                        callback=delivery_callback
                    )
                    total_sent += 1
                except BufferError:
                    producer.poll(0.1)

            producer.poll(0)

            batch_elapsed = time.time() - batch_start
            sleep_time = max(0, 1.0 - batch_elapsed)
            if sleep_time > 0:
                time.sleep(sleep_time)

            elapsed = time.time() - start_time
            if int(elapsed) % 10 == 0 and int(elapsed) > 0:
                logger.info(f"Status: {total_sent:,} events | Rate: {total_sent/elapsed:.0f} evts/s")

    except KeyboardInterrupt:
        logger.info("Producer execution interrupted by user.")
    finally:
        producer.flush(timeout=30)
        elapsed = time.time() - start_time
        logger.info(f"Summary: Sent {total_sent:,} events over {elapsed:.1f}s (Avg {total_sent/max(1,elapsed):.0f} evts/s)")


if __name__ == "__main__":
    main()
