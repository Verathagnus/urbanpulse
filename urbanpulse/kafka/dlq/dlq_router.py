"""Dead Letter Queue (DLQ) Validation Router.

Subscribes to all telemetry ingestion streams (`bus_gps`, `air_quality`, `traffic_signals`, `smart_meters`),
validates record payload schema & boundary criteria, and routes non-conforming messages to `urbanpulse.dlq`
tagged with classified `error_reason` metadata.
"""

import json
import time
import logging
import argparse
from datetime import datetime, timezone, timedelta
from confluent_kafka import Consumer, Producer, KafkaError

DLQ_TOPIC = "urbanpulse.dlq"

SOURCE_TOPICS = [
    "urbanpulse.bus_gps",
    "urbanpulse.air_quality",
    "urbanpulse.traffic_signals",
    "urbanpulse.smart_meters"
]

CITY_BOUNDS = {
    "lat_min": 18.85,
    "lat_max": 19.35,
    "lon_min": 72.70,
    "lon_max": 73.10
}

MAX_FUTURE_OFFSET_SEC = 300

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(name)s: %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger("DLQRouter")


class ValidationEngine:
    """Evaluates payload fields against domain integrity constraints."""

    def __init__(self):
        self.stats = {
            "null_aqi": 0,
            "aqi_out_of_range": 0,
            "impossible_gps": 0,
            "negative_speed": 0,
            "future_timestamp": 0,
            "missing_fields": 0,
            "json_parse_error": 0,
        }
        self.total_validated = 0
        self.total_invalid = 0

    def validate(self, topic: str, raw_value: bytes) -> list:
        """Parses payload bytes and checks topic-specific schema constraints."""
        self.total_validated += 1
        errors = []

        try:
            event = json.loads(raw_value.decode('utf-8'))
        except (json.JSONDecodeError, UnicodeDecodeError) as e:
            self.stats["json_parse_error"] += 1
            return [("JSON_PARSE_ERROR", f"Failed to parse JSON: {str(e)}")]

        if topic == "urbanpulse.air_quality":
            errors.extend(self._validate_air_quality(event))
        elif topic == "urbanpulse.bus_gps":
            errors.extend(self._validate_bus_gps(event))
        elif topic == "urbanpulse.traffic_signals":
            errors.extend(self._validate_traffic_signals(event))
        elif topic == "urbanpulse.smart_meters":
            errors.extend(self._validate_smart_meters(event))

        errors.extend(self._validate_timestamp(event))

        if errors:
            self.total_invalid += 1

        return errors

    def _validate_air_quality(self, event: dict) -> list:
        errors = []

        if event.get("aqi") is None:
            errors.append(("NULL_AQI", f"AQI value is null for sensor {event.get('sensor_id', 'unknown')}"))
            self.stats["null_aqi"] += 1
        elif not isinstance(event.get("aqi"), (int, float)):
            errors.append(("AQI_INVALID_TYPE", f"AQI is non-numeric: {event.get('aqi')}"))
            self.stats["aqi_out_of_range"] += 1
        elif event["aqi"] < 0 or event["aqi"] > 500:
            errors.append(("AQI_OUT_OF_RANGE", f"AQI value {event['aqi']} outside range [0, 500]"))
            self.stats["aqi_out_of_range"] += 1

        required = ["sensor_id", "zone", "pm25", "pm10", "no2", "timestamp"]
        for field in required:
            if field not in event:
                errors.append(("MISSING_FIELD", f"Required field '{field}' missing"))
                self.stats["missing_fields"] += 1

        return errors

    def _validate_bus_gps(self, event: dict) -> list:
        errors = []

        lat = event.get("lat")
        lon = event.get("lon")
        if lat is not None and lon is not None:
            if (lat < CITY_BOUNDS["lat_min"] or lat > CITY_BOUNDS["lat_max"] or
                    lon < CITY_BOUNDS["lon_min"] or lon > CITY_BOUNDS["lon_max"]):
                errors.append(("IMPOSSIBLE_GPS", f"GPS ({lat}, {lon}) outside municipal bounds"))
                self.stats["impossible_gps"] += 1
        elif lat is None or lon is None:
            errors.append(("MISSING_GPS", "GPS coordinates null"))
            self.stats["impossible_gps"] += 1

        speed = event.get("speed_kmh")
        if speed is not None and speed < 0:
            errors.append(("NEGATIVE_SPEED", f"Speed value {speed} km/h is negative"))
            self.stats["negative_speed"] += 1

        required = ["bus_id", "route_id", "lat", "lon", "speed_kmh", "occupancy_pct", "timestamp"]
        for field in required:
            if field not in event:
                errors.append(("MISSING_FIELD", f"Required field '{field}' missing"))
                self.stats["missing_fields"] += 1

        return errors

    def _validate_traffic_signals(self, event: dict) -> list:
        errors = []
        required = ["junction_id", "zone", "vehicle_count", "avg_wait_sec", "signal_phase", "timestamp"]
        for field in required:
            if field not in event:
                errors.append(("MISSING_FIELD", f"Required field '{field}' missing"))
                self.stats["missing_fields"] += 1

        if event.get("vehicle_count", 0) < 0:
            errors.append(("NEGATIVE_VEHICLE_COUNT", f"Vehicle count {event['vehicle_count']} is negative"))

        return errors

    def _validate_smart_meters(self, event: dict) -> list:
        errors = []
        required = ["meter_id", "ward_id", "kwh_reading", "voltage", "power_factor", "timestamp"]
        for field in required:
            if field not in event:
                errors.append(("MISSING_FIELD", f"Required field '{field}' missing"))
                self.stats["missing_fields"] += 1

        pf = event.get("power_factor")
        if pf is not None and (pf < 0 or pf > 1):
            errors.append(("POWER_FACTOR_OUT_OF_RANGE", f"Power factor {pf} outside range [0, 1]"))

        return errors

    def _validate_timestamp(self, event: dict) -> list:
        """Verifies event timestamp isn't skewed > 5 minutes in the future."""
        errors = []
        ts_str = event.get("timestamp")
        if ts_str:
            try:
                ts = datetime.fromisoformat(ts_str.replace('Z', '+00:00'))
                now = datetime.now(timezone.utc)
                if ts > now + timedelta(seconds=MAX_FUTURE_OFFSET_SEC):
                    errors.append(("FUTURE_TIMESTAMP", f"Timestamp {ts_str} skewed into future"))
                    self.stats["future_timestamp"] += 1
            except (ValueError, TypeError):
                errors.append(("INVALID_TIMESTAMP", f"Unparseable timestamp: {ts_str}"))
        return errors

    def get_stats(self) -> dict:
        return {
            "total_validated": self.total_validated,
            "total_invalid": self.total_invalid,
            "error_distribution": dict(self.stats),
            "invalid_rate": round(self.total_invalid / max(1, self.total_validated) * 100, 2)
        }


def create_dlq_producer(bootstrap_servers: str) -> Producer:
    config = {
        'bootstrap.servers': bootstrap_servers,
        'client.id': 'urbanpulse-dlq-router',
        'enable.idempotence': True,
        'acks': 'all',
        'compression.type': 'lz4',
    }
    return Producer(config)


def create_consumer(bootstrap_servers: str) -> Consumer:
    config = {
        'bootstrap.servers': bootstrap_servers,
        'group.id': 'DLQ_VALIDATION_ROUTER',
        'client.id': 'dlq-validation-router',
        'auto.offset.reset': 'latest',
        'enable.auto.commit': True,
        'auto.commit.interval.ms': 1000,
        'fetch.min.bytes': 1,
        'fetch.wait.max.ms': 100,
    }
    return Consumer(config)


def main():
    parser = argparse.ArgumentParser(description="Dead Letter Queue Validation Router")
    parser.add_argument('--bootstrap-servers', default='localhost:9092')
    parser.add_argument('--duration', type=int, default=300)
    args = parser.parse_args()

    logger.info(f"Initializing DLQ Router on topics {SOURCE_TOPICS} -> {DLQ_TOPIC}")

    consumer = create_consumer(args.bootstrap_servers)
    consumer.subscribe(SOURCE_TOPICS)

    dlq_producer = create_dlq_producer(args.bootstrap_servers)
    validator = ValidationEngine()

    start_time = time.time()
    last_log_time = start_time
    dlq_sent = 0

    try:
        while time.time() - start_time < args.duration:
            msg = consumer.poll(timeout=0.1)

            if msg is None:
                continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                logger.error(f"Consumer exception: {msg.error()}")
                continue

            errors = validator.validate(msg.topic(), msg.value())

            if errors:
                original_value = msg.value().decode('utf-8')
                dlq_message = {
                    "source_topic": msg.topic(),
                    "source_partition": msg.partition(),
                    "source_offset": msg.offset(),
                    "original_message": json.loads(original_value) if original_value else None,
                    "error_count": len(errors),
                    "errors": [{"error_type": et, "error_message": em} for et, em in errors],
                    "error_reason": errors[0][0],
                    "dlq_timestamp": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"
                }

                try:
                    dlq_producer.produce(
                        topic=DLQ_TOPIC,
                        key=errors[0][0].encode('utf-8'),
                        value=json.dumps(dlq_message).encode('utf-8')
                    )
                    dlq_sent += 1
                except Exception as e:
                    logger.error(f"Produce to DLQ failed: {e}")

            dlq_producer.poll(0)

            now = time.time()
            if now - last_log_time >= 10:
                stats = validator.get_stats()
                logger.info(
                    f"DLQ Status: Validated={stats['total_validated']:,} | "
                    f"Invalid={stats['total_invalid']:,} ({stats['invalid_rate']:.1f}%) | "
                    f"DLQ Sent={dlq_sent:,}")
                last_log_time = now

    except KeyboardInterrupt:
        logger.info("DLQ Router execution halted by user.")
    finally:
        dlq_producer.flush(timeout=30)
        consumer.close()

        stats = validator.get_stats()
        elapsed = time.time() - start_time
        logger.info(f"Summary: Validated {stats['total_validated']:,} records. DLQ routed: {dlq_sent:,} ({stats['invalid_rate']:.2f}%) over {elapsed:.1f}s")


if __name__ == "__main__":
    main()
