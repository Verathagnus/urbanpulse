"""Bus GPS telemetry producer.

Simulates transit fleet telemetry for 12,000 municipal buses across ~20 active transit routes.
Keyed by `route_id` to guarantee per-route partition ordering in Kafka.
Supports synthetic bus bunching scenario generation (~3% rate) for Flink CEP evaluation.
"""

import json
import time
import random
import logging
import argparse
import math
from datetime import datetime, timezone
from confluent_kafka import Producer, KafkaError

TOPIC = "urbanpulse.bus_gps"

# Metropolitan bounding box coordinates
CITY_BOUNDS = {
    "lat_min": 18.87,
    "lat_max": 19.28,
    "lon_min": 72.77,
    "lon_max": 73.05
}

ROUTES = [
    {"route_id": "R_301_UP", "name": "Andheri-Borivali Express", "lat_start": 19.1197, "lon_start": 72.8464, "lat_end": 19.2307, "lon_end": 72.8567},
    {"route_id": "R_301_DN", "name": "Andheri-Borivali Express", "lat_start": 19.2307, "lon_start": 72.8567, "lat_end": 19.1197, "lon_end": 72.8464},
    {"route_id": "R_102_UP", "name": "Dadar-Bandra Circular", "lat_start": 19.0178, "lon_start": 72.8478, "lat_end": 19.0596, "lon_end": 72.8295},
    {"route_id": "R_102_DN", "name": "Dadar-Bandra Circular", "lat_start": 19.0596, "lon_start": 72.8295, "lat_end": 19.0178, "lon_end": 72.8478},
    {"route_id": "R_205_UP", "name": "Kurla-Chembur Link", "lat_start": 19.0726, "lon_start": 72.8796, "lat_end": 19.0522, "lon_end": 72.8986},
    {"route_id": "R_205_DN", "name": "Kurla-Chembur Link", "lat_start": 19.0522, "lon_start": 72.8986, "lat_end": 19.0726, "lon_end": 72.8796},
    {"route_id": "R_410_UP", "name": "Thane-Mulund Shuttle", "lat_start": 19.1860, "lon_start": 72.9757, "lat_end": 19.1726, "lon_end": 72.9566},
    {"route_id": "R_410_DN", "name": "Thane-Mulund Shuttle", "lat_start": 19.1726, "lon_start": 72.9566, "lat_end": 19.1860, "lon_end": 72.9757},
    {"route_id": "R_507_UP", "name": "Powai-Vikhroli Industrial", "lat_start": 19.1176, "lon_start": 72.9060, "lat_end": 19.1096, "lon_end": 72.9283},
    {"route_id": "R_507_DN", "name": "Powai-Vikhroli Industrial", "lat_start": 19.1096, "lon_start": 72.9283, "lat_end": 19.1176, "lon_end": 72.9060},
    {"route_id": "R_603_UP", "name": "Goregaon-Malad Fast", "lat_start": 19.1554, "lon_start": 72.8494, "lat_end": 19.1868, "lon_end": 72.8484},
    {"route_id": "R_603_DN", "name": "Goregaon-Malad Fast", "lat_start": 19.1868, "lon_start": 72.8484, "lat_end": 19.1554, "lon_end": 72.8494},
    {"route_id": "R_718_UP", "name": "Wadala-Sion Express", "lat_start": 19.0177, "lon_start": 72.8674, "lat_end": 19.0400, "lon_end": 72.8621},
    {"route_id": "R_718_DN", "name": "Wadala-Sion Express", "lat_start": 19.0400, "lon_start": 72.8621, "lat_end": 19.0177, "lon_end": 72.8674},
    {"route_id": "R_825_UP", "name": "Lower Parel-Worli Coastal", "lat_start": 19.0073, "lon_start": 72.8310, "lat_end": 19.0176, "lon_end": 72.8152},
    {"route_id": "R_825_DN", "name": "Lower Parel-Worli Coastal", "lat_start": 19.0176, "lon_start": 72.8152, "lat_end": 19.0073, "lon_end": 72.8310},
    {"route_id": "R_933_UP", "name": "Vashi-CBD Belapur Link", "lat_start": 19.0771, "lon_start": 72.9987, "lat_end": 19.0213, "lon_end": 73.0344},
    {"route_id": "R_933_DN", "name": "Vashi-CBD Belapur Link", "lat_start": 19.0213, "lon_start": 73.0344, "lat_end": 19.0771, "lon_end": 72.9987},
    {"route_id": "R_044_UP", "name": "Colaba-Fort Heritage", "lat_start": 18.9067, "lon_start": 72.8147, "lat_end": 18.9322, "lon_end": 72.8347},
    {"route_id": "R_044_DN", "name": "Colaba-Fort Heritage", "lat_start": 18.9322, "lon_start": 72.8347, "lat_end": 18.9067, "lon_end": 72.8147},
]

BUSES_PER_ROUTE = 600

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(name)s: %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger("BusGPSProducer")


class BusSimulator:
    """Tracks vehicle position, instantaneous velocity, and passenger load along route trajectory."""

    def __init__(self, bus_id: str, route: dict):
        self.bus_id = bus_id
        self.route_id = route["route_id"]
        self.route = route
        self.progress = random.uniform(0.0, 1.0)
        self.speed_kmh = random.uniform(15.0, 45.0)
        self.occupancy_pct = random.randint(10, 95)
        self.direction = 1

    def update(self):
        """Advances bus trajectory along line geometry according to dynamic speed model."""
        speed_delta = random.gauss(0, 5.0)
        self.speed_kmh = max(0, min(80, self.speed_kmh + speed_delta))

        route_length_km = self._route_length_km()
        if route_length_km > 0:
            progress_increment = (self.speed_kmh / 3600.0) / route_length_km
        else:
            progress_increment = 0.001
        self.progress += self.direction * progress_increment

        if self.progress >= 1.0:
            self.progress = 1.0
            self.direction = -1
        elif self.progress <= 0.0:
            self.progress = 0.0
            self.direction = 1

        self.occupancy_pct = max(0, min(100, self.occupancy_pct + random.randint(-5, 5)))

    def get_position(self) -> tuple:
        """Linearly interpolates current lat/lon position with Gaussian positioning noise."""
        lat = self.route["lat_start"] + self.progress * (
            self.route["lat_end"] - self.route["lat_start"])
        lon = self.route["lon_start"] + self.progress * (
            self.route["lon_end"] - self.route["lon_start"])
        lat += random.gauss(0, 0.0001)
        lon += random.gauss(0, 0.0001)
        return round(lat, 6), round(lon, 6)

    def _route_length_km(self) -> float:
        """Computes geodesic route length via Haversine formulation."""
        lat1 = math.radians(self.route["lat_start"])
        lat2 = math.radians(self.route["lat_end"])
        dlat = lat2 - lat1
        dlon = math.radians(self.route["lon_end"] - self.route["lon_start"])
        a = math.sin(dlat/2)**2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon/2)**2
        return 6371 * 2 * math.atan2(math.sqrt(a), math.sqrt(1-a))

    def generate_event(self) -> dict:
        """Generates structured GPS location payload."""
        self.update()
        lat, lon = self.get_position()
        return {
            "bus_id": self.bus_id,
            "route_id": self.route_id,
            "lat": lat,
            "lon": lon,
            "speed_kmh": round(self.speed_kmh, 1),
            "occupancy_pct": self.occupancy_pct,
            "timestamp": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"
        }


def create_producer(bootstrap_servers: str) -> Producer:
    """Configures librdkafka producer client with idempotent batching settings."""
    config = {
        'bootstrap.servers': bootstrap_servers,
        'client.id': 'urbanpulse-bus-gps-producer',
        'enable.idempotence': True,
        'acks': 'all',
        'retries': 2147483647,
        'max.in.flight.requests.per.connection': 5,
        'batch.size': 65536,
        'linger.ms': 5,
        'compression.type': 'lz4',
        'queue.buffering.max.kbytes': 65536,
        'delivery.timeout.ms': 120000,
        'request.timeout.ms': 30000,
    }
    return Producer(config)


def delivery_callback(err, msg):
    """Delivery confirmation handler for async produce requests."""
    if err is not None:
        logger.error(f"Failed to deliver record: {err} [topic={msg.topic()}, partition={msg.partition()}]")
    else:
        if random.random() < 0.001:
            logger.debug(f"Delivered {msg.topic()}[{msg.partition()}] @ offset {msg.offset()}")


def inject_bunching(buses: list, route_id: str, probability: float = 0.03):
    """Injects spatial proximity (< 200m) between pairs of buses to trigger Flink CEP rules."""
    if random.random() < probability:
        route_buses = [b for b in buses if b.route_id == route_id]
        if len(route_buses) >= 2:
            bus_a, bus_b = random.sample(route_buses, 2)
            bus_b.progress = bus_a.progress + random.uniform(-0.002, 0.002)
            bus_b.progress = max(0, min(1, bus_b.progress))
            logger.info(f"Bunching anomaly injected: {bus_a.bus_id} and {bus_b.bus_id} on route {route_id}")


def main():
    parser = argparse.ArgumentParser(description="Bus GPS Telemetry Producer")
    parser.add_argument('--bootstrap-servers', default='localhost:9092', help='Kafka bootstrap servers')
    parser.add_argument('--rate', type=int, default=2400, help='Target events per second')
    parser.add_argument('--duration', type=int, default=300, help='Duration in seconds')
    parser.add_argument('--num-buses', type=int, default=240, help='Simulated fleet size')
    args = parser.parse_args()

    logger.info(f"Initializing Bus GPS Producer -> {TOPIC} (key=route_id)")

    producer = create_producer(args.bootstrap_servers)

    buses = []
    for route in ROUTES:
        num_buses = args.num_buses // len(ROUTES)
        for i in range(max(1, num_buses)):
            bus_id = f"BEST_{route['route_id']}_{i:04d}"
            buses.append(BusSimulator(bus_id, route))

    logger.info(f"Fleet active: {len(buses)} buses across {len(ROUTES)} routes")

    total_sent = 0
    total_errors = 0
    start_time = time.time()

    try:
        while args.duration <= 0 or (time.time() - start_time < args.duration):
            batch_start = time.time()
            batch_target = min(args.rate, len(buses))

            for i in range(batch_target):
                bus = buses[i % len(buses)]

                if i % len(ROUTES) == 0:
                    inject_bunching(buses, bus.route_id)

                event = bus.generate_event()

                try:
                    # Partition by route_id to preserve event ordering per transit corridor
                    producer.produce(
                        topic=TOPIC,
                        key=event["route_id"].encode('utf-8'),
                        value=json.dumps(event).encode('utf-8'),
                        callback=delivery_callback
                    )
                    total_sent += 1
                except BufferError:
                    producer.poll(0.1)
                    try:
                        producer.produce(
                            topic=TOPIC,
                            key=event["route_id"].encode('utf-8'),
                            value=json.dumps(event).encode('utf-8'),
                            callback=delivery_callback
                        )
                        total_sent += 1
                    except Exception as e:
                        total_errors += 1
                        logger.error(f"Produce failure after retry: {e}")

            producer.poll(0)

            batch_elapsed = time.time() - batch_start
            sleep_time = max(0, 1.0 - batch_elapsed)
            if sleep_time > 0:
                time.sleep(sleep_time)

            elapsed = time.time() - start_time
            if int(elapsed) % 10 == 0 and int(elapsed) > 0:
                actual_rate = total_sent / elapsed
                logger.info(f"Status: {total_sent:,} sent | {actual_rate:.0f} evts/s | Errors: {total_errors}")

    except KeyboardInterrupt:
        logger.info("Producer execution interrupted by user.")
    finally:
        logger.info("Flushing remaining buffer...")
        remaining = producer.flush(timeout=30)
        if remaining > 0:
            logger.warning(f"{remaining} messages were dropped/undelivered")

        elapsed = time.time() - start_time
        logger.info(f"Summary: Sent {total_sent:,} events over {elapsed:.1f}s (Avg {total_sent/max(1,elapsed):.0f} evts/s)")


if __name__ == "__main__":
    main()
