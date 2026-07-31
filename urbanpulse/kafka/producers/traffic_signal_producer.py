"""Traffic signal state telemetry producer.

Simulates junction controller states and vehicle wait queues across 3,800 municipal intersections.
Keyed by `junction_id` to route signals to 6 dedicated partitions for partition-affinity consumer scaling.
Generates gridlock patterns (wait times >180s over 3+ consecutive cycles) to trigger Flink stateful CEP rules.
"""

import json
import time
import random
import logging
import argparse
from datetime import datetime, timezone
from confluent_kafka import Producer

TOPIC = "urbanpulse.traffic_signals"

ZONES = [
    "Zone_North", "Zone_NorthWest", "Zone_West", "Zone_Central",
    "Zone_South", "Zone_East", "Zone_NorthEast", "Zone_SouthEast",
    "Zone_Harbor", "Zone_Industrial", "Zone_SouthCentral", "Zone_WesternSuburb",
    "Zone_TransHarbor", "Zone_Extended", "Zone_Coastal", "Zone_Metro",
    "Zone_Heritage", "Zone_Commercial", "Zone_Suburban", "Zone_NewDev"
]

SIGNAL_PHASES = ["RED", "GREEN", "AMBER"]
JUNCTIONS_PER_ZONE = 190
GRIDLOCK_PROBABILITY = 0.02

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(name)s: %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger("TrafficSignalProducer")


class JunctionSimulator:
    """Simulates signal phase transitions and intersection queue dynamics."""

    def __init__(self, junction_id: str, zone: str):
        self.junction_id = junction_id
        self.zone = zone
        self.cycle_index = random.randint(0, 2)
        self.base_vehicle_count = random.randint(20, 120)
        self.base_wait_sec = random.uniform(30, 90)
        self.gridlock_active = False
        self.gridlock_cycles_remaining = 0

    def generate_event(self) -> dict:
        """Advances signal phase and computes vehicle queue length and average wait times."""
        self.cycle_index = (self.cycle_index + 1) % 3
        phase = SIGNAL_PHASES[self.cycle_index]

        vehicle_count = max(0, int(self.base_vehicle_count + random.gauss(0, 15)))
        avg_wait_sec = max(0, round(self.base_wait_sec + random.gauss(0, 10), 1))

        # Inject gridlock state (>180s wait for multiple cycles) to test Flink ListState buffer evaluation
        if not self.gridlock_active and random.random() < GRIDLOCK_PROBABILITY:
            self.gridlock_active = True
            self.gridlock_cycles_remaining = random.randint(4, 8)
            logger.warning(f"Gridlock state injected: junction={self.junction_id}, zone={self.zone}")

        if self.gridlock_active:
            avg_wait_sec = random.uniform(185, 300)
            vehicle_count = random.randint(100, 200)
            self.gridlock_cycles_remaining -= 1
            if self.gridlock_cycles_remaining <= 0:
                self.gridlock_active = False
                logger.info(f"Gridlock cleared: junction={self.junction_id}")

        return {
            "junction_id": self.junction_id,
            "zone": self.zone,
            "vehicle_count": vehicle_count,
            "avg_wait_sec": round(avg_wait_sec, 1),
            "signal_phase": phase,
            "timestamp": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"
        }


def create_producer(bootstrap_servers: str) -> Producer:
    """Configures Kafka producer client with idempotence enabled."""
    config = {
        'bootstrap.servers': bootstrap_servers,
        'client.id': 'urbanpulse-traffic-signal-producer',
        'enable.idempotence': True,
        'acks': 'all',
        'retries': 2147483647,
        'max.in.flight.requests.per.connection': 5,
        'batch.size': 32768,
        'linger.ms': 5,
        'compression.type': 'lz4',
        'queue.buffering.max.kbytes': 32768,
    }
    return Producer(config)


def delivery_callback(err, msg):
    if err is not None:
        logger.error(f"Delivery failure: {err}")


def main():
    parser = argparse.ArgumentParser(description="Traffic Signal Telemetry Producer")
    parser.add_argument('--bootstrap-servers', default='localhost:9092')
    parser.add_argument('--rate', type=int, default=380)
    parser.add_argument('--duration', type=int, default=300)
    parser.add_argument('--num-junctions', type=int, default=380, help='Active junction count')
    args = parser.parse_args()

    logger.info(f"Initializing Traffic Signal Producer -> {TOPIC} (key=junction_id)")

    producer = create_producer(args.bootstrap_servers)

    junctions = []
    for zone in ZONES:
        num = max(1, args.num_junctions // len(ZONES))
        for i in range(num):
            jid = f"JNC_{zone.split('_')[1]}_{i:03d}"
            junctions.append(JunctionSimulator(jid, zone))

    logger.info(f"Junctions active: {len(junctions)} across {len(ZONES)} zones")

    total_sent = 0
    gridlock_events = 0
    start_time = time.time()

    try:
        while args.duration <= 0 or (time.time() - start_time < args.duration):
            batch_start = time.time()

            for i in range(min(args.rate, len(junctions))):
                junction = junctions[i % len(junctions)]
                event = junction.generate_event()

                if event["avg_wait_sec"] > 180:
                    gridlock_events += 1

                try:
                    producer.produce(
                        topic=TOPIC,
                        key=event["junction_id"].encode('utf-8'),
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
                logger.info(f"Status: {total_sent:,} events | Rate: {total_sent/elapsed:.0f} evts/s | Gridlock count: {gridlock_events}")

    except KeyboardInterrupt:
        logger.info("Producer execution halted by user.")
    finally:
        producer.flush(timeout=30)
        elapsed = time.time() - start_time
        logger.info(f"Summary: Sent {total_sent:,} events over {elapsed:.1f}s (Avg {total_sent/max(1,elapsed):.0f} evts/s)")


if __name__ == "__main__":
    main()
