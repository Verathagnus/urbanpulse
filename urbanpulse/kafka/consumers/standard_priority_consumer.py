"""STANDARD_PRIORITY Kafka Consumer for Analytics Dashboard.

Deploys consumer instances belonging to group `STANDARD_ANALYTICS_DASHBOARD`.
Simulates an analytical query engine workload by introducing artificial per-message processing latency (200ms default),
demonstrating independent consumer group offset lag isolation relative to high-priority control consumers.
"""

import json
import time
import logging
import argparse
from datetime import datetime, timezone
from confluent_kafka import Consumer, KafkaError

TOPIC = "urbanpulse.traffic_signals"
GROUP_ID = "STANDARD_ANALYTICS_DASHBOARD"

PROCESSING_DELAY_MS = 200

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(name)s: %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger("StandardPriorityConsumer")


class DashboardProcessor:
    """Simulates an batch/analytical processing worker with CPU/IO overhead."""

    def __init__(self, consumer_id: int):
        self.consumer_id = consumer_id
        self.events_processed = 0
        self.zone_stats = {}

    def process_event(self, event: dict):
        """Processes event and applies simulated analytics execution delay."""
        self.events_processed += 1

        zone = event.get("zone", "unknown")
        if zone not in self.zone_stats:
            self.zone_stats[zone] = {
                "total_events": 0,
                "total_vehicle_count": 0,
                "total_wait_sec": 0,
                "max_wait_sec": 0,
                "gridlock_count": 0
            }

        stats = self.zone_stats[zone]
        stats["total_events"] += 1
        stats["total_vehicle_count"] += event.get("vehicle_count", 0)
        stats["total_wait_sec"] += event.get("avg_wait_sec", 0)
        stats["max_wait_sec"] = max(stats["max_wait_sec"], event.get("avg_wait_sec", 0))
        if event.get("avg_wait_sec", 0) > 180:
            stats["gridlock_count"] += 1

        # Simulated dashboard rendering / DB write latency (200ms)
        time.sleep(PROCESSING_DELAY_MS / 1000.0)

    def get_stats(self) -> dict:
        return {
            "consumer_id": self.consumer_id,
            "events_processed": self.events_processed,
            "zones_tracked": len(self.zone_stats),
            "zone_summary": {
                zone: {
                    "events": s["total_events"],
                    "avg_vehicle_count": round(s["total_vehicle_count"] / max(1, s["total_events"]), 1),
                    "avg_wait_sec": round(s["total_wait_sec"] / max(1, s["total_events"]), 1),
                    "max_wait_sec": s["max_wait_sec"],
                    "gridlock_count": s["gridlock_count"]
                }
                for zone, s in self.zone_stats.items()
            }
        }


def create_consumer(bootstrap_servers: str, consumer_id: int) -> Consumer:
    """Configures consumer with batch-oriented fetch options."""
    config = {
        'bootstrap.servers': bootstrap_servers,
        'group.id': GROUP_ID,
        'client.id': f'standard-analytics-dashboard-{consumer_id}',
        'auto.offset.reset': 'latest',
        'enable.auto.commit': True,
        'auto.commit.interval.ms': 5000,
        'fetch.min.bytes': 1024,
        'fetch.wait.max.ms': 500,
        'max.poll.interval.ms': 300000,
        'max.partition.fetch.bytes': 1048576,
    }
    return Consumer(config)


def report_lag(consumer: Consumer, consumer_id: int):
    """Logs current partition offset lag relative to topic log-end offsets."""
    try:
        assignment = consumer.assignment()
        if not assignment:
            return

        total_lag = 0
        partition_lags = []
        for tp in assignment:
            committed = consumer.committed([tp])[0]
            lo, hi = consumer.get_watermark_offsets(tp)
            committed_offset = committed.offset if committed and committed.offset >= 0 else 0
            lag = max(0, hi - committed_offset)
            total_lag += lag
            partition_lags.append(f"P{tp.partition}:{lag}")

        logger.info(
            f"STANDARD Consumer-{consumer_id} Partition Lag: "
            f"Total={total_lag:,} | Partitions=[{', '.join(partition_lags)}]")

    except Exception as e:
        logger.debug(f"Lag fetch exception: {e}")


def main():
    parser = argparse.ArgumentParser(description="STANDARD_PRIORITY Analytics Consumer")
    parser.add_argument('--bootstrap-servers', default='localhost:9092')
    parser.add_argument('--consumer-id', type=int, default=1, help='Consumer instance ID')
    parser.add_argument('--duration', type=int, default=300)
    parser.add_argument('--delay-ms', type=int, default=200, help='Processing delay per message in ms')
    args = parser.parse_args()

    global PROCESSING_DELAY_MS
    PROCESSING_DELAY_MS = args.delay_ms

    logger.info(f"Initializing STANDARD_PRIORITY Consumer #{args.consumer_id} -> Group: {GROUP_ID}")

    consumer = create_consumer(args.bootstrap_servers, args.consumer_id)
    consumer.subscribe([TOPIC])

    processor = DashboardProcessor(args.consumer_id)

    start_time = time.time()
    last_log_time = start_time

    try:
        while time.time() - start_time < args.duration:
            msg = consumer.poll(timeout=1.0)

            if msg is None:
                continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                logger.error(f"Consumer exception: {msg.error()}")
                continue

            try:
                event = json.loads(msg.value().decode('utf-8'))
                processor.process_event(event)
            except json.JSONDecodeError as e:
                logger.error(f"JSON parsing error: {e}")

            now = time.time()
            if now - last_log_time >= 10:
                stats = processor.get_stats()
                elapsed = now - start_time
                rate = stats["events_processed"] / elapsed

                logger.info(
                    f"STANDARD Consumer-{args.consumer_id} | "
                    f"Processed: {stats['events_processed']:,} | "
                    f"Rate: {rate:.0f}/s | Delay: {PROCESSING_DELAY_MS}ms/msg")

                report_lag(consumer, args.consumer_id)
                last_log_time = now

    except KeyboardInterrupt:
        logger.info("Consumer execution interrupted by user.")
    finally:
        consumer.close()
        stats = processor.get_stats()
        elapsed = time.time() - start_time
        logger.info(f"Summary: Consumer #{args.consumer_id} processed {stats['events_processed']:,} events over {elapsed:.1f}s")


if __name__ == "__main__":
    main()
