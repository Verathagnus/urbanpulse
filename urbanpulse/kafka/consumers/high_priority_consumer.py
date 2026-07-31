"""HIGH_PRIORITY Kafka Consumer for Real-Time Signal Control.

Deploys a single consumer instance within consumer group `HIGH_PRIORITY_SIGNAL_CONTROL`
to process all 6 partitions of `urbanpulse.traffic_signals`.
Processes incoming records immediately without artificial latency delays to maintain near-zero lag.
"""

import json
import time
import logging
import argparse
from datetime import datetime, timezone
from confluent_kafka import Consumer, KafkaError, TopicPartition

TOPIC = "urbanpulse.traffic_signals"
GROUP_ID = "HIGH_PRIORITY_SIGNAL_CONTROL"

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(name)s: %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger("HighPriorityConsumer")


class SignalController:
    """Processes real-time signal telemetry for adaptive control loops."""

    def __init__(self):
        self.events_processed = 0
        self.gridlock_alerts = 0
        self.total_processing_time_ms = 0

    def process_event(self, event: dict):
        """Evaluates incoming signal status for gridlock conditions (avg_wait > 180s)."""
        start = time.time()
        self.events_processed += 1

        if event.get("avg_wait_sec", 0) > 180:
            self.gridlock_alerts += 1
            logger.warning(
                f"GRIDLOCK DETECTED: junction={event['junction_id']}, "
                f"zone={event['zone']}, avg_wait={event['avg_wait_sec']}s")

        elapsed_ms = (time.time() - start) * 1000
        self.total_processing_time_ms += elapsed_ms

    def get_stats(self) -> dict:
        avg_latency = self.total_processing_time_ms / max(1, self.events_processed)
        return {
            "events_processed": self.events_processed,
            "gridlock_alerts": self.gridlock_alerts,
            "avg_processing_latency_ms": round(avg_latency, 3)
        }


def create_consumer(bootstrap_servers: str) -> Consumer:
    """Configures librdkafka consumer for low-latency fetch settings."""
    config = {
        'bootstrap.servers': bootstrap_servers,
        'group.id': GROUP_ID,
        'client.id': 'high-priority-signal-ctrl-1',
        'auto.offset.reset': 'latest',
        'enable.auto.commit': True,
        'auto.commit.interval.ms': 1000,
        'fetch.min.bytes': 1,
        'fetch.wait.max.ms': 10,
        'max.partition.fetch.bytes': 1048576,
        'queued.max.messages.kbytes': 65536,
    }
    return Consumer(config)


class LagMonitor:
    """Tracks consumer lag across assigned partitions."""

    def __init__(self, consumer: Consumer, topic: str):
        self.consumer = consumer
        self.topic = topic

    def get_lag(self) -> dict:
        """Calculates difference between high watermark and committed offset per partition."""
        lag_info = {}
        try:
            assignment = self.consumer.assignment()
            if not assignment:
                return {"total_lag": -1, "partitions": {}}

            for tp in assignment:
                committed = self.consumer.committed([tp])[0]
                lo, hi = self.consumer.get_watermark_offsets(tp)

                committed_offset = committed.offset if committed and committed.offset >= 0 else 0
                lag = max(0, hi - committed_offset)
                lag_info[f"partition-{tp.partition}"] = {
                    "committed": committed_offset,
                    "high_watermark": hi,
                    "lag": lag
                }

            total_lag = sum(p["lag"] for p in lag_info.values())
            return {"total_lag": total_lag, "partitions": lag_info}

        except Exception as e:
            logger.error(f"Error checking partition lag: {e}")
            return {"total_lag": -1, "error": str(e)}


def main():
    parser = argparse.ArgumentParser(description="HIGH_PRIORITY Signal Telemetry Consumer")
    parser.add_argument('--bootstrap-servers', default='localhost:9092')
    parser.add_argument('--duration', type=int, default=300)
    args = parser.parse_args()

    logger.info(f"Initializing HIGH_PRIORITY Consumer -> Group: {GROUP_ID}")

    consumer = create_consumer(args.bootstrap_servers)
    consumer.subscribe([TOPIC])

    controller = SignalController()
    lag_monitor = LagMonitor(consumer, TOPIC)

    start_time = time.time()
    last_log_time = start_time

    try:
        while time.time() - start_time < args.duration:
            msg = consumer.poll(timeout=0.01)

            if msg is None:
                continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                logger.error(f"Consumer exception: {msg.error()}")
                continue

            try:
                event = json.loads(msg.value().decode('utf-8'))
                controller.process_event(event)
            except json.JSONDecodeError as e:
                logger.error(f"JSON parsing error: {e}")

            now = time.time()
            if now - last_log_time >= 5:
                stats = controller.get_stats()
                lag = lag_monitor.get_lag()
                elapsed = now - start_time
                rate = stats["events_processed"] / elapsed

                logger.info(
                    f"HIGH_PRIORITY Status | Processed: {stats['events_processed']:,} | "
                    f"Rate: {rate:.0f}/s | Lag: {lag['total_lag']} | "
                    f"Latency: {stats['avg_processing_latency_ms']:.3f}ms")
                last_log_time = now

    except KeyboardInterrupt:
        logger.info("Consumer execution interrupted by user.")
    finally:
        consumer.close()
        stats = controller.get_stats()
        elapsed = time.time() - start_time
        logger.info(f"Summary: Processed {stats['events_processed']:,} events over {elapsed:.1f}s (Avg {stats['events_processed']/max(1,elapsed):.0f} evts/s). Avg Latency: {stats['avg_processing_latency_ms']:.3f}ms")


if __name__ == "__main__":
    main()
