"""Dead Letter Queue (DLQ) Error Distribution Reporter.

Consumes records published to `urbanpulse.dlq`, aggregates error count distributions by `error_reason` category,
and generates summary reporting for stream quality auditing.
"""

import json
import time
import logging
import argparse
from collections import Counter, defaultdict
from datetime import datetime, timezone
from confluent_kafka import Consumer, KafkaError

DLQ_TOPIC = "urbanpulse.dlq"
REPORT_DURATION_SEC = 300

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(name)s: %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger("DLQReport")


class DLQReportGenerator:
    """Aggregates DLQ message batches and outputs structured metrics summaries."""

    def __init__(self):
        self.error_counts = Counter()
        self.source_topic_counts = Counter()
        self.error_samples = defaultdict(list)
        self.total_messages = 0
        self.start_time = None
        self.end_time = None

    def add_message(self, dlq_message: dict):
        """Accumulates an individual DLQ error record into distribution metrics."""
        self.total_messages += 1

        error_reason = dlq_message.get("error_reason", "UNKNOWN")
        source_topic = dlq_message.get("source_topic", "unknown")

        self.error_counts[error_reason] += 1
        self.source_topic_counts[source_topic] += 1

        if len(self.error_samples[error_reason]) < 3:
            self.error_samples[error_reason].append({
                "source_topic": source_topic,
                "errors": dlq_message.get("errors", []),
                "original_message": dlq_message.get("original_message"),
                "dlq_timestamp": dlq_message.get("dlq_timestamp")
            })

    def generate_report(self) -> str:
        """Formats collected metrics into console text report."""
        duration = (self.end_time - self.start_time) if self.start_time and self.end_time else 0

        report = []
        report.append("")
        report.append("=" * 70)
        report.append("  UrbanPulse — Dead Letter Queue (DLQ) Error Distribution Report")
        report.append("=" * 70)
        report.append(f"  Report Period:    {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')} UTC")
        report.append(f"  Duration:         {duration:.0f} seconds ({duration/60:.1f} minutes)")
        report.append(f"  Total DLQ Events: {self.total_messages:,}")
        report.append(f"  DLQ Topic:        {DLQ_TOPIC}")
        report.append("")

        report.append("-" * 70)
        report.append("  1. Error Type Distribution")
        report.append("-" * 70)
        report.append(f"  {'Error Type':<30} {'Count':>8} {'Percentage':>12}")
        report.append(f"  {'-'*30} {'-'*8} {'-'*12}")

        for error_type, count in self.error_counts.most_common():
            pct = count / max(1, self.total_messages) * 100
            bar = "█" * int(pct / 2)
            report.append(f"  {error_type:<30} {count:>8,} {pct:>10.1f}%  {bar}")

        report.append(f"  {'-'*30} {'-'*8} {'-'*12}")
        report.append(f"  {'TOTAL':<30} {self.total_messages:>8,} {'100.0%':>12}")

        report.append("")
        report.append("-" * 70)
        report.append("  2. Errors by Source Topic")
        report.append("-" * 70)
        report.append(f"  {'Source Topic':<35} {'Count':>8} {'Percentage':>12}")
        report.append(f"  {'-'*35} {'-'*8} {'-'*12}")

        for topic, count in self.source_topic_counts.most_common():
            pct = count / max(1, self.total_messages) * 100
            report.append(f"  {topic:<35} {count:>8,} {pct:>10.1f}%")

        report.append("")
        report.append("-" * 70)
        report.append("  3. Sample Error Messages (up to 3 per error type)")
        report.append("-" * 70)

        for error_type, samples in self.error_samples.items():
            report.append(f"\n  [{error_type}]")
            for i, sample in enumerate(samples, 1):
                report.append(f"    Sample {i}:")
                report.append(f"      Source: {sample['source_topic']}")
                if sample.get('errors'):
                    for err in sample['errors']:
                        report.append(f"      Error: {err.get('error_message', 'N/A')}")
                if sample.get('original_message'):
                    msg_str = json.dumps(sample['original_message'])
                    if len(msg_str) > 100:
                        msg_str = msg_str[:100] + "..."
                    report.append(f"      Message: {msg_str}")

        report.append("")
        report.append("-" * 70)
        report.append("  4. Observations & Recommendations")
        report.append("-" * 70)

        if self.error_counts.get("NULL_AQI", 0) > 0:
            null_pct = self.error_counts["NULL_AQI"] / max(1, self.total_messages) * 100
            report.append(f"  • NULL_AQI errors constitute {null_pct:.1f}% of DLQ messages.")
            report.append(f"    → Expected: ~5% of air quality events have null AQI (sensor timeout)")

        if self.error_counts.get("IMPOSSIBLE_GPS", 0) > 0:
            report.append(f"  • {self.error_counts['IMPOSSIBLE_GPS']} GPS coordinate errors detected.")

        if self.error_counts.get("FUTURE_TIMESTAMP", 0) > 0:
            report.append(f"  • {self.error_counts['FUTURE_TIMESTAMP']} future timestamp errors.")

        report.append("")
        report.append("=" * 70)
        report.append("  End of DLQ Report")
        report.append("=" * 70)

        return "\n".join(report)


def main():
    parser = argparse.ArgumentParser(description="DLQ Error Distribution Report Generator")
    parser.add_argument('--bootstrap-servers', default='localhost:9092')
    parser.add_argument('--duration', type=int, default=300, help='Sampling window in seconds')
    args = parser.parse_args()

    logger.info(f"Initializing DLQ Reporter on topic {DLQ_TOPIC}")

    consumer_config = {
        'bootstrap.servers': args.bootstrap_servers,
        'group.id': 'DLQ_REPORT_GENERATOR',
        'client.id': 'dlq-report-gen',
        'auto.offset.reset': 'earliest',
        'enable.auto.commit': True,
    }
    consumer = Consumer(consumer_config)
    consumer.subscribe([DLQ_TOPIC])

    report_gen = DLQReportGenerator()
    report_gen.start_time = time.time()

    logger.info(f"Sampling DLQ records for {args.duration}s...")

    try:
        while time.time() - report_gen.start_time < args.duration:
            msg = consumer.poll(timeout=1.0)

            if msg is None:
                continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                logger.error(f"Consumer exception: {msg.error()}")
                continue

            try:
                dlq_message = json.loads(msg.value().decode('utf-8'))
                report_gen.add_message(dlq_message)
            except json.JSONDecodeError:
                logger.error(f"Failed to parse DLQ record payload")

            elapsed = time.time() - report_gen.start_time
            if report_gen.total_messages % 100 == 0 and report_gen.total_messages > 0:
                logger.info(f"Sampled {report_gen.total_messages:,} records ({elapsed:.0f}s / {args.duration}s)")

    except KeyboardInterrupt:
        logger.info("Report collection interrupted by user.")
    finally:
        report_gen.end_time = time.time()
        consumer.close()

    report = report_gen.generate_report()
    print(report)

    report_path = f"dlq_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
    with open(report_path, 'w') as f:
        f.write(report)
    logger.info(f"Report exported to: {report_path}")


if __name__ == "__main__":
    main()
