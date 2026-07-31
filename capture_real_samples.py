"""
Capture real enriched event samples and DLQ report data by running actual simulations.
Outputs JSON files for updating the technical report with genuine data.
"""

import os
import sys
import json
import time
import signal
import subprocess
from datetime import datetime

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
PYTHON = sys.executable
BOOTSTRAP = "localhost:9092"

# Duration for producers/DLQ router (seconds) — 300s = 5 min trace
PRODUCER_DURATION = 300
DLQ_ROUTER_DURATION = 310
DLQ_REPORT_DURATION = 15

def run_bg(cmd, label):
    """Start a background process."""
    print(f"  [START] {label}: {' '.join(cmd)}")
    p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, cwd=BASE_DIR)
    return p

def capture_enriched_sample():
    """Use kafka-python-ng to grab a real enriched bus GPS event from Kafka."""
    try:
        from kafka import KafkaConsumer, TopicPartition
        consumer = KafkaConsumer(
            bootstrap_servers=BOOTSTRAP,
            auto_offset_reset='latest',
            consumer_timeout_ms=30000,
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            request_timeout_ms=10000,
        )
        topic = 'urbanpulse.enriched_bus_gps'
        partitions = consumer.partitions_for_topic(topic)
        if not partitions:
            print("  [WARN] Topic urbanpulse.enriched_bus_gps not found. Waiting...")
            time.sleep(10)
            partitions = consumer.partitions_for_topic(topic)
        
        if not partitions:
            consumer.close()
            return None

        tps = [TopicPartition(topic, p) for p in sorted(partitions)]
        consumer.assign(tps)
        # Seek to end minus a few messages
        for tp in tps:
            consumer.seek_to_end(tp)
            end = consumer.position(tp)
            consumer.seek(tp, max(0, end - 5))

        samples = []
        for msg in consumer:
            samples.append(msg.value)
            if len(samples) >= 3:
                break
        consumer.close()
        return samples
    except Exception as e:
        print(f"  [ERROR] Failed to capture enriched sample: {e}")
        return None


def capture_dlq_samples():
    """Grab real DLQ messages from Kafka."""
    try:
        from kafka import KafkaConsumer, TopicPartition
        consumer = KafkaConsumer(
            bootstrap_servers=BOOTSTRAP,
            auto_offset_reset='earliest',
            consumer_timeout_ms=10000,
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            request_timeout_ms=10000,
        )
        topic = 'urbanpulse.dlq'
        partitions = consumer.partitions_for_topic(topic)
        if not partitions:
            consumer.close()
            return [], {}

        tps = [TopicPartition(topic, p) for p in sorted(partitions)]
        consumer.assign(tps)
        # Read from beginning for full distribution
        for tp in tps:
            consumer.seek_to_beginning(tp)

        all_msgs = []
        for msg in consumer:
            all_msgs.append(msg.value)
            if len(all_msgs) >= 5000:
                break
        consumer.close()

        # Compute distribution
        from collections import Counter, defaultdict
        error_counts = Counter()
        source_counts = Counter()
        error_samples = defaultdict(list)

        for m in all_msgs:
            err_type = m.get("error_reason", "UNKNOWN")
            src = m.get("source_topic", "unknown")
            error_counts[err_type] += 1
            source_counts[src] += 1
            if len(error_samples[err_type]) < 3:
                error_samples[err_type].append(m)

        dist = {
            "total": len(all_msgs),
            "error_counts": dict(error_counts.most_common()),
            "source_counts": dict(source_counts.most_common()),
            "error_samples": {k: v for k, v in error_samples.items()},
        }
        return all_msgs, dist
    except Exception as e:
        print(f"  [ERROR] Failed to capture DLQ samples: {e}")
        return [], {}


def main():
    print("=" * 70)
    print("  UrbanPulse — Real Data Capture for Technical Report")
    print("=" * 70)
    print()

    procs = []

    # 1. Start all 4 producers
    print("[Phase 1] Starting producers...")
    producers = [
        ([PYTHON, "urbanpulse/kafka/producers/bus_gps_producer.py", "--rate", "200", "--duration", str(PRODUCER_DURATION)], "Bus GPS Producer"),
        ([PYTHON, "urbanpulse/kafka/producers/air_quality_producer.py", "--rate", "60", "--duration", str(PRODUCER_DURATION)], "Air Quality Producer"),
        ([PYTHON, "urbanpulse/kafka/producers/traffic_signal_producer.py", "--rate", "50", "--duration", str(PRODUCER_DURATION)], "Traffic Signal Producer"),
        ([PYTHON, "urbanpulse/kafka/producers/smart_meter_producer.py", "--rate", "100", "--duration", str(PRODUCER_DURATION)], "Smart Meter Producer"),
    ]
    for cmd, label in producers:
        procs.append((run_bg(cmd, label), label))

    # 2. Start DLQ router
    print("\n[Phase 2] Starting DLQ Router...")
    dlq_router_proc = run_bg(
        [PYTHON, "urbanpulse/kafka/dlq/dlq_router.py", "--duration", str(DLQ_ROUTER_DURATION)],
        "DLQ Router"
    )
    procs.append((dlq_router_proc, "DLQ Router"))

    # 3. Start route enrichment
    print("\n[Phase 3] Starting Route Enrichment (Faust)...")
    enrich_proc = run_bg(
        [PYTHON, "urbanpulse/kafka/streams/route_enrichment.py", "worker", "--without-web", "-l", "info"],
        "Route Enrichment"
    )
    procs.append((enrich_proc, "Route Enrichment"))

    # 4. Wait for data to flow
    print(f"\n[Phase 4] Waiting {PRODUCER_DURATION + 10}s for data to flow through pipelines...")
    time.sleep(PRODUCER_DURATION + 10)

    # 5. Capture enriched samples
    print("\n[Phase 5] Capturing enriched bus GPS samples...")
    enriched_samples = capture_enriched_sample()
    if enriched_samples:
        out_path = os.path.join(BASE_DIR, "captured_enriched_samples.json")
        with open(out_path, 'w') as f:
            json.dump(enriched_samples, f, indent=2)
        print(f"  ✅ Captured {len(enriched_samples)} enriched samples → {out_path}")
        print(f"  Sample 1 preview:")
        print(json.dumps(enriched_samples[0], indent=2))
    else:
        print("  ⚠️  No enriched samples captured (topic may be empty)")

    # 6. Capture DLQ distribution
    print("\n[Phase 6] Capturing DLQ distribution...")
    dlq_msgs, dlq_dist = capture_dlq_samples()
    if dlq_dist:
        out_path = os.path.join(BASE_DIR, "captured_dlq_distribution.json")
        with open(out_path, 'w') as f:
            json.dump(dlq_dist, f, indent=2, default=str)
        print(f"  ✅ Captured {dlq_dist['total']} DLQ messages → {out_path}")
        print(f"  Error distribution:")
        for err_type, count in dlq_dist['error_counts'].items():
            pct = count / max(1, dlq_dist['total']) * 100
            print(f"    {err_type}: {count} ({pct:.1f}%)")
    else:
        print("  ⚠️  No DLQ data captured")

    # 7. Terminate background processes
    print("\n[Phase 7] Stopping background processes...")
    for proc, label in procs:
        try:
            proc.send_signal(signal.SIGINT)
            proc.wait(timeout=10)
            print(f"  ✓ Stopped {label}")
        except Exception:
            proc.kill()
            print(f"  ✗ Killed {label}")

    print("\n" + "=" * 70)
    print("  ✅ Data capture complete!")
    print("=" * 70)


if __name__ == "__main__":
    main()
