#!/bin/bash
# ============================================================
# UrbanPulse — Kafka Topic Creation & Retention Configuration
# ============================================================
# This script creates all required Kafka topics with justified
# partition counts and retention policies for the UrbanPulse
# Smart City platform.
#
# Prerequisites: 3-broker KRaft cluster running via docker-compose
# Usage: docker exec -it urbanpulse-kafka-1 bash < cluster_setup.sh
# ============================================================

KAFKA_BOOTSTRAP="kafka-broker-1:29092"

echo "============================================================"
echo "  UrbanPulse — Kafka Topic Setup"
echo "  Bootstrap Server: ${KAFKA_BOOTSTRAP}"
echo "============================================================"

# ---------------------------------------------------------------
# Topic 1: urbanpulse.bus_gps
# Rate: ~2,400 events/sec
# Partitions: 12
#   Justification: High throughput topic. With 12 partitions, each
#   partition handles ~200 events/sec. Keyed by route_id (~50 routes),
#   12 partitions provides good load distribution across 3 brokers
#   (4 partitions per broker). 12 also allows scaling to 12 consumers.
# Retention: 24 hours (86,400,000 ms)
#   Justification: GPS data is transient — real-time value only.
#   24-hour retention enables accident investigation replay (most
#   incidents are reported within same day). Beyond 24h, data is
#   archived to PostGIS for historical route analysis.
# ---------------------------------------------------------------
echo ""
echo "[1/9] Creating urbanpulse.bus_gps (12 partitions, 24h retention)..."
kafka-topics --create \
  --bootstrap-server ${KAFKA_BOOTSTRAP} \
  --topic urbanpulse.bus_gps \
  --partitions 12 \
  --replication-factor 3 \
  --config retention.ms=86400000 \
  --config cleanup.policy=delete \
  --config segment.bytes=536870912 \
  --config min.insync.replicas=2 \
  --if-not-exists

# ---------------------------------------------------------------
# Topic 2: urbanpulse.traffic_signals
# Rate: ~380 events/sec
# Partitions: 6
#   Justification: Moderate throughput (~63 events/sec per partition).
#   3,800 junctions across ~20 zones — 6 partitions provides
#   adequate parallelism for both HIGH_PRIORITY (1 consumer reads all)
#   and STANDARD_PRIORITY (3 consumers, 2 partitions each) groups.
# Retention: 7 days (604,800,000 ms)
#   Justification: Traffic signal data needed for weekly pattern
#   analysis (weekday vs weekend signal optimization). Not required
#   for regulatory retention but useful for tuning adaptive algorithms.
# ---------------------------------------------------------------
echo "[2/9] Creating urbanpulse.traffic_signals (6 partitions, 7d retention)..."
kafka-topics --create \
  --bootstrap-server ${KAFKA_BOOTSTRAP} \
  --topic urbanpulse.traffic_signals \
  --partitions 6 \
  --replication-factor 3 \
  --config retention.ms=604800000 \
  --config cleanup.policy=delete \
  --config segment.bytes=268435456 \
  --config min.insync.replicas=2 \
  --if-not-exists

# ---------------------------------------------------------------
# Topic 3: urbanpulse.air_quality
# Rate: ~60 events/sec
# Partitions: 4
#   Justification: Lower throughput (~15 events/sec per partition).
#   600 sensors across 20 zones — 4 partitions sufficient. We
#   prioritize low latency over parallelism since AQI alerts must
#   fire within 2 minutes. Fewer partitions = less coordination
#   overhead for Flink's keyed state.
# Retention: 90 days (7,776,000,000 ms)
#   Justification: CPCB (Central Pollution Control Board) requires
#   90-day AQI data retention for pollution trend analysis and
#   compliance reporting. This retention enables: (1) seasonal
#   trend comparison, (2) Diwali/festival pollution spike analysis,
#   (3) reprocessing for corrected sensor calibration data.
# ---------------------------------------------------------------
echo "[3/9] Creating urbanpulse.air_quality (4 partitions, 90d retention)..."
kafka-topics --create \
  --bootstrap-server ${KAFKA_BOOTSTRAP} \
  --topic urbanpulse.air_quality \
  --partitions 4 \
  --replication-factor 3 \
  --config retention.ms=7776000000 \
  --config cleanup.policy=delete \
  --config segment.bytes=1073741824 \
  --config min.insync.replicas=2 \
  --if-not-exists

# ---------------------------------------------------------------
# Topic 4: urbanpulse.smart_meters
# Rate: ~1,100 events/sec
# Partitions: 8
#   Justification: High throughput with 1.1M meters across ~20 wards.
#   8 partitions (~137 events/sec each). Keyed by ward_id for
#   ward-level aggregation in Spark Structured Streaming.
#   8 partitions aligns with Spark's default parallelism (2 cores × 4).
# Retention: 365 days (31,536,000,000 ms)
#   Justification: SERC (State Electricity Regulatory Commission)
#   mandates 1-year energy consumption data retention for:
#   (1) annual energy audit and regulatory compliance,
#   (2) yearly billing reconciliation,
#   (3) demand forecasting model training.
#   This is the longest retention — we use larger segment sizes
#   and enable log compaction to manage storage.
# ---------------------------------------------------------------
echo "[4/9] Creating urbanpulse.smart_meters (8 partitions, 365d retention)..."
kafka-topics --create \
  --bootstrap-server ${KAFKA_BOOTSTRAP} \
  --topic urbanpulse.smart_meters \
  --partitions 8 \
  --replication-factor 3 \
  --config retention.ms=31536000000 \
  --config cleanup.policy=delete \
  --config segment.bytes=1073741824 \
  --config min.insync.replicas=2 \
  --if-not-exists

# ---------------------------------------------------------------
# Topic 5: urbanpulse.dlq (Dead Letter Queue)
# Receives invalid/malformed messages from all streams
# Partitions: 3 (one per broker for even distribution)
# Retention: 30 days (for error trend analysis)
# ---------------------------------------------------------------
echo "[5/9] Creating urbanpulse.dlq (3 partitions, 30d retention)..."
kafka-topics --create \
  --bootstrap-server ${KAFKA_BOOTSTRAP} \
  --topic urbanpulse.dlq \
  --partitions 3 \
  --replication-factor 3 \
  --config retention.ms=2592000000 \
  --config cleanup.policy=delete \
  --config min.insync.replicas=2 \
  --if-not-exists

# ---------------------------------------------------------------
# Topic 6: urbanpulse.incidents (Flink alert output)
# ---------------------------------------------------------------
echo "[6/9] Creating urbanpulse.incidents (4 partitions, 30d retention)..."
kafka-topics --create \
  --bootstrap-server ${KAFKA_BOOTSTRAP} \
  --topic urbanpulse.incidents \
  --partitions 4 \
  --replication-factor 3 \
  --config retention.ms=2592000000 \
  --config cleanup.policy=delete \
  --config min.insync.replicas=2 \
  --if-not-exists

# ---------------------------------------------------------------
# Topic 7: urbanpulse.enriched_bus_gps (Kafka Streams output)
# ---------------------------------------------------------------
echo "[7/9] Creating urbanpulse.enriched_bus_gps (12 partitions, 24h retention)..."
kafka-topics --create \
  --bootstrap-server ${KAFKA_BOOTSTRAP} \
  --topic urbanpulse.enriched_bus_gps \
  --partitions 12 \
  --replication-factor 3 \
  --config retention.ms=86400000 \
  --config cleanup.policy=delete \
  --config min.insync.replicas=2 \
  --if-not-exists

# ---------------------------------------------------------------
# Topic 8: urbanpulse.ward_energy_summary (Spark output)
# ---------------------------------------------------------------
echo "[8/9] Creating urbanpulse.ward_energy_summary (6 partitions, 90d retention)..."
kafka-topics --create \
  --bootstrap-server ${KAFKA_BOOTSTRAP} \
  --topic urbanpulse.ward_energy_summary \
  --partitions 6 \
  --replication-factor 3 \
  --config retention.ms=7776000000 \
  --config cleanup.policy=delete \
  --config min.insync.replicas=2 \
  --if-not-exists

# ---------------------------------------------------------------
# Topic 9: urbanpulse.health_advisories (Spark SQL output)
# ---------------------------------------------------------------
echo "[9/9] Creating urbanpulse.health_advisories (4 partitions, 90d retention)..."
kafka-topics --create \
  --bootstrap-server ${KAFKA_BOOTSTRAP} \
  --topic urbanpulse.health_advisories \
  --partitions 4 \
  --replication-factor 3 \
  --config retention.ms=7776000000 \
  --config cleanup.policy=delete \
  --config min.insync.replicas=2 \
  --if-not-exists

# ---------------------------------------------------------------
# Verification: List all topics and their configurations
# ---------------------------------------------------------------
echo ""
echo "============================================================"
echo "  Topic Creation Complete — Verification"
echo "============================================================"
echo ""
echo "--- All Topics ---"
kafka-topics --list --bootstrap-server ${KAFKA_BOOTSTRAP}

echo ""
echo "--- Topic Details ---"
for topic in urbanpulse.bus_gps urbanpulse.traffic_signals urbanpulse.air_quality urbanpulse.smart_meters urbanpulse.dlq urbanpulse.incidents urbanpulse.enriched_bus_gps urbanpulse.ward_energy_summary urbanpulse.health_advisories; do
  echo ""
  echo "=== ${topic} ==="
  kafka-topics --describe --bootstrap-server ${KAFKA_BOOTSTRAP} --topic ${topic}
done

echo ""
echo "============================================================"
echo "  UrbanPulse Kafka Setup Complete!"
echo "  Brokers: 3 (KRaft mode)"
echo "  Topics: 9"
echo "  Replication Factor: 3 (all topics)"
echo "  Min ISR: 2 (all topics)"
echo "============================================================"
