# UrbanPulse: Real-Time Urban Operations Intelligence & Smart Traffic Platform

## Github Link: https://github.com/Verathagnus/urbanpulse.git

## Video Demonstration Link: https://drive.google.com/file/d/1zmVh6CHo0g664dBjp4BMzR4cCdlrr8fY/view?usp=sharing

## System Overview

UrbanPulse is a distributed stream processing and analytics platform developed for the municipal administration of MetroConnect. The platform ingests telemetry from four urban infrastructure streams:
- **Bus GPS Telemetry:** 12,000 transit buses (~2,400 events/sec)
- **Traffic Signal Sensors:** 3,800 junctions (~380 events/sec)
- **Air Quality Monitors:** 600 environmental sensors (~60 events/sec)
- **Smart Electricity Meters:** 1.1 million meters across 20 wards (~1,100 events/sec)

The architecture is implemented as a standard **Lambda Architecture**:
1. **Speed Layer (Apache Flink):** Low-latency, event-time stream processing with 30s bounded out-of-orderness watermarking and RocksDB state backend for sub-minute emergency incident detection (AQI emergencies, traffic gridlock, bus bunching).
2. **Batch / Micro-Batch Layer (Apache Spark Structured Streaming & Streaming SQL):** Windowed aggregations over 15-minute tumbling windows (with 45-minute late data watermarks) and 10-minute sliding SQL joins against static municipal zone profiles for regulatory compliance and health advisories.
3. **Ingestion Tier (Apache Kafka):** 3-broker KRaft cluster supporting consumer group isolation between real-time operational consumers and delay-tolerant analytical consumers.

---

## Directory Structure

```
SPA/
├── docs/                                  # Technical documentation and report artifacts
│   ├── TaskA_Architecture_Design.md       # Task A: System architecture, Lambda vs Kappa evaluation, Gov checklist
│   ├── TaskB_Kafka_Ingestion.md           # Task B: Kafka cluster topology, producer semantics, priority consumers, DLQ report
│   ├── TaskC_Flink_Spark_Processing.md    # Task C: PyFlink CEP engine, Spark Streaming SQL, comparative matrix
│   ├── TaskA_Architecture_Design-1.svg    # System architecture diagram
│   ├── TaskB_Kafka_Ingestion-1.svg        # Ingestion topology diagram
│   ├── TaskC_Flink_Spark_Processing-1.svg # Processing layer architecture diagram
│   └── pdf/
│       └── UrbanPulse_Technical_Report.pdf# Consolidated submission PDF
│
├── urbanpulse/                            # Implementation source code
│   ├── docker/
│   │   └── docker-compose.yml             # 3-broker Kafka (KRaft), Flink, Spark, TimescaleDB, MinIO, Grafana
│   │
│   ├── data/                              # Static reference datasets
│   │   ├── route_schedule.csv             # Route timetable for KTable enrichment (50 transit routes)
│   │   └── zone_profile.csv               # Municipal zone profile with demographics (20 wards)
│   │
│   ├── kafka/                             # Ingestion & Stream Processing
│   │   ├── config/
│   │   │   └── cluster_setup.sh           # Topic creation script with partition and retention specs
│   │   ├── producers/
│   │   │   ├── bus_gps_producer.py        # ~2,400 evt/s | route_id keying | idempotent producer
│   │   │   ├── air_quality_producer.py    # ~60 evt/s | at-least-once | 5% null AQI injection
│   │   │   ├── traffic_signal_producer.py # ~380 evt/s | junction_id keying | gridlock injection
│   │   │   └── smart_meter_producer.py    # ~1,100 evt/s | ward_id keying | diurnal load curve
│   │   ├── consumers/
│   │   │   ├── high_priority_consumer.py  # 1 consumer | all 6 partitions | 0ms delay | near-zero lag
│   │   │   └── standard_priority_consumer.py # 3-consumer group | 200ms processing delay | lag isolation
│   │   ├── streams/
│   │   │   └── route_enrichment.py        # Faust Stream-Table KTable join (bus_gps ⋈ route_schedule.csv)
│   │   └── dlq/
│   │       ├── dlq_router.py              # Schema validation engine (6 rules: nulls, range, GPS bounds, skew)
│   │       └── dlq_report.py              # 5-minute Dead Letter Queue error distribution reporter
│   │
│   ├── flink/                             # Speed Layer Processing
│   │   └── incident_detection.py          # PyFlink DataStream app (AQI emergency, gridlock, bus bunching)
│   │
│   └── spark/                             # Batch & Micro-Batch Processing Layer
│       ├── ward_energy_streaming.py       # Structured Streaming: 15-min tumbling window | 45-min watermark | dual sink
│       └── aqi_health_advisory.py         # Streaming SQL: 10-min rolling average | zone_profile join | filter > 150
│
├── streamlit_app.py                       # Unified interactive testing dashboard and process manager
├── requirements.txt                       # Python dependencies
└── README.md                              # Repository reference and execution guide
```

---

## Technical Stack

| Component | Technology | Purpose |
| :--- | :--- | :--- |
| **Messaging Infrastructure** | Apache Kafka (KRaft mode) | 3-broker distributed ingestion cluster (no ZooKeeper dependency) |
| **Speed Processing Engine** | Apache Flink | Sub-second stateful incident detection (RocksDB state backend) |
| **Batch / Micro-Batch Engine** | Apache Spark | Structured Streaming aggregations and Streaming SQL joins |
| **Stream-Table Enrichment** | Faust Streaming | Python stream processing framework for KTable joins |
| **Time-Series Storage** | TimescaleDB | Hypertable indexing for time-series sensor telemetry |
| **Geospatial & Cold Storage** | PostGIS / Parquet on MinIO | Spatial indexing and partitioned historical Parquet storage |
| **Visualization & Management** | Streamlit / Grafana | Interactive testing dashboard and municipal web console |

---

## Deployment & Setup Guide

### 1. Prerequisites
- Operating System: Linux or Windows 10+ (PowerShell / WSL)
- Docker Engine 24.0+ and Docker Compose v2.0+
- Python 3.10 with `pip` and virtual environment support
- Recommended Hardware: x64 architecture, 8 Core CPU, 16 GB RAM, 20 GB free disk space

### 2. Environment Setup
```bash
git clone https://github.com/Verathagnus/urbanpulse.git
cd SPA
python -m venv venv
source venv/bin/activate  # On Windows: .\venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

---

## Step-by-Step Execution Procedure

### Step 1: Launch Infrastructure Containers
Initialize the 3-broker Kafka cluster, Flink JobManager/TaskManager, Spark Master/Worker, TimescaleDB, MinIO, and Grafana containers:
```bash
cd urbanpulse/docker
docker compose up -d
docker compose ps
```

### Step 2: Initialize Kafka Topics and Retention Policies
Execute topic initialization to configure partition counts and statutory log retention policies (24h GPS, 7d traffic, 90d AQI, 365d smart meters):
```bash
# Execute inside Kafka broker container:
docker exec -it urbanpulse-kafka-1 bash /opt/kafka/cluster_setup.sh

# Or directly from host:
bash urbanpulse/kafka/config/cluster_setup.sh
```

### Step 3: Run Ingestion and Stream Verification (Task B)

#### A. Stream-Table Route Enrichment Service
```bash
cd urbanpulse/kafka/streams
python route_enrichment.py worker --without-web -l info
```

#### B. Dead Letter Queue (DLQ) Validation Router
```bash
cd urbanpulse/kafka/dlq
python dlq_router.py --bootstrap-servers localhost:9092 --duration 300
```

#### C. Telemetry Stream Producers
Launch telemetry generators in separate terminal sessions:
```bash
# Bus GPS Producer (~2,400 events/sec, route_id keying)
python urbanpulse/kafka/producers/bus_gps_producer.py --rate 2400 --duration 0

# Air Quality Producer (~60 events/sec, at-least-once, 5% null AQI)
python urbanpulse/kafka/producers/air_quality_producer.py --rate 60 --duration 0

# Traffic Signal Producer (~380 events/sec, gridlock simulation)
python urbanpulse/kafka/producers/traffic_signal_producer.py --rate 380 --duration 0

# Smart Meter Producer (~1,100 events/sec, ward_id keying)
python urbanpulse/kafka/producers/smart_meter_producer.py --rate 1100 --duration 0
```

#### D. Priority Consumer Group Isolation
Demonstrate zero-lag operational control alongside delayed analytical consumers:
```bash
# HIGH_PRIORITY Signal Control Consumer (0ms delay, near-zero lag)
python urbanpulse/kafka/consumers/high_priority_consumer.py --duration 300

# STANDARD_PRIORITY Analytics Consumer Group (3 instances, 200ms processing delay)
python urbanpulse/kafka/consumers/standard_priority_consumer.py --consumer-id 1 --duration 300 &
python urbanpulse/kafka/consumers/standard_priority_consumer.py --consumer-id 2 --duration 300 &
python urbanpulse/kafka/consumers/standard_priority_consumer.py --consumer-id 3 --duration 300 &
```

#### E. DLQ Error Distribution Summary
Generate the 5-minute error classification report:
```bash
python urbanpulse/kafka/dlq/dlq_report.py --duration 300
```

---

### Step 4: Run Stream Processing Engines (Task C)

#### A. Apache Flink Incident Detection Pipeline
Submit the PyFlink DataStream application to evaluate event-time window patterns and emit alerts to `urbanpulse.incidents`:
```bash
cd urbanpulse/flink
python incident_detection.py
```
Monitor alerts via Kafka console consumer:
```bash
docker exec -it urbanpulse-kafka-1 kafka-console-consumer \
    --bootstrap-server kafka-broker-1:29092 \
    --topic urbanpulse.incidents \
    --from-beginning
```

#### B. Apache Spark Ward Energy Analytics
Compute 15-minute tumbling aggregations with 45-minute late watermarking:
```bash
cd urbanpulse/spark
python ward_energy_streaming.py --bootstrap-servers localhost:9092
```

#### C. Apache Spark Streaming SQL AQI Health Advisory
Compute 10-minute sliding window averages and join against static zone metadata:
```bash
cd urbanpulse/spark
python aqi_health_advisory.py --bootstrap-servers localhost:9092
```

---

### Step 5: Interactive Dashboard & Process Controller
Launch the Streamlit dashboard for central process control, log monitoring, and serving layer querying:
```bash
streamlit run streamlit_app.py
```

---

## Deliverables & Submission Summary

| Deliverable | Location / Reference | Description |
| :--- | :--- | :--- |
| **Consolidated PDF Report** | [`Report.pdf`](Report.pdf) / [`docs/pdf/UrbanPulse_Technical_Report.pdf`](docs/pdf/UrbanPulse_Technical_Report.pdf) | Comprehensive PDF report covering all Task A, Task B, and Task C requirements. |
| **Source Code Repository** | [GitHub Repository](https://github.com/Verathagnus/urbanpulse.git) | Complete source code organized by pipeline stage (Kafka, PyFlink, Spark, Faust, Streamlit). |
| **End-to-End Video Demonstration** | [Google Drive Video Link](https://drive.google.com/file/d/1zmVh6CHo0g664dBjp4BMzR4cCdlrr8fY/view?usp=sharing) | Walkthrough video demonstrating the end-to-end working platform with spoken design logic. |
| **Interactive Test Console** | [`streamlit_app.py`](streamlit_app.py) | Streamlit dashboard for process lifecycle management and serving layer verification. |

---